
import requests
import pandas as pd
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Dict, Any, Callable, Optional, Tuple
from datetime import datetime
from requests.exceptions import JSONDecodeError
from sqlalchemy import create_engine

# --- 1. Configuração do Logging ---
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
log = logging.getLogger(__name__)


# --- 2. TODO: CONFIGURE SEUS SEGREDOS AQUI ---

CRM_API_URL = "https://subdominio.kommo.com/"
CRM_API_TOKEN = ""

POSTGRES_CONN_STRING = "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow_db"

MAX_THREADS = 8
PAGE_LIMIT = 250

def create_session_with_retries(token: str) -> requests.Session:
    """Cria uma sessão de requests com retries automáticos."""
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    })
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_api_page(session: requests.Session, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Busca e parseia uma única página da API."""
    response = None
    try:
        response = session.get(url, params=params, timeout=(10, 20))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404 or e.response.status_code == 204:
             log.info(f"Página não encontrada ou sem conteúdo ({e.response.status_code}) em {url} com params {params}. Fim.")
        else:
             log.warning(f"Erro HTTP {e.response.status_code} de {url} com params {params}: {e.response.text}")
    except JSONDecodeError as e:
        status = response.status_code if response else 'N/A'
        text = response.text[:200] if response else 'N/A'
        log.error(f"Falha ao decodificar JSON de {url} com params {params}. Status: {status}. Resposta: '{text}...'")
    except requests.exceptions.RequestException as e:
        log.error(f"Erro de request (timeout, conexão) em {url} com params {params}: {e}")
    return None

def parse_and_process_page(
    session: requests.Session, 
    base_url: str, 
    base_params: Dict[str, Any], 
    page: int, 
    embed_key: str, 
    parse_func: Callable[[List[Dict]], List[Dict]]
) -> pd.DataFrame:
    """Função alvo da thread: busca, extrai itens e aplica parse."""
    params = base_params.copy()
    params['page'] = page
    params['limit'] = PAGE_LIMIT
    
    data = fetch_api_page(session, base_url, params)
    
    if not data:
        return pd.DataFrame()
    items = data.get("_embedded", {}).get(embed_key, [])
    if not items:
        return pd.DataFrame()
    
    try:
        parsed_data = parse_func(items)
        return pd.DataFrame(parsed_data)
    except Exception as e:
        log.error(f"Erro durante o parse_func na página {page}: {e}", exc_info=True)
        return pd.DataFrame()

def fetch_all_pages_concurrently(
    session: requests.Session,
    base_url: str,
    base_params: Dict[str, Any],
    embed_key: str,
    parse_func: Callable[[List[Dict]], List[Dict]]
) -> pd.DataFrame:
    """Paginador concorrente genérico."""
    all_dfs = []
    page = 1
    log.info(f"Iniciando busca concorrente para: {base_url} (embed_key: '{embed_key}')")
    
    while True:
        tasks_submitted = 0
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for i in range(MAX_THREADS):
                current_page = page + i
                futures.append(executor.submit(
                    parse_and_process_page,
                    session, base_url, base_params, current_page, embed_key, parse_func
                ))
                tasks_submitted += 1
            
            page_has_data = False
            for future in as_completed(futures):
                df_page = future.result()
                if not df_page.empty:
                    all_dfs.append(df_page)
                    page_has_data = True
        
        if not page_has_data:
            log.info(f"Lote de páginas {page}-{page+tasks_submitted-1} não retornou dados. Fim da paginação.")
            break
            
        total_rows = sum(len(df) for df in all_dfs)
        log.info(f"Busca concorrente: {total_rows} linhas coletadas até a página {page + tasks_submitted - 1}.")
        page += tasks_submitted

    if not all_dfs:
        log.warning(f"Nenhum dado encontrado para {base_url}")
        return pd.DataFrame()
    
    log.info(f"Concatenação final de {len(all_dfs)} dataframes de página.")
    return pd.concat(all_dfs, ignore_index=True)

# --- 3.1. Lógica Específica de Events ---
def parse_events_data(events_items: List[Dict]) -> List[Dict]:
    """Função de parse específica para o endpoint /events."""
    parsed_list = []
    for event in events_items:
        try:
            # Filtra apenas por 'lead_status_changed' no parse para segurança extra
            if event.get("type") != 'lead_status_changed': 
                continue
                
            value_after = event.get('value_after', [{}])[0]
            lead_status = value_after.get('lead_status', {})
            lead_status_after = lead_status.get('id')
            pipeline = lead_status.get('pipeline_id')
            
            # Garante que temos o status_id, que é crucial
            if not lead_status_after: 
                continue

            parsed_list.append({
                "id": event.get("id"),
                "type": event.get("type"),
                "entity_id": event.get("entity_id"), # ID do Lead associado
                "created_at": event.get("created_at"),
                "status_id": lead_status_after, # Status do lead APÓS a mudança
                'pipeline_id': pipeline # Pipeline associado ao status
            })
        except (IndexError, TypeError, AttributeError) as e:
            log.warning(f"Erro ao parsear evento ID {event.get('id')}: {e}. Pulando.")
    return parsed_list

def run_events_extraction(
    base_url: str, 
    api_token: str,
    start_date: Optional[datetime] = None, # Mantido para consistência, mas não usado em Full Refresh
    end_date: Optional[datetime] = None   # Mantido para consistência, mas não usado em Full Refresh
) -> pd.DataFrame:
    """Função principal para extração de Events (Full Refresh)."""
    log.info("--- Iniciando Extração de EVENTS (Modo Full Refresh) ---")
    
    # Parâmetros base para buscar apenas eventos de mudança de status de lead
    base_params = {'filter[type]': 'lead_status_changed'}
    
    # Ignora start_date e end_date para Full Refresh
    log.warning("Execução em MODO FULL REFRESH (sem filtro de data).")

    session = create_session_with_retries(api_token)
    try:
        df_events = fetch_all_pages_concurrently(
            session=session,
            base_url=f"{base_url}/api/v4/events", # URL correta do endpoint
            base_params=base_params,
            embed_key="events", # Chave correta no JSON de resposta
            parse_func=parse_events_data # Função de parse específica
        )
    finally:
        session.close()
        log.info("Sessão de requests fechada.")

    # Pós-processamento: Converter timestamp e remover duplicatas
    if not df_events.empty:
        log.info(f"Total de {len(df_events)} eventos brutos baixados. Processando...")
        # Converte timestamp UNIX para datetime e ajusta fuso horário
        df_events['created_at'] = pd.to_datetime(df_events['created_at'], unit='s', utc=True, errors='coerce')
        try:
             df_events['created_at'] = df_events['created_at'].dt.tz_convert('America/Sao_Paulo')
        except Exception:
             pass # Mantém UTC se a conversão falhar
        # Remove duplicatas baseado no ID único do evento
        df_events.drop_duplicates(subset=['id'], inplace=True, ignore_index=True)
        log.info(f"Processamento concluído. {len(df_events)} eventos únicos encontrados.")
    
    log.info("--- Extração de EVENTS Concluída ---")
    return df_events

# --- 4. FUNÇÃO DE CARGA (LOAD) ---

def load_to_postgres(df: pd.DataFrame, table_name: str, engine, if_exists: str = "replace", schema: str = "public"):
    """Função genérica de carga para o Postgres."""
    if df.empty:
        log.warning(f"DataFrame para '{table_name}' está vazio. Pulando carga.")
        return
    
    log.info(f"Iniciando carga de {len(df)} linhas para '{table_name}' (modo: {if_exists})...")
    
    # Limpa colunas de fuso horário (necessário para o to_sql)
    for col in df.select_dtypes(include=['datetimetz']):
        df[col] = df[col].dt.tz_convert(None)
    
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists, # Usaremos 'replace' para o Full Refresh
            index=False,
            schema=schema
        )
        log.info(f"Carga para {table_name} concluída.")
    except Exception as e:
        log.error(f"FALHA AO CARREGAR no Postgres: {e}", exc_info=True)


# --- 5. FUNÇÃO PRINCIPAL (ORQUESTRADOR) ---

def main_run_events_only():
    """Orquestra a extração e carga APENAS do endpoint de Events."""
    log.info("--- INICIANDO SCRIPT DE CARGA COMPLETA (FULL REFRESH) - APENAS EVENTS ---")
    
    try:
        engine = create_engine(POSTGRES_CONN_STRING)
        log.info(f"Conexão com o banco de dados Postgres ({POSTGRES_CONN_STRING}) estabelecida.")
    except Exception as e:
        log.error(f"Falha ao conectar no Postgres: {e}")
        log.error("Verifique se o Docker Compose está rodando e se a porta 5433 está acessível.")
        return

    # --- Extração e Carga de Events ---
    try:
        # Roda a extração em modo Full Refresh (start_date=None, end_date=None)
        df_events = run_events_extraction(CRM_API_URL, CRM_API_TOKEN, start_date=None, end_date=None)
        
        # Carrega no banco usando 'replace' para garantir Full Refresh
        load_to_postgres(df_events, "stg_crm_events", engine, if_exists="replace")
        
        log.info("--- [SUCESSO] Extração e Carga de Events ---")

    except Exception as e:
        log.error(f"Falha na extração/carga de Events: {e}", exc_info=True)

    log.info("--- SCRIPT DE CARGA DE EVENTS CONCLUÍDO ---")

main_run_events_only()