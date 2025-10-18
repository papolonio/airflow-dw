# Conteúdo para: include/crm_extractor/leads_extractor.py
import requests
import pandas as pd
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Dict, Any, Callable, Optional, Tuple
from datetime import datetime
from requests.exceptions import JSONDecodeError

# Configurações
MAX_THREADS = 8
PAGE_LIMIT = 250
log = logging.getLogger(__name__)


def create_session_with_retries(token: str) -> requests.Session:

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
        if e.response.status_code == 404:
             log.info(f"Página não encontrada (404) em {url} com params {params}. Fim da paginação.")
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
        # time.sleep(1) # Descomente se a API for muito sensível

    if not all_dfs:
        log.warning(f"Nenhum dado encontrado para {base_url}")
        return pd.DataFrame()
    
    log.info(f"Concatenação final de {len(all_dfs)} dataframes de página.")
    return pd.concat(all_dfs, ignore_index=True)

# --- LEADS: Funções Específicas ---

def parse_leads_data(leads_items: List[Dict]) -> List[Dict]:
    """Parseia a lista de 'leads' da API em dicionários planos."""
    parsed_list = []
    for lead in leads_items:
        embedded = lead.get('_embedded', {})
        tags = embedded.get('tags', []) 
        companies = [c['id'] for c in embedded.get('companies', []) if 'id' in c]
        contacts = [c['id'] for c in embedded.get('contacts', []) if 'id' in c]
        loss_reason_list = embedded.get('loss_reason')
        loss_reason = [r['id'] for r in loss_reason_list if 'id' in r] if loss_reason_list else []

        parsed_list.append({
            'id': lead.get('id'),
            'name': lead.get('name'),
            'price': lead.get('price'),
            'responsible_user_id': lead.get('responsible_user_id'),
            'status_id': lead.get('status_id'),
            'pipeline_id': lead.get('pipeline_id'),
            'created_by': lead.get('created_by'),
            'updated_by': lead.get('updated_by'),
            'created_at': lead.get('created_at'),
            'updated_at': lead.get('updated_at'),
            'closed_at': lead.get('closed_at'),
            'tags_raw': tags,
            'companies_ids': companies,
            'loss_reason_ids': loss_reason,
            'contacts_ids': contacts,
            'custom_fields_values_raw': lead.get('custom_fields_values'),
        })
    return parsed_list

def post_process_leads_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aplica pós-processamento e explode os dados em 3 DFs."""
    if df.empty:
        log.warning("DataFrame de leads está vazio. Pulando pós-processamento.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    df_leads_all = df.copy()

    # --- 1. Conversão de Datas ---
    log.info("Processando: Conversão de datas...")
    time_cols = ['created_at', 'updated_at', 'closed_at']
    for col in time_cols:
        df_leads_all[col] = pd.to_datetime(df_leads_all[col], unit='s', utc=True, errors='coerce')
        # Tenta converter para São Paulo, se falhar (ex: NaT), mantém NaT
        try:
            df_leads_all[col] = df_leads_all[col].dt.tz_convert('America/Sao_Paulo')
        except Exception:
             pass # Mantém NaT ou o que quer que seja

    # --- 2. Conversão de Listas para Strings ---
    log.info("Processando: Conversão de listas de IDs para string...")
    list_cols = ['contacts_ids', 'companies_ids', 'loss_reason_ids']
    for col in list_cols:
        df_leads_all[col] = df_leads_all[col].apply(
            lambda x: ','.join(map(str, x)) if isinstance(x, list) and x else None
        )

    # --- 3. Processamento de Custom Fields ---
    log.info("Processando: Extração de Custom Fields...")
    df_cf = df_leads_all[['id', 'created_at', 'custom_fields_values_raw']].dropna(subset=['custom_fields_values_raw'])
    
    def _process_cf_values(cf_list):
        result = []
        if not isinstance(cf_list, list): return []
        for field in cf_list:
            field_name = field.get('field_name')
            for value in field.get('values', []):
                result.append({'field_name': field_name, 'value': value.get('value')})
        return result

    df_cf['processed'] = df_cf['custom_fields_values_raw'].apply(_process_cf_values)
    df_custom_fields = df_cf.explode('processed').dropna(subset=['processed'])
    
    if not df_custom_fields.empty:
        cf_normalized = pd.json_normalize(df_custom_fields['processed'])
        df_custom_fields = df_custom_fields[['id', 'created_at']].reset_index(drop=True).join(cf_normalized.reset_index(drop=True))
        df_custom_fields = df_custom_fields[['id', 'created_at', 'field_name', 'value']]
    else:
        df_custom_fields = pd.DataFrame(columns=['id', 'created_at', 'field_name', 'value'])

    # --- 4. Processamento de Tags ---
    log.info("Processando: Extração de Tags...")
    df_t = df_leads_all[['id', 'created_at', 'tags_raw']].dropna(subset=['tags_raw'])
    df_tags = df_t.explode('tags_raw').dropna(subset=['tags_raw'])
    
    if not df_tags.empty:
        tags_normalized = pd.json_normalize(df_tags['tags_raw'])
        df_tags = df_tags[['id', 'created_at']].reset_index(drop=True).join(tags_normalized[['name']].reset_index(drop=True))
        df_tags = df_tags.rename(columns={'name': 'tag_name'})
    else:
        df_tags = pd.DataFrame(columns=['id', 'created_at', 'tag_name'])
    
    # --- 5. Limpar DF principal ---
    df_leads = df_leads_all.drop(columns=['custom_fields_values_raw', 'tags_raw'], errors='ignore')
    
    log.info("Pós-processamento de Leads concluído.")
    return df_leads, df_custom_fields, df_tags

# --- FUNÇÃO PRINCIPAL DO EXTRATOR ---

def run_leads_extraction(
    base_url: str, 
    api_token: str, 
    start_date: Optional[datetime] = None, 
    end_date: Optional[datetime] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Função principal para extração de Leads.
    Orquestra a busca, o parse e o pós-processamento.
    Aceita datas para filtro incremental.
    """
    log.info("--- Iniciando Extração de LEADS ---")
    
    base_params = {
        "with": "loss_reason,contacts,companies"
    }
    if start_date and end_date:
        ts_from = int(start_date.timestamp())
        ts_to = int(end_date.timestamp())
        log.info(f"Execução incremental: de {start_date} (ts {ts_from}) até {end_date} (ts {ts_to})")
        
        # Filtro da API (ex: Kommo) por 'updated_at'
        base_params["filter[updated_at][from]"] = ts_from
        base_params["filter[updated_at][to]"] = ts_to
    else:
        log.warning("Execução em MODO FULL REFRESH (sem filtro de data).")
        
    session = create_session_with_retries(api_token)
    
    try:
        df_leads_all_raw = fetch_all_pages_concurrently(
            session=session,
            base_url=f"{base_url}/leads",
            base_params=base_params,
            embed_key="leads",
            parse_func=parse_leads_data
        )
    finally:
        session.close()
        log.info("Sessão de requests fechada.")
    
    if df_leads_all_raw.empty:
        log.warning("Nenhum lead foi baixado. Encerrando extração de leads.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    log.info(f"Total de {len(df_leads_all_raw)} leads brutos baixados. Iniciando pós-processamento...")
    
    df_leads, df_custom_fields, df_tags = post_process_leads_df(df_leads_all_raw)
    
    log.info("--- Extração de LEADS Concluída ---")
    return df_leads, df_custom_fields, df_tags