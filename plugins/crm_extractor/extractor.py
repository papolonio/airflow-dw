# Conteúdo COMPLETO para: plugins/crm_extractor/extractor.py
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
MAX_THREADS = 2
PAGE_LIMIT = 250
log = logging.getLogger(__name__)


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
        # Status 204 (No Content) é esperado quando uma página não tem dados
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


# --- 2. LÓGICA DO ENDPOINT: LEADS (O que já tínhamos) ---

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
        try:
            df_leads_all[col] = df_leads_all[col].dt.tz_convert('America/Sao_Paulo')
        except Exception:
             pass

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

def run_leads_extraction(
    base_url: str, 
    api_token: str, 
    start_date: Optional[datetime] = None, 
    end_date: Optional[datetime] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Função principal para extração de Leads."""
    log.info("--- Iniciando Extração de LEADS ---")
    
    base_params = {
        "with": "loss_reason,contacts,companies"
    }
    if start_date and end_date:
        ts_from = int(start_date.timestamp())
        ts_to = int(end_date.timestamp())
        log.info(f"Execução incremental: de {start_date} (ts {ts_from}) até {end_date} (ts {ts_to})")
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


# --- 3. LÓGICA DO ENDPOINT: EVENTS (NOVO) ---

def parse_events_data(events_items: List[Dict]) -> List[Dict]:
    """Função de parse específica para o endpoint /events."""
    parsed_list = []
    for event in events_items:
        try:
            if event.get("type") != 'lead_status_changed':
                continue
                
            value_after = event.get('value_after', [{}])[0]
            lead_status = value_after.get('lead_status', {})
            lead_status_after = lead_status.get('id')
            pipeline = lead_status.get('pipeline_id')
            
            if not lead_status_after:
                continue

            parsed_list.append({
                "id": event.get("id"),
                "type": event.get("type"),
                "entity_id": event.get("entity_id"),
                "created_at": event.get("created_at"),
                "status_id": lead_status_after,
                'pipeline_id': pipeline
            })
        except (IndexError, TypeError, AttributeError) as e:
            log.warning(f"Erro ao parsear evento ID {event.get('id')}: {e}. Pulando.")
    return parsed_list

def run_events_extraction(
    base_url: str, 
    api_token: str, 
    start_date: Optional[datetime] = None, 
    end_date: Optional[datetime] = None
) -> pd.DataFrame:
    """Função principal para extração de Events."""
    log.info("--- Iniciando Extração de EVENTS ---")
    
    base_params = {'filter[type]': 'lead_status_changed'}
    if start_date and end_date:
        ts_from = int(start_date.timestamp())
        ts_to = int(end_date.timestamp())
        log.info(f"Execução incremental: de {start_date} (ts {ts_from}) até {end_date} (ts {ts_to})")
        base_params['filter[created_at][from]'] = ts_from
        base_params['filter[created_at][to]'] = ts_to
    else:
        log.warning("Execução em MODO FULL REFRESH (sem filtro de data).")

    session = create_session_with_retries(api_token)
    try:
        df_events = fetch_all_pages_concurrently(
            session=session,
            base_url=f"{base_url}/events",
            base_params=base_params,
            embed_key="events",
            parse_func=parse_events_data
        )
    finally:
        session.close()

    if not df_events.empty:
        log.info(f"Total de {len(df_events)} eventos baixados. Processando...")
        df_events['created_at'] = pd.to_datetime(df_events['created_at'], unit='s', utc=True, errors='coerce')
        df_events['created_at'] = df_events['created_at'].dt.tz_convert('America/Sao_Paulo')
        df_events.drop_duplicates(subset=['id'], inplace=True, ignore_index=True)
    
    log.info("--- Extração de EVENTS Concluída ---")
    return df_events

# --- 4. LÓGICA DO ENDPOINT: PIPELINES (NOVO) ---

def parse_pipelines_data(pipelines_items: List[Dict]) -> List[Dict]:
    """Parseia /leads/pipelines, explodindo 'statuses'."""
    parsed_list = []
    for pipeline in pipelines_items:
        pipeline_id = pipeline.get("id")
        pipeline_name = pipeline.get("name")
        statuses = pipeline.get('_embedded', {}).get('statuses', [])
        for status in statuses:
            parsed_list.append({
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "status_id": status.get('id'),
                "status_name": status.get('name'),
                "status_sort": status.get('sort'),
                "status_color": status.get('color'),
            })
    return parsed_list

def run_pipelines_extraction(base_url: str, api_token: str) -> pd.DataFrame:
    """Função principal para extração de Pipelines. (Não paginado)"""
    log.info("--- Iniciando Extração de PIPELINES ---")
    url = f"{base_url}/leads/pipelines"
    session = create_session_with_retries(api_token)
    
    try:
        data = fetch_api_page(session, url, params={})
    finally:
        session.close()
    
    df_pipelines_status = pd.DataFrame()
    if data:
        items = data.get("_embedded", {}).get("pipelines", [])
        if items:
            parsed_data = parse_pipelines_data(items)
            df_pipelines_status = pd.DataFrame(parsed_data)
            df_pipelines_status.drop_duplicates(subset=['status_id'], inplace=True, ignore_index=True)
        else:
            log.warning("Nenhum pipeline encontrado.")
    else:
        log.error("Falha ao buscar dados de pipelines.")
        
    log.info(f"--- Extração de PIPELINES Concluída. {len(df_pipelines_status)} status encontrados. ---")
    return df_pipelines_status

# --- 5. LÓGICA DO ENDPOINT: USERS (NOVO) ---

def parse_users_data(users_items: List[Dict]) -> List[Dict]:
    """Função de parse específica para /users."""
    return [
        {"id": user.get("id"), "name": user.get("name")}
        for user in users_items if user.get("id")
    ]

def run_users_extraction(base_url: str, api_token: str) -> pd.DataFrame:
    """Função principal para extração de Users. (Paginado)"""
    log.info("--- Iniciando Extração de USERS ---")
    session = create_session_with_retries(api_token)
    try:
        df_users = fetch_all_pages_concurrently(
            session=session,
            base_url=f"{base_url}/users",
            base_params={},
            embed_key="users",
            parse_func=parse_users_data
        )
    finally:
        session.close()
    
    log.info(f"--- Extração de USERS Concluída. {len(df_users)} usuários encontrados. ---")
    return df_users

# --- 6. LÓGICA DO ENDPOINT: CATALOGS (NOVO) ---

def parse_catalogs_data(catalogs_items: List[Dict]) -> List[Dict]:
    """Parseia a lista de catálogos-pai."""
    return [
        {"id": c.get("id"), "name": c.get("name"), "type": c.get("type")}
        for c in catalogs_items if c.get("id")
    ]

def parse_catalog_elements_data(elements_items: List[Dict], catalog_id: int) -> List[Dict]:
    """Parseia os elementos de um catálogo específico."""
    parsed_list = []
    for element in elements_items:
        custom_fields = element.get('custom_fields_values', [])
        if not custom_fields:
            continue
            
        for field in custom_fields:
            parsed_list.append({
                "id": element.get('id'),
                "name": element.get('name'),
                "catalog_id": catalog_id,
                "field_id": field.get('field_id'),
                "field_name": field.get('field_name', ''),
                "value": field.get('values')[0].get('value') if field.get('values') else None,
            })
    return parsed_list

def fetch_elements_for_one_catalog(session: requests.Session, base_url: str, catalog_id: int) -> List[Dict]:
    """Worker que busca TODOS os elementos de UM catálogo (paginação sequencial)."""
    all_elements = []
    page = 1
    url = f"{base_url}/catalogs/{catalog_id}/elements"
    log.info(f"CATALOGS: Iniciando busca para catalog_id {catalog_id}...")
    
    while True:
        params = {'page': page, 'limit': PAGE_LIMIT}
        data = fetch_api_page(session, url, params)
        if not data: break
        items = data.get("_embedded", {}).get("elements", [])
        if not items: break
            
        all_elements.extend(parse_catalog_elements_data(items, catalog_id))
        page += 1
        
    log.info(f"CATALOGS: Concluída busca para catalog_id {catalog_id}. Total: {len(all_elements)} elementos.")
    return all_elements

def run_catalogs_extraction(base_url: str, api_token: str) -> pd.DataFrame:
    """Função principal para extração de Catalogs e seus Elementos."""
    log.info("--- Iniciando Extração de CATALOGS ---")
    session = create_session_with_retries(api_token)
    try:
        # 1. Buscar todos os catálogos-pai (em paralelo)
        df_catalogs_list = fetch_all_pages_concurrently(
            session=session,
            base_url=f"{base_url}/catalogs",
            base_params={},
            embed_key="catalogs",
            parse_func=parse_catalogs_data
        )
        
        if df_catalogs_list.empty:
            log.warning("Nenhum catálogo encontrado. Encerrando.")
            return pd.DataFrame()

        catalog_ids = df_catalogs_list["id"].tolist()
        log.info(f"Encontrados {len(catalog_ids)} catálogos. Buscando elementos em paralelo...")
        
        # 2. Buscar elementos para cada catálogo (em paralelo)
        all_elements_data = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {
                executor.submit(fetch_elements_for_one_catalog, session, base_url, cid): cid
                for cid in catalog_ids
            }
            for future in as_completed(futures):
                try:
                    elements_list = future.result()
                    if elements_list:
                        all_elements_data.extend(elements_list)
                except Exception as e:
                    cid = futures[future]
                    log.error(f"Erro ao processar elementos do catalog_id {cid}: {e}", exc_info=True)
                    
        if not all_elements_data:
            log.warning("Nenhum elemento de catálogo foi encontrado.")
            return pd.DataFrame()

        df_catalogs_elements = pd.DataFrame(all_elements_data)
        
    finally:
        session.close()
        
    log.info(f"--- Extração de CATALOGS Concluída. {len(df_catalogs_elements)} elementos encontrados. ---")
    return df_catalogs_elements