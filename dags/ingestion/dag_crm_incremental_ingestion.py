import logging
from datetime import datetime, timedelta
import pandas as pd

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# Importa as funções específicas do nosso "motor"
from crm_extractor.extractor import (
    run_leads_extraction,
    run_events_extraction
)

log = logging.getLogger(__name__)

# --- Constantes ---
API_CONN_ID = "crm_kommo_api"
DW_CONN_ID = "postgres_dw"
DW_SCHEMA = "public" # Schema de Staging

# --- Argumentos Padrão da DAG ---
default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Funções Helper Reutilizáveis (Copiadas da outra DAG) ---

def _get_api_credentials():
    """Busca as credenciais da API do CRM."""
    log.info(f"Buscando credenciais da Conexão Airflow '{API_CONN_ID}'...")
    try:
        conn = BaseHook.get_connection(API_CONN_ID)
        api_token = conn.password
        base_url = conn.host
    except AirflowNotFoundException:
        log.error(f"ERRO CRÍTICO: Airflow Connection '{API_CONN_ID}' não foi encontrada.")
        raise
    if not api_token or not base_url:
         log.error(f"ERRO CRÍTICO: Conexão '{API_CONN_ID}' mal configurada.")
         raise ValueError(f"Conexão '{API_CONN_ID}' mal configurada.")
    return base_url, api_token

def _load_to_postgres(df: pd.DataFrame, table_name: str):
    """Função genérica de carga para o Postgres."""
    if df.empty:
        log.warning(f"DataFrame para '{table_name}' está vazio. Pulando carga.")
        return
    
    log.info(f"Iniciando carga de {len(df)} linhas para '{table_name}' no schema '{DW_SCHEMA}'...")
    try:
        pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
    except AirflowNotFoundException:
        log.error(f"ERRO CRÍTICO: Airflow Connection '{DW_CONN_ID}' não foi encontrada.")
        raise

    for col in df.select_dtypes(include=['datetimetz']):
        df[col] = df[col].dt.tz_convert(None)
    
    # MELHOR PRÁTICA: Lógica incremental usa 'append'
    # A etapa 'transform' será responsável pela de-duplicação (MERGE/UPSERT)
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append", # <-- Lógica INCREMENTAL
        index=False,
        schema=DW_SCHEMA
    )
    log.info(f"Carga para {table_name} concluída.")

# --- Definição Clássica da DAG ---
with DAG(
    dag_id="crm_incremental_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 4 * * *",  # Todo dia às 4 da manhã
    catchup=True, # <-- MELHOR PRÁTICA: Incremental faz backfill
    tags=["crm", "ingestion", "facts", "elt"],
    max_active_runs=1,
) as dag:
    """
    DAG para extrair dados incrementais (Leads, Events) do CRM.
    Realiza um 'append' nas tabelas de staging.
    A etapa 'Transform' é responsável por aplicar o MERGE/UPSERT na tabela final.
    """

    # --- TaskGroup para Leads ---
    with TaskGroup(group_id="leads_group") as leads_group:
        @task(task_id="extract_leads")
        def extract_leads(data_interval_start: str, data_interval_end: str) -> dict:
            base_url, api_token = _get_api_credentials()
            start_date_dt = datetime.fromisoformat(data_interval_start)
            end_date_dt = datetime.fromisoformat(data_interval_end)

            log.info(f"Iniciando extração de Leads para o intervalo: {start_date_dt} a {end_date_dt}")
            df_leads, df_custom_fields, df_tags = run_leads_extraction(
                base_url=base_url,
                api_token=api_token,
                start_date=start_date_dt,
                end_date=end_date_dt
            )
            log.info(f"Extração concluída. Leads: {len(df_leads)}, CFs: {len(df_custom_fields)}, Tags: {len(df_tags)}")
            return {
                "leads": df_leads,
                "custom_fields": df_custom_fields,
                "tags": df_tags
            }

        @task(task_id="load_leads")
        def load_leads(dataframes_dict: dict):
            _load_to_postgres(dataframes_dict['leads'], "stg_crm_leads")
            _load_to_postgres(dataframes_dict['custom_fields'], "stg_crm_leads_custom_fields")
            _load_to_postgres(dataframes_dict['tags'], "stg_crm_leads_tags")

        @task(task_id="transform_leads")
        def transform_leads():
            log.info("Placeholder: Rodando dbt model para fato_leads e de-duplicação (MERGE)...")

        # Fluxo de dados (TaskFlow)
        extracted_data_leads = extract_leads(
            data_interval_start="{{ data_interval_start }}",
            data_interval_end="{{ data_interval_end }}"
        )
        loaded_result_leads = load_leads(extracted_data_leads)
        loaded_result_leads >> transform_leads()

    # --- TaskGroup para Events ---
    with TaskGroup(group_id="events_group") as events_group:
        @task(task_id="extract_events")
        def extract_events(data_interval_start: str, data_interval_end: str) -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            start_date_dt = datetime.fromisoformat(data_interval_start)
            end_date_dt = datetime.fromisoformat(data_interval_end)
            
            log.info(f"Iniciando extração de Events para o intervalo: {start_date_dt} a {end_date_dt}")
            df_events = run_events_extraction(
                base_url=base_url,
                api_token=api_token,
                start_date=start_date_dt,
                end_date=end_date_dt
            )
            log.info(f"Extração concluída. Events: {len(df_events)}")
            return df_events

        @task(task_id="load_events")
        def load_events(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_events")

        @task(task_id="transform_events")
        def transform_events():
            log.info("Placeholder: Rodando dbt model para fato_events...")

        # Fluxo de dados (TaskFlow)
        extracted_data_events = extract_events(
            data_interval_start="{{ data_interval_start }}",
            data_interval_end="{{ data_interval_end }}"
        )
        loaded_result_events = load_events(extracted_data_events)
        loaded_result_events >> transform_events()

    # --- Definindo o Fluxo da DAG ---
    # Os dois grupos de fatos rodam em paralelo
    [leads_group, events_group]