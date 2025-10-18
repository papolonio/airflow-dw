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
    run_pipelines_extraction,
    run_users_extraction,
    run_catalogs_extraction
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

# --- Funções Helper Reutilizáveis ---

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
    
    # Lógica de Full Refresh: Apaga e recarrega
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace", # TRUNCATE + INSERT
        index=False,
        schema=DW_SCHEMA
    )
    log.info(f"Carga para {table_name} concluída.")


# --- Definição Clássica da DAG ---
with DAG(
    dag_id="crm_dimensions_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",  # Todo dia às 3 da manhã
    catchup=False, # <-- MELHOR PRÁTICA: Full Refresh não faz backfill
    tags=["crm", "ingestion", "dimensions", "elt"],
    max_active_runs=1,
) as dag:
    """
    DAG para extrair dados dimensionais (Pipelines, Users, Catalogs) do CRM.
    Realiza um FULL REFRESH (Truncate + Insert) diário nas tabelas de staging.
    """

    # --- TaskGroup para Pipelines ---
    with TaskGroup(group_id="pipelines_group") as pipelines_group:
        @task(task_id="extract_pipelines")
        def extract_pipelines() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_pipelines_extraction(base_url, api_token)
            log.info(f"Extração concluída. Pipelines/Status: {len(df)}")
            return df

        @task(task_id="load_pipelines")
        def load_pipelines(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_pipelines_status")
            
        @task(task_id="transform_pipelines")
        def transform_pipelines():
            log.info("Placeholder: Rodando dbt model para dim_pipelines...")
            
        # Fluxo de dados (TaskFlow)
        extracted_df_pipelines = extract_pipelines()
        loaded_result_pipelines = load_pipelines(extracted_df_pipelines)
        loaded_result_pipelines >> transform_pipelines()

    # --- TaskGroup para Users ---
    with TaskGroup(group_id="users_group") as users_group:
        @task(task_id="extract_users")
        def extract_users() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_users_extraction(base_url, api_token)
            log.info(f"Extração concluída. Users: {len(df)}")
            return df
        
        @task(task_id="load_users")
        def load_users(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_users")

        @task(task_id="transform_users")
        def transform_users():
            log.info("Placeholder: Rodando dbt model para dim_users...")

        # Fluxo de dados (TaskFlow)
        extracted_df_users = extract_users()
        loaded_result_users = load_users(extracted_df_users)
        loaded_result_users >> transform_users()

    # --- TaskGroup para Catalogs ---
    with TaskGroup(group_id="catalogs_group") as catalogs_group:
        @task(task_id="extract_catalogs")
        def extract_catalogs() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_catalogs_extraction(base_url, api_token)
            log.info(f"Extração concluída. Catalog Elements: {len(df)}")
            return df

        @task(task_id="load_catalogs")
        def load_catalogs(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_catalog_elements")

        @task(task_id="transform_catalogs")
        def transform_catalogs():
            log.info("Placeholder: Rodando dbt model para dim_catalogs...")

        # Fluxo de dados (TaskFlow)
        extracted_df_catalogs = extract_catalogs()
        loaded_result_catalogs = load_catalogs(extracted_df_catalogs)
        loaded_result_catalogs >> transform_catalogs()

    # --- Definindo o Fluxo da DAG ---
    # As 3 extrações de dimensão rodam em paralelo
    [pipelines_group, users_group, catalogs_group]