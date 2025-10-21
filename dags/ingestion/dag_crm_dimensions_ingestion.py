import logging
from datetime import datetime, timedelta
import pandas as pd

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Importa o operador necessário para rodar SQL
from airflow.providers.postgres.operators.postgres import PostgresOperator # <-- ADICIONADO
from airflow.utils.task_group import TaskGroup

# Importa as funções de extração
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

# --- Argumentos Padrão ---
default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Funções Helper ---
def _get_api_credentials():
    log.info(f"Buscando credenciais da Conexão Airflow '{API_CONN_ID}'...")
    try:
        conn = BaseHook.get_connection(API_CONN_ID)
        api_token = conn.password
        base_url = conn.host
    except AirflowNotFoundException:
        log.error(f"ERRO CRÍTICO: Conexão '{API_CONN_ID}' não encontrada.")
        raise
    if not api_token or not base_url:
         log.error(f"ERRO CRÍTICO: Conexão '{API_CONN_ID}' mal configurada.")
         raise ValueError(f"Conexão '{API_CONN_ID}' mal configurada.")
    return base_url, api_token

def _load_to_postgres(df: pd.DataFrame, table_name: str):
    if df.empty:
        log.warning(f"DataFrame para '{table_name}' vazio. Pulando carga.")
        return
    log.info(f"Iniciando carga de {len(df)} linhas para '{table_name}' no schema '{DW_SCHEMA}' (modo: replace)...")
    try:
        pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
    except AirflowNotFoundException:
        log.error(f"ERRO CRÍTICO: Conexão '{DW_CONN_ID}' não encontrada.")
        raise
    for col in df.select_dtypes(include=['datetimetz']):
        df[col] = df[col].dt.tz_convert(None)
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False, schema=DW_SCHEMA)
    log.info(f"Carga para {table_name} concluída.")

# --- Definição da DAG ---
with DAG(
    dag_id="crm_dimensions_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 1, 1), # <-- Note que sua start_date original era 2025
    schedule="0 3 * * *",
    catchup=False,
    tags=["crm", "ingestion", "dimensions", "elt"],
    max_active_runs=1,
    # Informa ao Airflow onde procurar pelos arquivos .sql
    template_searchpath="/opt/airflow/dags" # <-- ADICIONADO
) as dag:
    """
    DAG para extrair dados dimensionais (Pipelines, Users, Catalogs) do CRM.
    Realiza um FULL REFRESH (Replace) diário nas tabelas de staging.
    Executa a transformação para carregar as tabelas dimensionais finais.
    """

    # --- Grupo: Pipelines ---
    with TaskGroup(group_id="pipelines_group") as pipelines_group:
        @task(task_id="extract_pipelines")
        def extract_pipelines() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_pipelines_extraction(base_url, api_token)
            log.info(f"Extração Pipelines/Status: {len(df)}")
            return df

        @task(task_id="load_pipelines")
        def load_pipelines(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_pipelines_status")

        # Task de Transformação - Executa o SQL
        transform_pipelines = PostgresOperator(
            task_id="transform_dim_pipelines", # Nome mais descritivo
            postgres_conn_id=DW_CONN_ID,
            # Caminho relativo à pasta 'dags' (definido no template_searchpath)
            sql="sql/transform_dim_pipelines.sql"
        )

        # Define o fluxo E -> L -> T
        extract_pipelines() >> load_pipelines() >> transform_pipelines

    # --- Grupo: Users ---
    with TaskGroup(group_id="users_group") as users_group:
        @task(task_id="extract_users")
        def extract_users() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_users_extraction(base_url, api_token)
            log.info(f"Extração Users: {len(df)}")
            return df

        @task(task_id="load_users")
        def load_users(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_users")

        # Task de Transformação - Executa o SQL
        transform_users = PostgresOperator(
            task_id="transform_dim_users", # Nome mais descritivo
            postgres_conn_id=DW_CONN_ID,
            sql="sql/transform_dim_users.sql"
        )

        # Define o fluxo E -> L -> T
        extract_users() >> load_users() >> transform_users

    # --- Grupo: Catalogs ---
    with TaskGroup(group_id="catalogs_group") as catalogs_group:
        @task(task_id="extract_catalogs")
        def extract_catalogs() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_catalogs_extraction(base_url, api_token)
            log.info(f"Extração Catalog Elements: {len(df)}")
            return df

        @task(task_id="load_catalogs")
        def load_catalogs(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_catalog_elements")

        # Placeholder - SQL para catálogos não foi criado
        @task(task_id="transform_catalogs")
        def transform_catalogs():
            log.info("Placeholder: Rodando SQL/dbt para dim_catalogs...")

        # Define o fluxo E -> L -> T (placeholder)
        extract_catalogs() >> load_catalogs() >> transform_catalogs()

    # Define que os grupos rodam em paralelo
    [pipelines_group, users_group, catalogs_group]