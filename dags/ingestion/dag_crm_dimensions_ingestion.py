import logging
from datetime import datetime, timedelta
import pandas as pd

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from crm_extractor.extractor import (
    run_pipelines_extraction,
    run_users_extraction,
    run_catalogs_extraction
)

log = logging.getLogger(__name__)

API_CONN_ID = "crm_kommo_api"
DW_CONN_ID = "postgres_dw"
DW_SCHEMA = "public"

default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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

    for col in df.select_dtypes(include=["datetimetz"]):
        df[col] = df[col].dt.tz_convert(None)

    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False, schema=DW_SCHEMA)
    log.info(f"Carga para {table_name} concluída.")


with DAG(
    dag_id="crm_dimensions_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    tags=["crm", "ingestion", "dimensions", "elt"],
    max_active_runs=1,
    template_searchpath="/opt/airflow/dags"
) as dag:
    """
    DAG para extrair dados dimensionais (Pipelines, Users, Catalogs) do CRM.
    Realiza um FULL REFRESH (Replace) diário nas tabelas de staging.
    Executa a transformação para carregar as tabelas dimensionais finais.
    """

    # ===================== PIPELINES =====================
    with TaskGroup(group_id="pipelines_group") as pipelines_group:

        # --- CORREÇÃO APLICADA AQUI ---
        @task(task_id="extract_pipelines", show_return_value_in_logs=False)
        def extract_pipelines() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_pipelines_extraction(base_url, api_token)
            log.info(f"Extração Pipelines/Status: {len(df)} registros.")
            return df

        @task(task_id="load_pipelines")
        def load_pipelines(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_pipelines_status")

        transform_pipelines_task = PostgresOperator(
            task_id="transform_dim_pipelines",
            postgres_conn_id=DW_CONN_ID,
            sql="sql/transform_dim_pipelines.sql"
        )

        extracted_df_pipelines = extract_pipelines()
        loaded_pipelines = load_pipelines(extracted_df_pipelines)
        loaded_pipelines >> transform_pipelines_task

    with TaskGroup(group_id="users_group") as users_group:

        @task(task_id="extract_users", show_return_value_in_logs=False)
        def extract_users() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_users_extraction(base_url, api_token)
            log.info(f"Extração Users: {len(df)} registros.")
            return df

        @task(task_id="load_users")
        def load_users(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_users")

        transform_users_task = PostgresOperator(
            task_id="transform_dim_users",
            postgres_conn_id=DW_CONN_ID,
            sql="sql/transform_dim_users.sql"
        )

        extracted_df_users = extract_users()
        loaded_users = load_users(extracted_df_users)
        loaded_users >> transform_users_task

    with TaskGroup(group_id="catalogs_group") as catalogs_group:

        @task(task_id="extract_catalogs", show_return_value_in_logs=False)
        def extract_catalogs() -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            df = run_catalogs_extraction(base_url, api_token)
            log.info(f"Extração Catalog Elements: {len(df)} registros.")
            return df

        @task(task_id="load_catalogs")
        def load_catalogs(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_catalog_elements")

        @task(task_id="transform_catalogs")
        def transform_catalogs():
            log.info("Executando transformação de catálogos (placeholder ou SQL/dbt)...")

        extracted_df_catalogs = extract_catalogs()
        loaded_result_catalogs = load_catalogs(extracted_df_catalogs)
        
        transform_catalogs_task = transform_catalogs()
        loaded_result_catalogs >> transform_catalogs_task

    [pipelines_group, users_group, catalogs_group]