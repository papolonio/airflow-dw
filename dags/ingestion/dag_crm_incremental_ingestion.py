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
    run_leads_extraction,
    run_events_extraction
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
    log.info(f"Iniciando carga de {len(df)} linhas para '{table_name}' no schema '{DW_SCHEMA}' (modo: append)...")
    try:
        pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
    except AirflowNotFoundException:
        log.error(f"ERRO CRÍTICO: Conexão '{DW_CONN_ID}' não encontrada.")
        raise
    for col in df.select_dtypes(include=["datetimetz"]):
        df[col] = df[col].dt.tz_convert(None)
    df.to_sql(name=table_name, con=engine, if_exists="append", index=False, schema=DW_SCHEMA)
    log.info(f"Carga para {table_name} concluída.")

with DAG(
    dag_id="crm_incremental_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 4 * * *",
    catchup=True,
    tags=["crm", "ingestion", "facts", "elt"],
    max_active_runs=1,
    template_searchpath="/opt/airflow/dags"
) as dag:
    """
    DAG para extrair dados incrementais (Leads, Events) do CRM.
    Realiza um 'append' nas tabelas de staging.
    Executa a transformação para aplicar o MERGE/UPSERT nas tabelas finais.
    """

    with TaskGroup(group_id="leads_group") as leads_group:
        
        @task(task_id="extract_leads", show_return_value_in_logs=False)
        def extract_leads(data_interval_start: str, data_interval_end: str) -> dict:
            base_url, api_token = _get_api_credentials()
            start_date_dt = datetime.fromisoformat(data_interval_start)
            end_date_dt = datetime.fromisoformat(data_interval_end)
            log.info(f"Iniciando extração Leads intervalo: {start_date_dt} a {end_date_dt}")
            df_leads, df_custom_fields, df_tags = run_leads_extraction(
                base_url, api_token, start_date=start_date_dt, end_date=end_date_dt
            )
            log.info(f"Extração concluída. Leads: {len(df_leads)}, CFs: {len(df_custom_fields)}, Tags: {len(df_tags)}")
            return {"leads": df_leads, "custom_fields": df_custom_fields, "tags": df_tags}

        @task(task_id="load_leads")
        def load_leads(dataframes_dict: dict):
            _load_to_postgres(dataframes_dict['leads'], "stg_crm_leads")
            _load_to_postgres(dataframes_dict['custom_fields'], "stg_crm_leads_custom_fields")
            _load_to_postgres(dataframes_dict['tags'], "stg_crm_leads_tags")

        transform_fct_leads = PostgresOperator(
            task_id="transform_fct_leads",
            postgres_conn_id=DW_CONN_ID,
            sql="sql/transform_fct_leads.sql"
        )
        transform_fct_leads_cf = PostgresOperator(
            task_id="transform_fct_leads_custom_fields",
            postgres_conn_id=DW_CONN_ID,
            sql="sql/transform_fct_leads_custom_fields.sql"
        )
        transform_fct_leads_tags = PostgresOperator(
            task_id="transform_fct_leads_tags",
            postgres_conn_id=DW_CONN_ID,
            sql="sql/transform_fct_leads_tags.sql"
        )

        extracted_data = extract_leads(
            data_interval_start="{{ data_interval_start }}",
            data_interval_end="{{ data_interval_end }}"
        )
        loaded_data = load_leads(extracted_data)
        
        loaded_data >> [transform_fct_leads, transform_fct_leads_cf, transform_fct_leads_tags]

    with TaskGroup(group_id="events_group") as events_group:
        
        @task(task_id="extract_events", show_return_value_in_logs=False)
        def extract_events(data_interval_start: str, data_interval_end: str) -> pd.DataFrame:
            base_url, api_token = _get_api_credentials()
            start_date_dt = datetime.fromisoformat(data_interval_start)
            end_date_dt = datetime.fromisoformat(data_interval_end)
            log.info(f"Iniciando extração Events intervalo: {start_date_dt} a {end_date_dt}")
            df_events = run_events_extraction(
                base_url, api_token, start_date=start_date_dt, end_date=end_date_dt
            )
            log.info(f"Extração Events: {len(df_events)}")
            return df_events

        @task(task_id="load_events")
        def load_events(df: pd.DataFrame):
            _load_to_postgres(df, "stg_crm_events")

        transform_events = PostgresOperator(
            task_id="transform_fct_events",
            postgres_conn_id=DW_CONN_ID,
            sql="sql/transform_fct_events.sql"
        )

        extracted_data_ev = extract_events(
            data_interval_start="{{ data_interval_start }}",
            data_interval_end="{{ data_interval_end }}"
        )
        loaded_data_ev = load_events(extracted_data_ev)
        
        loaded_data_ev >> transform_events

    [leads_group, events_group]