import logging
from datetime import datetime, timedelta
import pandas as pd

# Importa o 'DAG' clássico e o '@task'
from airflow.models.dag import DAG
from airflow.decorators import task

from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

# Importa o "motor" que criamos
from crm_extractor.leads_extractor import run_leads_extraction

log = logging.getLogger(__name__)

# --- Argumentos Padrão da DAG ---
# (start_date é movido para dentro da definição da DAG)
default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Definição Clássica da DAG ---
# Esta sintaxe resolve o 'TypeError'
with DAG(
    dag_id="crm_leads_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1), # start_date é obrigatório aqui
    schedule="0 4 * * *",  # schedule_interval funciona aqui
    catchup=True,
    tags=["crm", "ingestion", "leads", "elt"],
    max_active_runs=1,
) as dag:
    """
    DAG para extrair dados de Leads do CRM (Kommo), carregá-los no
    Data Warehouse (Staging) e disparar a transformação (T).
    
    Depende de Conexões: 'crm_kommo_api', 'gcp_dw_service_account'
    Depende de Variáveis: 'CRM_STAGING_PROJECT_ID', 'CRM_STAGING_DATASET'
    """

    @task(task_id="extract_and_transform_api_data")
    def extract_and_transform(data_interval_start: datetime, data_interval_end: datetime) -> dict:
        """
        Tarefa E-T (Extract & Transform): Busca dados da API via Conexão Airflow.
        """
        log.info("Buscando credenciais da Conexão Airflow 'crm_kommo_api'...")
        try:
            conn = BaseHook.get_connection("crm_kommo_api")
            api_token = conn.password
            base_url = conn.host
        except AirflowNotFoundException:
            log.error("ERRO CRÍTICO: Airflow Connection 'crm_kommo_api' não foi encontrada.")
            raise
        
        if not api_token or not base_url:
             log.error("ERRO CRÍTICO: Conexão 'crm_kommo_api' encontrada, mas 'Host' (URL) ou 'Password' (Token) estão vazios.")
             raise ValueError("Conexão 'crm_kommo_api' mal configurada.")

        log.info(f"Iniciando extração para o intervalo: {data_interval_start} a {data_interval_end}")
        
        df_leads, df_custom_fields, df_tags = run_leads_extraction(
            base_url=base_url,
            api_token=api_token,
            start_date=data_interval_start,
            end_date=data_interval_end
        )
        
        log.info(f"Extração concluída. Leads: {len(df_leads)}, CFs: {len(df_custom_fields)}, Tags: {len(df_tags)}")
        
        return {
            "leads": df_leads,
            "custom_fields": df_custom_fields,
            "tags": df_tags
        }

    @task(task_id="load_to_data_warehouse")
    def load_to_dw(dataframes_dict: dict):
        """
        Tarefa L (Load): Carrega os DataFrames no Data Warehouse (BigQuery)
        usando a Conexão de Service Account e Variáveis.
        """
        log.info("Iniciando carga para o Data Warehouse...")
        
        try:
            gcp_conn_id = "gcp_dw_service_account"
            gcp_credentials = BaseHook.get_connection(gcp_conn_id).get_google_credentials()
            
            PROJECT_ID = Variable.get("CRM_STAGING_PROJECT_ID")
            STAGING_DATASET = Variable.get("CRM_STAGING_DATASET")
            
        except (AirflowNotFoundException, KeyError) as e:
            log.error(f"ERRO CRÍTICO: Falha ao carregar configuração do Airflow (Conexão ou Variável): {e}")
            raise

        # Define os nomes das tabelas AQUI
        TABLES = {
            "leads": f"{PROJECT_ID}.{STAGING_DATASET}.leads",
            "custom_fields": f"{PROJECT_ID}.{STAGING_DATASET}.leads_custom_fields",
            "tags": f"{PROJECT_ID}.{STAGING_DATASET}.leads_tags"
        }

        for table_key, df in dataframes_dict.items():
            if df.empty:
                log.warning(f"DataFrame para '{table_key}' está vazio. Pulando carga.")
                continue
            
            table_id = TABLES[table_key]
            log.info(f"Carregando {len(df)} linhas para a tabela {table_id}...")
            
            for col in df.select_dtypes(include=['datetimetz']):
                df[col] = df[col].dt.tz_convert(None)
            
            df.to_gbq(
                destination_table=table_id,
                project_id=PROJECT_ID,
                if_exists="replace",
                credentials=gcp_credentials,
            )
            log.info(f"Carga para {table_id} concluída.")
            
    @task(task_id="trigger_dbt_transform")
    def trigger_dbt_transform():
        """(Placeholder) Dispara a transformação."""
        log.info("Disparando transformações...")
        log.info("Transformações concluídas.")
        pass

    # --- Definindo o Fluxo da DAG ---
    
    extracted_data = extract_and_transform(
        data_interval_start="{{ data_interval_start }}",
        data_interval_end="{{ data_interval_end }}"
    )
    
    load_task = load_to_dw(extracted_data)
    transform_task = trigger_dbt_transform()
    
    load_task >> transform_task

# (Não há mais chamada de função no final, 
# a DAG é instanciada pelo 'with DAG(...)')