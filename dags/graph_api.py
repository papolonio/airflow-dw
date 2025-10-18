from airflow import DAG
from airflow.operators.python import PythonOperator  # ✅ novo import
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta, date

import pandas as pd
import requests
import hashlib
import time
import json
from sqlalchemy import create_engine, text, event
from urllib.parse import quote_plus

# 🔗 Conexões
POSTGRES_HOST = "147.79.83.185"
POSTGRES_PORT = "7711"
POSTGRES_USER = "dataup_user"
POSTGRES_PASSWORD = "D9wx7LlpR4PqT2zV"
POSTGRES_DATABASE = "dataup_integration"
POSTGRES_SCHEMA = "grax_midia"

CONNECTION_STRING_POSTGRES = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
)

PYODBC_URL_DATABASE = (
    'Driver={ODBC Driver 18 for SQL Server};'
    'Server=tcp:dataup-server.database.windows.net,1433;'
    'Database=grax-midia-db;'
    'Uid=grax_midia_user;'
    'Pwd=GdçuyM!d1a2025g*;'
    'Encrypt=yes;'
    'TrustServerCertificate=no;'
    'Connection Timeout=30;'
)

CONNECTION_STRING_SQLSERVER = f"mssql+pyodbc:///?odbc_connect={quote_plus(PYODBC_URL_DATABASE)}"


# 🔥 Engines
postgres_engine = create_engine(CONNECTION_STRING_POSTGRES)
sqlserver_engine = create_engine(CONNECTION_STRING_SQLSERVER)

@event.listens_for(sqlserver_engine, "before_cursor_execute")
def enable_fast_executemany(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        cursor.fast_executemany = True

tokens = [
    "EAAJ3wFZCreXwBOylUZAyar2QLjO65iZC2LZCBQDlEQyEmFdOKLx87dxVAmKZBJaHpiA3YZBVu1bSkdIXNiAbLG91J1NRGZAuVSOocU1MPJS2zLZCZADnewZCQ3a7CsumSRenAz2RLgfhYOAcChON5ZC7cES5GbVbloambfC7pFunfQf5WH5CIeM5NvZCYq8NkCNANHQyD8ovJg6yoCx9J3UDzbrEtLBh",
    "EAAjVjOOk1CoBO3QpzNtIpDACfrTqxpkGZBFyyU7KmCaHVM2Y3hvflvpkX91dsHNYZCz8EdyuESZBniUGpkbvzqIKwmi8fMhiAC0F7bFKRnZBr7KYVjSK7u1WMHvSKUVFAbehZCSfh95zssE8GAl6QHBA9R29rPSbB2TwOGrZAut66lZBgx7bZAIx3xiNMuYZCMKPp7XrLsk8KpnSlgVh6XAovMapT",
    "EAAITnBfySbsBO0MPKTFsN5QuK7BpZAHQ3jvKAtkmydC3f2nkpZCzwJaZC6uhSQQLLGhEzvKA2f629bZCZC01uclaLd7DK7TWjQcmQUs12HZCSmBBMhU4irmogemxpelNCrczSQgTRyGUIFul1nm2eAEI14vV7CaG7VyaRHfcE4O1rcvLpAKeHZCGn5blZCdzu4lHOAZCWZCYZB0f0XwftiHdjd038Y9",
    "EAAOmGk5JvhIBO6ZC987kyOUWqtzGCvoqaMG02loVGKCJZCoQ2NZBBZAFsZCwh7j5Q6VMm1JvkMq9dY7emNGV7oKR9G1pdDqrZCycVBZBYNSKbfEqlaGMdnJD4WKHfVnWvij9Il2FZCUAJZAMkCRfZCrXy8fj7v89aoVeuJqGoxMDwEoNShrO3hiWvgi3BJwZCGOpfXS1HxBO1AhtY7ARWBmeHsSypyf",
    "EAAJ3wFZCreXwBOylUZAyar2QLjO65iZC2LZCBQDlEQyEmFdOKLx87dxVAmKZBJaHpiA3YZBVu1bSkdIXNiAbLG91J1NRGZAuVSOocU1MPJS2zLZCZADnewZCQ3a7CsumSRenAz2RLgfhYOAcChON5ZC7cES5GbVbloambfC7pFunfQf5WH5CIeM5NvZCYq8NkCNANHQyD8ovJg6yoCx9J3UDzbrEtLBh",
]

accounts = [
    {"account_id": "737382221885689", "table": "bm_01"},
    {"account_id": "3796096747377162", "table": "bm_02"},
    {"account_id": "306032042586782", "table": "bm_03"},
    {"account_id": "800915088760916", "table": "bm_04"},
    {"account_id": "593585518866085", "table": "bm_05"},
    {"account_id": "1083239526710286", "table": "bm_06"},
    {"account_id": "933442988635016", "table": "bm_07"},
    {"account_id": "520195120646787", "table": "bm_08"},
    {"account_id": "890830872680175", "table": "bm_09"},
    {"account_id": "851825113757827", "table": "bm_10"},
]


# 🔥 Função hash
def generate_unique_id(row):
    concatenated = "_".join(row.astype(str))
    return hashlib.md5(concatenated.encode()).hexdigest()


# 🔥 Função Upsert PostgreSQL
def upsert_dataframe(df, table_name, date_column, connection_string, schema):
    engine = create_engine(connection_string)

    if df.empty:
        print(f"⚠️ DataFrame vazio. Nada para inserir na tabela {schema}.{table_name}.")
        return

    min_date = df[date_column].min()
    max_date = df[date_column].max()

    print(f"🗑️ Deletando {schema}.{table_name} no intervalo de {min_date} até {max_date}...")

    delete_query = text(f"""
        DELETE FROM {schema}.{table_name}
        WHERE {date_column} BETWEEN :min_date AND :max_date
    """)

    with engine.begin() as conn:
        conn.execute(delete_query, {"min_date": min_date, "max_date": max_date})

    print(f"🚀 Inserindo {len(df)} registros na tabela {schema}.{table_name}...")

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )

    print(f"✅ {len(df)} registros inseridos na tabela {schema}.{table_name}!")


# 🔥 Função principal de extração da Graph API
def extract_meta_ads(account_id, token, table_name, actions_table_name, schema=POSTGRES_SCHEMA):
    URL_BASE_INSIGHTS = f"https://graph.facebook.com/v19.0/act_{account_id}/insights"
    URL_BASE_CAMPAIGNS = f"https://graph.facebook.com/v19.0/act_{account_id}/campaigns"

    def get_time_range():
        today = date.today()
        start_date = today - timedelta(days=15)
        return {"since": str(start_date), "until": str(today)}

    def paginate_api(url, params):
        all_data = []
        next_url = url
        page_count = 1

        print(f"📡 Iniciando requisição: {url}")

        while next_url:
            try:
                response = requests.get(next_url, params=params)
                if response.status_code != 200:
                    print(f"❌ Erro {response.status_code}: {response.text}")
                    if "too many calls" in response.text:
                        print("⏳ Rate limit, aguardando 60 segundos...")
                        time.sleep(60)
                        continue
                    break

                json_data = response.json()
                data = json_data.get('data', [])
                all_data.extend(data)

                print(f"✅ Página {page_count} - {len(data)} itens")
                next_url = json_data.get('paging', {}).get('next')
                page_count += 1

            except requests.exceptions.RequestException as e:
                print(f"❌ Erro na requisição: {e}")
                break

        return all_data

    def get_insights():
        params = {
            "time_increment": 1,
            "time_range": json.dumps(get_time_range()),
            "level": "ad",
            "fields": "ad_name,ad_id,adset_name,adset_id,campaign_name,campaign_id,account_id,account_name,spend,clicks,inline_link_clicks,impressions,objective,actions,date_start",
            "access_token": token
        }
        return paginate_api(URL_BASE_INSIGHTS, params)

    def get_campaigns():
        params = {
            "fields": "id,name,status,start_time",
            "access_token": token
        }
        return paginate_api(URL_BASE_CAMPAIGNS, params)

    print("🚀 Iniciando extração...")
    insights = get_insights()
    campaigns = get_campaigns()
    print("✅ Extração concluída.")

    adset_data, actions_data, campaigns_data = [], [], []

    for item in insights:
        adset_info = {
            'account_id': account_id,
            'account_name': item.get('account_name'),
            'adset_id': item.get('adset_id'),
            'adset_name': item.get('adset_name'),
            'ad_id': item.get('ad_id'),
            'ad_name': item.get('ad_name'),
            'campaign_id': item.get('campaign_id'),
            'campaign_name': item.get('campaign_name'),
            'objective': item.get('objective'),
            'spend': float(item.get('spend', 0)),
            'clicks': int(item.get('clicks', 0)),
            'inline_link_clicks': int(item.get('inline_link_clicks', 0)),
            'impressions': int(item.get('impressions', 0)),
            'date': item.get('date_start')
        }
        adset_data.append(adset_info)

        for action in item.get('actions', []):
            actions_data.append({
                'account_id': account_id,
                'ad_id': item.get('ad_id'),
                'action_type': action.get('action_type'),
                'value': int(action.get('value', 0)),
                'date': adset_info['date']
            })

    for campaign in campaigns:
        campaigns_data.append({
            'account_id': account_id,
            'campaign_id': campaign.get('id'),
            'campaign_name': campaign.get('name'),
            'campaign_status': campaign.get('status'),
            'start_time': campaign.get('start_time')
        })

    df_adset = pd.DataFrame(adset_data)
    df_actions = pd.DataFrame(actions_data)
    df_campaigns = pd.DataFrame(campaigns_data)

    df_ad = df_adset.merge(df_campaigns[['campaign_id', 'campaign_status']], on='campaign_id', how='left')

    if not df_ad.empty:
        df_ad["unique_id"] = df_ad.apply(generate_unique_id, axis=1)

    upsert_dataframe(df_ad, table_name, "date", CONNECTION_STRING_POSTGRES, schema)
    upsert_dataframe(df_actions, actions_table_name, "date", CONNECTION_STRING_POSTGRES, schema)

    print("🎯 Concluído com sucesso no PostgreSQL.")



def sync_postgres_to_sqlserver(**kwargs):
    from concurrent.futures import ThreadPoolExecutor

    def preprocess_dataframe(df):
        df = df.copy()
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        return df

    def get_sqlserver_columns(engine, schema, table):
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return [row[0] for row in result]

    def save_dataframe_to_sql_delete_insert(
        df: pd.DataFrame,
        connection_string,
        table_name,
        schema_name,
        date_column,
        days=15,
        chunksize=1000,
        max_threads=8
    ):
        df = preprocess_dataframe(df)
        df[date_column] = pd.to_datetime(df[date_column]).dt.date

        engine = create_engine(connection_string)

        # Validar colunas compatíveis
        sql_columns = get_sqlserver_columns(engine, schema_name, table_name)
        df = df[[col for col in df.columns if col in sql_columns]]

        # DELETE registros antigos
        start_date = (datetime.now() - timedelta(days=days)).date()
        end_date = datetime.now().date()
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    DELETE FROM {schema_name}.{table_name}
                    WHERE {date_column} BETWEEN :start AND :end
                """),
                {"start": start_date, "end": end_date}
            )
            print(f"🗑️ Deletados dados de {schema_name}.{table_name} entre {start_date} e {end_date}.")

        # Inserção por chunks
        chunks = [df.iloc[i:i+chunksize] for i in range(0, len(df), chunksize)]

        def insert_chunk(chunk):
            try:
                chunk.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
            except Exception as e:
                print(f"⚠️ Erro no chunk: {e}")

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            _ = [executor.submit(insert_chunk, chunk) for chunk in chunks]

        print(f"✅ Inseridos {len(df)} registros em {schema_name}.{table_name}")
        engine.dispose()

    engine_pg = create_engine(CONNECTION_STRING_POSTGRES)
    days = 15
    date_column = "date"
    schema_sql = "graph"

    tables = [
        {"view": "vw_graph_ads", "table": "graph_ads"},
        {"view": "vw_graph_ads_actions", "table": "graph_ads_actions"},
        # Adicione outras views/tabelas aqui se necessário
    ]

    for item in tables:
            view_name = item["view"]
            table_name = item["table"]
            print(f"\n🔄 Iniciando sincronização: {view_name} ➝ {schema_sql}.{table_name}")

            query = f"""
                SELECT * FROM {POSTGRES_SCHEMA}.{view_name}
                WHERE {date_column} >= '{(datetime.now() - timedelta(days=days)).date()}'
            """
            df = pd.read_sql(query, engine_pg)
            print(f"📥 Lidos {len(df)} registros da view {view_name}")

            if not df.empty:
                save_dataframe_to_sql_delete_insert(
                    df=df,
                    connection_string=CONNECTION_STRING_SQLSERVER,
                    table_name=table_name,
                    schema_name=schema_sql,
                    date_column=date_column,
                    days=days
                )
            else:
                print(f"⚠️ Nenhum dado encontrado para {view_name}")
        
with DAG(
    dag_id='graph_api_meta_ads',
    start_date=datetime.now() - timedelta(days=1),
    schedule="0 8,14,20 * * 1-6",
    catchup=False,
    default_args={'owner': 'Pedro', 'retries': 2, 'retry_delay': timedelta(minutes=5)},
    tags=['graph api', 'grax midia', 'meta ads', 'postgres', 'sqlserver'],
    max_active_runs=1,
) as dag:

    with TaskGroup(group_id="group_1", tooltip="Contas 1 até 5") as group_1:
        for idx, account in enumerate(accounts[:5]):
            PythonOperator(
                task_id=f"{account['table']}",
                python_callable=extract_meta_ads,
                op_kwargs={
                    'account_id': account['account_id'],
                    'token': tokens[idx],
                    'table_name': account['table'],
                    'actions_table_name': f"{account['table']}_actions"
                },
                trigger_rule=TriggerRule.ALL_DONE
            )

    with TaskGroup(group_id="group_2", tooltip="Contas 6 até 10") as group_2:
        for idx, account in enumerate(accounts[5:]):
            token = tokens[(idx + 5) % len(tokens)]
            PythonOperator(
                task_id=f"{account['table']}",
                python_callable=extract_meta_ads,
                op_kwargs={
                    'account_id': account['account_id'],
                    'token': token,
                    'table_name': account['table'],
                    'actions_table_name': f"{account['table']}_actions"
                },
                trigger_rule=TriggerRule.ALL_DONE
            )

    sync_graph_data_sqlserver = PythonOperator(
        task_id='sync_graph_data_to_sqlserver',
        python_callable=sync_postgres_to_sqlserver,
        trigger_rule=TriggerRule.ALL_DONE
    )

    group_1 >> group_2 >> sync_graph_data_sqlserver