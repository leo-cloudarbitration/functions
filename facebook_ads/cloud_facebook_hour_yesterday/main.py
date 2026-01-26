import functions_framework
import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
import logging
import time
import pytz
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import json
import base64
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CONFIGURAÇÃO DOS GRUPOS - Carregado de GitHub Secret ou arquivo JSON local
def load_groups_config():
    """
    Carrega a configuração de grupos:
    1. GitHub Secret (SECRET_FACEBOOK_GROUPS_CONFIG) - Produção/GitHub Actions
    2. Arquivo local (facebook_ads/groups_config.json) - Desenvolvimento local
    """
    try:
        # Opção 1: GitHub Secret (produção)
        secret_json = os.getenv("SECRET_FACEBOOK_GROUPS_CONFIG")
        if secret_json:
            logger.info("✅ Configuração de grupos carregada do SECRET_FACEBOOK_GROUPS_CONFIG (GitHub Secret)")
            groups = json.loads(secret_json)
            return groups
        
        # Opção 2: Arquivo local (desenvolvimento)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Sobe um nível (de cloud_facebook_hour_yesterday para facebook_ads)
        facebook_ads_dir = os.path.dirname(script_dir)
        config_path = os.path.join(facebook_ads_dir, "groups_config.json")
        
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                groups = json.load(f)
            logger.info(f"✅ Configuração de grupos carregada de arquivo local: {config_path}")
            return groups
        else:
            logger.warning(f"⚠️ Arquivo de configuração não encontrado: {config_path}")
            logger.warning("⚠️ Configure SECRET_FACEBOOK_GROUPS_CONFIG ou o arquivo groups_config.json")
            return {}
            
    except json.JSONDecodeError as e:
        logger.error(f"❌ Erro ao fazer parse do JSON: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao carregar configuração de grupos: {e}")
        raise

GROUPS = load_groups_config()

# Configurações de paralelismo
MAX_WORKERS = 15
REQUEST_DELAY = 0.35
ACCOUNT_DELAY = 0.7
RATE_LIMIT_DELAY = 30.0

# Tabela BigQuery única para todos os grupos
TABLE_ID = "data-v1-423414.test.cloud_facebook_hour_historical"

# Credenciais do Google Cloud via GitHub Secrets ou arquivo local
def get_bigquery_client():
    """
    Obtém cliente BigQuery:
    1. SECRET_GOOGLE_SERVICE_ACCOUNT (JSON string) - GitHub Actions
    2. Arquivo local (GOOGLE_APPLICATION_CREDENTIALS) - Desenvolvimento local
    3. Application Default Credentials - Fallback
    """
    try:
        # Opção 1: GitHub Actions Secret (SECRET_GOOGLE_SERVICE_ACCOUNT)
        credentials_json = os.getenv("SECRET_GOOGLE_SERVICE_ACCOUNT")
    if credentials_json:
            logger.info("✅ Usando credenciais do SECRET_GOOGLE_SERVICE_ACCOUNT (GitHub Actions)")
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        return bigquery.Client(credentials=credentials)
    
    # Opção 2: Arquivo local (GOOGLE_APPLICATION_CREDENTIALS)
    credentials_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_file and os.path.exists(credentials_file):
        logger.info(f"✅ Usando credenciais do arquivo: {credentials_file}")
        with open(credentials_file, 'r') as f:
            credentials_info = json.load(f)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        return bigquery.Client(credentials=credentials)
    
        # Opção 3: Application Default Credentials (fallback)
        logger.info("✅ Usando Application Default Credentials")
        return bigquery.Client()
        
    except Exception as e:
        logger.error(f"❌ Erro ao configurar BigQuery client: {e}")
    raise ValueError(
        "❌ Nenhuma credencial encontrada!\n"
        "Configure uma das opções:\n"
            "  - GitHub Actions: adicione o secret 'SECRET_GOOGLE_SERVICE_ACCOUNT'\n"
        "  - Local: defina GOOGLE_APPLICATION_CREDENTIALS apontando para seu arquivo JSON"
    )

# Inicializar cliente BigQuery
try:
    client = get_bigquery_client()
except Exception as e:
    logger.error(f"Erro ao configurar BigQuery client: {e}")
    client = None

# Verificar se os grupos do Facebook estão configurados
if not GROUPS:
    logger.error("Grupos do Facebook não estão configurados.")
else:
    total_tokens = len(set(group_config["token"] for group_config in GROUPS.values()))
    total_accounts = sum(len(group_config["accounts"]) for group_config in GROUPS.values())
    logger.info(f"Facebook API grupos configurados com sucesso! ({len(GROUPS)} grupos, {total_tokens} tokens únicos, {total_accounts} contas)")

# Verificar se o cliente BigQuery foi configurado
if client is None:
    logger.error("Cliente BigQuery não está configurado.")
else:
    logger.info("Cliente BigQuery configurado com sucesso!")

async def get_insights_async(session, account_id, access_token, after_cursor=None):
    """Função async para buscar insights do Facebook"""
    base_url = f"https://graph.facebook.com/v22.0/{account_id}/insights"
    fields = "account_name,account_id,campaign_id,campaign_name,date_start,date_stop,impressions,spend,ctr"
    params = {
        "date_preset": "yesterday",
        "fields": fields,
        "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
        "level": "campaign",
        "access_token": access_token,
        "limit": 25
    }

    if after_cursor:
        params["after"] = after_cursor

    try:
        async with session.get(base_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logger.error(f"Error fetching insights for account {account_id}: HTTP {response.status}")
                return None
    except Exception as e:
        logger.error(f"Error fetching insights for account {account_id}: {e}")
        return None

async def fetch_all_pages_async(session, account_id, access_token):
    """Função async para buscar todas as páginas de insights"""
    all_data = []
    after_cursor = None
    retries = 3

    while True:
        response_data = None
        for attempt in range(retries):
            response_data = await get_insights_async(session, account_id, access_token, after_cursor)
            if response_data is not None:
                break
            logger.info(f"Retrying... ({attempt + 1}/{retries})")
            await asyncio.sleep(2 ** attempt)  # Exponential backoff

        if response_data is None:
            logger.error(f"Failed to fetch data for account {account_id} after {retries} retries.")
            break

        all_data.extend(response_data.get('data', []))

        paging_info = response_data.get('paging', {})
        after_cursor = paging_info.get('cursors', {}).get('after')

        if not after_cursor:
            break

    return all_data

async def fetch_all_groups_async():
    """Função async para buscar dados de todos os grupos em paralelo"""
    logger.info(f"🔄 Processando {len(GROUPS)} grupos em paralelo para máxima velocidade")
    
    all_data = []
    
    # Criar sessão aiohttp com configurações otimizadas
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Processar cada grupo em paralelo
        tasks = []
        
        for group_name, group_config in GROUPS.items():
            task = process_group_async(session, group_name, group_config)
            tasks.append(task)
        
        # Executar todas as tarefas em paralelo
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Consolidar resultados
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Erro no processamento: {result}")
            else:
                all_data.extend(result)
    
    logger.info(f"📊 Total de dados coletados: {len(all_data)} registros")
    return all_data

async def process_group_async(session, group_name, group_config):
    """Processa um grupo específico com suas contas e token"""
    token = group_config["token"]
    accounts = group_config["accounts"]
    
    logger.info(f"🔄 [{group_name}] Processando {len(accounts)} contas...")
    all_data = []
    
    # Processar contas em lotes para evitar rate limiting
    batch_size = 5
    for i in range(0, len(accounts), batch_size):
        batch = accounts[i:i + batch_size]
        logger.info(f"📊 [{group_name}] Processando lote {i//batch_size + 1}: {len(batch)} contas")
        
        # Processar lote em paralelo
        tasks = []
        for account_id in batch:
            task = fetch_all_pages_async(session, account_id, token)
            tasks.append(task)
        
        # Executar lote em paralelo
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Consolidar resultados do lote
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"[{group_name}] Erro no processamento: {result}")
            else:
                all_data.extend(result)
        
        # Delay entre lotes para evitar rate limiting
        if i + batch_size < len(accounts):
            await asyncio.sleep(ACCOUNT_DELAY)
    
    logger.info(f"✅ [{group_name}] Processamento concluído: {len(all_data)} registros")
    return all_data

def process_hourly_data(all_data):
    try:
        if not all_data:
            logger.warning("No data to process.")
            return pd.DataFrame()

        # Cria o DataFrame inicial
        df = pd.DataFrame(all_data)
        logger.info(f"Initial data contains {df.shape[0]} rows.")

        processed_data = []

        for _, row in df.iterrows():
            hourly_stats = row.get('hourly_stats_aggregated_by_advertiser_time_zone', None)
            campaign_name = row.get('campaign_name', '')

            # Processa site_name (primeiros dois caracteres do campaign_name)
            site_name = campaign_name[:2] if len(campaign_name) >= 2 else 'N/A'

            # Extrai account_name e account_id
            account_name = row.get('account_name', 'N/A')
            account_id = row.get('account_id', 'N/A')

            # Processa country (entre o primeiro "_" e o segundo "_")
           # try:
           #     country_segment = campaign_name.split("_")[1]
           #     country = country_segment.lower() if len(country_segment) == 2 and country_segment.isalpha() else "br"
           # except IndexError:
           #     country = "br"

            # Extrai cliques no link do campo `actions`
            actions = row.get('actions', [])
            link_clicks = next((action['value'] for action in actions if action['action_type'] == 'link_click'), 0)

            # Extrai data do campo `date_start`
            date = row.get('date_start', 'N/A')

            # Processa intervalos horários
            if isinstance(hourly_stats, str):
                processed_row = {
                    "site_name": site_name,
                    "account_name": account_name,
                    "account_id": account_id,
                    #"country": country,
                    "date": date,
                    "time_interval": hourly_stats,
                    "impressions": int(row.get('impressions', 0)),  # Força conversão para int
                    "spend": float(row.get('spend', 0.0)),          # Força conversão para float
                    "link_clicks": int(link_clicks),               # Garante que link_clicks é int
                }
                processed_data.append(processed_row)
            else:
                logger.warning(f"Unexpected format for 'hourly_stats_aggregated_by_advertiser_time_zone': {hourly_stats}")
                continue

        # Cria um DataFrame com os dados processados
        processed_df = pd.DataFrame(processed_data)
        logger.info(f"Processed data contains {processed_df.shape[0]} rows.")

        # Remove duplicatas antes da agregação
        processed_df.drop_duplicates(inplace=True)
        logger.info(f"Data after removing duplicates contains {processed_df.shape[0]} rows.")

        # Garante que os formatos estão corretos
        processed_df['impressions'] = processed_df['impressions'].astype(int)
        processed_df['spend'] = processed_df['spend'].astype(float)
        processed_df['link_clicks'] = processed_df['link_clicks'].astype(int)

        # Realiza a agregação
        aggregated_df = processed_df.groupby(
            ["site_name", "account_name", "account_id", "date", "time_interval"], as_index=False
        ).agg({
            "impressions": "sum",
            "spend": "sum",
            "link_clicks": "sum",
        })

        logger.info(f"Aggregated data contains {aggregated_df.shape[0]} rows.")
        logger.info(f"Aggregated DataFrame sample:\n{aggregated_df.head()}")

        saopaulo_tz = pytz.timezone('America/Sao_Paulo')
        current_time_saopaulo = datetime.now(saopaulo_tz).strftime("%Y-%m-%d %H:%M:%S")
        aggregated_df['imported_at'] = current_time_saopaulo

        # Renomeia a coluna 'link_clicks' para 'clicks'
        aggregated_df.rename(columns={'link_clicks': 'clicks'}, inplace=True)

        logger.info(f"Processed aggregated DataFrame with {aggregated_df.shape[0]} rows.")
        return aggregated_df

    except Exception as e:
        logger.error(f"Error processing hourly data: {e}")
        return pd.DataFrame()





def split_dataframe(df, chunk_size):
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size]

def upload_to_bigquery(df, table_id, chunk_size=5000):
    if df is None or df.empty:
        logger.error("Dataframe is empty or None, skipping upload.")
        return

    try:
        for i, chunk in enumerate(split_dataframe(df, chunk_size)):
            logger.info(f"Uploading chunk {i + 1} to BigQuery...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = client.load_table_from_dataframe(chunk, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete
            logger.info(f"Chunk {i + 1} uploaded successfully.")
    except Exception as e:
        logger.error(f"Error uploading to BigQuery: {e}")

@functions_framework.cloud_event
def execute_notebook(event):
    pubsub_message = base64.b64decode(event.data["message"]["data"]).decode('utf-8')
    logger.info(f"Received Pub/Sub message: {pubsub_message}")

    # Executar a função principal async
    asyncio.run(main())

    return 'Execution completed.'

async def main():
    """Função principal async para execução local"""
    logger.info("BigQuery client configurado com credenciais padrão!")
    logger.info("Iniciando execução local do script (DADOS HOURLY) com ASYNC...")
    
    logger.info(f"Configuração: MAX_WORKERS={MAX_WORKERS}, REQUEST_DELAY={REQUEST_DELAY}, ACCOUNT_DELAY={ACCOUNT_DELAY}")
    
    # Contar total de contas em todos os grupos
    total_accounts = sum(len(group_config["accounts"]) for group_config in GROUPS.values())
    logger.info(f"🔄 Processando {len(GROUPS)} grupos com {total_accounts} contas em paralelo para máxima velocidade")
    
    # Processar todos os grupos com async
    logger.info("🔄 Iniciando coleta de dados de todos os grupos com ASYNC...")
    all_data = await fetch_all_groups_async()
    
    if all_data:
        logger.info(f"📊 Total de dados coletados: {len(all_data)} registros")
        
        # Processar dados por hora
        hourly_df = process_hourly_data(all_data)
        
        if not hourly_df.empty:
            logger.info(f"📈 Dados processados por hora: {len(hourly_df)} registros")
            
            logger.info(f"☁️ Fazendo upload para BigQuery: {TABLE_ID}")
            upload_to_bigquery(hourly_df, TABLE_ID)
            logger.info("✅ Dados enviados com sucesso para o BigQuery!")
        else:
            logger.warning("⚠️ Nenhum dado processado por hora")
    else:
        logger.error("❌ Nenhum dado foi coletado dos grupos")
    
    logger.info("🎉 Execução concluída com sucesso!")

if __name__ == "__main__":
    asyncio.run(main())


