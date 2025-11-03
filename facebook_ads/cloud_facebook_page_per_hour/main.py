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

# CONFIGURAÇÃO DA TABELA BIGQUERY (ÚNICA PARA TODOS OS GRUPOS)
TABLE_ID = "data-v1-423414.test.cloud_facebook_page_per_hour"

# CONFIGURAÇÃO DOS GRUPOS - ESTRUTURA COMPLETA
GROUPS = {
    "casf_a": {
        "token": "EAASZBzOX0YzUBP2Io7Qj5KkSTewdSdgZBarOUqV25wW8bY428sXOLN7nftC7t6UvMvuyN30CHODtgaZBP67Nni3uIaXySsZAfAl25t5bfZBXMRVsG2ZC4SLLZADZC04IsAmrWiW1BrV0kEw281oAZAOYeu5A2f9dS8kRRZAnTpgumZCJnbJZAWMGoPZCZBVZBh6Pe8zyZAyoMgZDZD",
        "accounts": [
            "act_2086793801756487",  # A 001
            "act_981724707176063",   # A 002
            "act_1336438444153855",  # A 004
            "act_1391292178903463",  # A 006
            "act_1299109444484986",  # A 007
            "act_1031835115731749",  # CASF A - 021
            "act_1452046959544076",  # CASF A - 022
        ]
    }, 

    "casf_a_2": {
        "token": "EAASZBzOX0YzUBP5sBgBYlA9vhVnZBO1W3oQjAiLQlMxRUqto6zjZBtvNbrO12PUndFqi5GDR0Sr8xyoG1yLzwbN1dGA7mrL5CO3DhfJOb527bi1NcDWwSRGFPF1um4mre0XB5wa2zXiISSV8vkrda93h3ZA21TO4WoybnAAuLXmZBGYoazfGYW0hsjtJ8PFzhgAZDZD",
        "accounts": [
            "act_4004026033209415",  # A 003
            "act_517255721392274",   # A 005
            "act_671403681969823",   # A 008
            "act_1651254315530600",  # A 009
            "act_1673050973599968",  # A 010
            "act_1263612182017570",  # CASF A - 023
            "act_1474738367239405",  # CASF A - 024
            "act_1412982356688062",  # CASF A - 025
        ]
    },       

    "casf_b": {
        "token": "EAASZBzOX0YzUBP8tkZCg0WGE5dobFuhAMzMFlKllEwNgn741p2tS1rb5u7tPhrQciZBhg8h4La7pXIajhMpZCBcVuO6iFhZAoFHSQvJyx5VYw9AfbWFRkYo2n8QmZBZBo9zD4nzGD7qUkWNF3aw1ZCwwoykPH9IyUUQ6hfS2A5wYqwzmst7Giicc2iJijl7pWsEzxIGef0oX",
        "accounts": [
            "act_1346308366549889",  # B 001
            "act_1370098907466943",  # B 003
            "act_606104969244156",   # B 004
            "act_1068778325288092",  # B 005
            "act_625690570258598",   # B 007
            "act_1200321581769557",  # B 009
            "act_1337378787572363",  # B 010
            "act_974681131291021",   # CASF B - 011
            "act_1378638580101069",  # CASF B - 012
            "act_1761670824725907",  # CASF B - 013
            "act_1772051913713580",  # CASF B - 014
            "act_1072725700887714",  # CASF B - 015
            "act_1471023010727298",  # CASF B - 016
            "act_695055153401445",   # CASF B - 017
            "act_1057740166376634",  # CASF B - 018
            "act_1455601052101135",  # CASF B - 019
            "act_622738277534907",   # CASF B - 020
            "act_574063968902469",   # CASF B - 021
            "act_1394320888505693",  # CASF B - 022
            "act_1433314247709835",  # CASF B - 023
            "act_2288442174883294",  # CASF B - 024
        ]
    },        

    "casf_b_2": {
        "token": "EAASZBzOX0YzUBPxTYvJSVzlZC9EtZBGX1qxhTasbsHzaRxG0HtEG1l0Op0hMXZBjx2OBbFCkcY1uD7UJIbNfBlukDx1DO3C0otwD2aQt8f2zg0HZAfSSxLPYDCL0TG1YUckSuOWvYNhbf82KdIc38EXV0LV6Ot5bpZByVvWhY9ZBhSJsFBKDyrLx2PH2LJmqipI3FKsjTil",
        "accounts": [
            "act_1346308366549889",  # B 001
            "act_955177216218047",  # B 002
            "act_517874211015842",   # B 006
            "act_1797415574539841",  # CASF B - 025
            "act_717862000867606",   # CASF B - 026
            "act_770019508926452",   # CASF B - 027
            "act_796006246705409",   # CASF B - 028
            "act_992601502899291",   # CASF B - 029
            "act_1312677630226374",  # CASF B - 030
            "act_1259041578841388",  # CASF B - 031
            "act_1121606749849488",  # CASF B - 032
            "act_1915342352364684",  # CASF B - 033
            "act_3192500777579255",  # CASF B - 034
        ]
    },   

    "casf_c": {
        "token": "EAASZBzOX0YzUBPyLQMKvclgQoFkkybpz5dDzSEdbYhpIaYY1YkXSWtqDZBZCZCa8ZAWWDpzASvDRqwQvpit8ZBSK3CzGHALHaoh4mD1xft5TH3LqFNkjZBOViZCZCZBUEDKu4JDo7mIJ0ZCGpFSRcR1SKvW9KG7e9V9AfhvMtRFZCu1FzZBj761K82xXaw9wZA7clhzJIOsJgJ8wL1",
        "accounts": [
            "act_1032538255591656",  # C 006
            "act_4151573991833128",  # CASF C - 018
            "act_1066574724914885",  # CASF C - 020
            "act_1392970961753541",  # CASF C - 021
            "act_1150534793775622",  # CASF C - 022
            "act_2653324861542109",  # CASF C - 023
            "act_1892401481539404",  # CASF C - 024
            "act_10026762610765790", # CASF C - 025
            "act_1271027548048483",  # CASF C - 026
            "act_4338176139795210",  # CASF C - 028
            "act_1454996898848132",  # CASF C - 029
            "act_709652395012874",   # CASF C - 030
            "act_3012814918876991",  # CASF C - 031
            "act_705327572107397",   # CASF C - 032
            "act_1030061175965269",  # CASF C - 042
            "act_1235931794248240",  # CASF C - 043
            "act_1018444543789280",  # CASF C - 044
            "act_1109560931044202",  # CASF C - 045
        ]
    },
    
    "casf_c_2": {
        "token": "EAASZBzOX0YzUBPyAB1MQylCljnXuWEZBZBRD8c08jKq1FJNwAqBGlq2MPXAy1cuSVVcQJqjttdqunsVCZBF96N3LrNvYsuwSSSlrZBlZAaIeN8xx1zdUZAzqoo7qoDRjr2ZAqJ4ZBiidZCG7WmzqnVBPTQgafd9gADc5xZCN7olMR1lUXZAQ2m0IY2qyuBuXa7gXniW2o4TZAYpsZC",
        "accounts": [
            "act_1584150448951619",  # C 007
            "act_1749850525573536",  # C 008
            "act_681898871185195",   # C 009
            "act_536441699129952",   # C 010
            "act_1953684948703370",  # C 011
            "act_1664780364223146",  # C 012
            "act_3086929154792389",  # C 013
            "act_1054819642836727",  # C 014
            "act_720259200649418",   # C 015
            "act_752173674044463",   # C 016
            "act_923457749913500",   # CASF C - 017
            "act_3039326342887144",  # CASF C - 033
            "act_2988355044704974",  # CASF C - 034
            "act_2199444827150117",  # CASF C - 035
            "act_947568777447505",   # CASF C - 036
            "act_740142858498998",   # CASF C - 037
        ]
    },

    "casf_c_3": {
        "token": "EAASZBzOX0YzUBP71gRyC7bWDNCpps0eN0TXFAKNZCZB3z8TW7TzXLc1ZBhBSqpk4Ry6pjb58XPkBMU48nXUcZA8FHgHfm8q5ZCXKIi4ZA6xiZAcreHesRct13ndClM3ucvFyNtnDenkuxFLykEVkfsRtmg2bL7nSzG4DHGNa3jVpCovnBIvzj4DIg4Ihk371CmZAyZCeZClSkrw",
        "accounts": [
            "act_3938176226452279",  # C 001
            "act_3983890891844511",  # C 002
            "act_1080376004101758",  # C 003
            "act_1733528333951100",  # CASF C - 038
            "act_1464456568314306",  # CASF C - 039
            "act_1724766664815078",  # CASF C - 040
            "act_1045985000997960",  # CASF C - 041
        ]
    },

    "cloudarbitration": {
        "token": "EAALBhI66tYsBO7sZCaopQMDlbWZCBjK42JuNUdvrVQZAmdZBZBLA7I6Eh364Tjo2S7uBbn6sn0zZAsyXCD0b3iDUf4ki9CZAL0ZAkN2ZBeVabl7gP5tTLYNWSUCJNBgIEriRS1ZA2MhjHhZCsYHo3vFKKIQdiqxdHZBzwOvxI4XW039iLBhhSr7DoR2ksr1MJ9FoP09jD",
        "accounts": [
            'act_1408181320053598',#Cloud 002
            'act_2400057770189729',#Cloud 009
            'act_678596297812481', #Cloud 005
        ]
    },
    
    "cloudarbitration_2": {
        "token": "EAALBhI66tYsBPzjsV3pZCna4zpZBilE05elpmXAdtZAb8UgcryTvp7ws5aWFFhyZAyzo1Wz02uBDntf36GAFiE8hJoc3ZCj9nUPH2l8ySSlqZBj7bSvNDxQWpF0ufQbvWN7ajEcXXhOUk9BeRbghHN6GPOmPo1O83hP3pBREF3ZAWRQ90yxVcZBi4SA87HIZCqZC77bwZDZD",
        "accounts": [
            'act_901745041151181', #Cloud 003
            'act_384373767863365', #Cloud 006
            'act_341062168314061', #Cloud 007
            'act_1609550796495814', #Cloud 008
            'act_1727835544421228',#Cloud 010
        ]
    },
    
    "cloudarbitration_3": {
        "token": "EAAQHDrkrpocBP98Fh68zaIU8FTjGaERnCq5bxVE7ZBZBdT0AJCCG9zm6xJkDu0JXQNPDcr9LOX68WglAeXjYyP5TguEw7sXE6pHIvmvznJvVKxHuApMPhUwYRcQyHtCoWkIqTlZBBKexGXhCqPGM5cyBFR73E4bUZC4lBVNMulTolhep7Q0Aegk6Jg16zB9d4wZDZD",
        "accounts": [
            'act_336914685607657',#Cloud 004
        ]
    },

    "caag_a": {
        "token": "EAAaHkg81HaQBPAD97uZChFeLcZBUogRVjyUhqa0pXVmPfOgXW5r8O4JAb8HkwQPdz3DOEaE4oXJtWDg9ZBe4DZAbKrfZCxQv9sdUTv1zqS2JFNm63dRyZAuZByOZAZAkmd2IpBJ2afIj2JevKZAGI8AkdZBb2aMdPGiaO072K8OxVD94fl2Q0HT6zN2xhPzq7aZCYzSSHAZDZD",
        "accounts": [
            'act_1385339079505837', # CAAG A 001
            'act_618898480729472', # CAAG A 002
            'act_586870314267963', # CAAG A 003
            'act_671266691936018', # CAAG A 004
            'act_10001478916622937' # CAAG A 005
        ]
    },
    
    "caag_b": {
        "token": "EAAYq23wDZCIwBPiyFZA8K62w4WXO5VeATZBZCmZChOotxjRQicDrssBa0ZBzXknPOULfUcXZAZCM67ikBxs3ZBoVToYdpWWVkPQfahXKSW17dvS4ZBLq63lyLWm5dgLZCAJObRueXeo2IVbY0gwtHEuz3SV80qcvoOG5QgZBZBTvT6ZAhmZCIzI6ZC8s2pNkZChLjMZBCeHu8bxQZDZD",
        "accounts": [
            'act_756342057114932', # CAAG B 001
        ]
    },
    
    "nassovia_a": {
        "token": "EAALbQS6qEcIBPnaXZByqQQqFQMMMGNXRPX7xaG882COyVzb3RrKHrSOB1vDD2T29WHmUszByI50zlBUu8rEgz1F5Mq4eZAbzPJ42t6keTV049fDoH9mNjg9XB4oZAwJhZBVJ3uWcIWqRxFi4IXe9FjzrnuD9MduwOhveNPl4R7ebqsZCGr3htNjDWqZAFtTNKizAZDZD",
        "accounts": [
            "act_1294299965610280",  # Nassovia A 001
            "act_1230275922480187",  # Nassovia A 002
            "act_1155530230051923",  # Nassovia A 003
        ]
    }
}

# Configurações de paralelismo
MAX_WORKERS = 15
REQUEST_DELAY = 0.35
ACCOUNT_DELAY = 0.7
RATE_LIMIT_DELAY = 30.0

# Configurar credenciais do BigQuery
# As credenciais são carregadas de variável de ambiente ou secret do GitHub
def get_bigquery_client():
    """Obtém cliente do BigQuery usando credenciais de ambiente ou padrão"""
    try:
        # Tentar carregar de variável de ambiente SECRET_GOOGLE_SERVICE_ACCOUNT
        credentials_json = os.environ.get('SECRET_GOOGLE_SERVICE_ACCOUNT')
        if credentials_json:
            credentials_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            return bigquery.Client(credentials=credentials)
        else:
            # Usar credenciais padrão do ambiente (para execução local ou Cloud Functions)
            return bigquery.Client()
    except Exception as e:
        logger.error(f"Erro ao configurar credenciais do BigQuery: {e}")
        # Fallback para credenciais padrão
        return bigquery.Client()

client = get_bigquery_client()

# Verificar se os grupos do Facebook estão configurados
if not GROUPS:
    logger.error("Grupos do Facebook não estão configurados.")
else:
    total_tokens = len(set(group_config["token"] for group_config in GROUPS.values()))
    total_accounts = sum(len(group_config["accounts"]) for group_config in GROUPS.values())
    logger.info(f"Facebook API grupos configurados com sucesso! ({len(GROUPS)} grupos, {total_tokens} tokens únicos, {total_accounts} contas)")

async def get_insights_async(session, account_id, access_token, after_cursor=None):
    """Função async para buscar insights do Facebook"""
    base_url = f"https://graph.facebook.com/v22.0/{account_id}/insights"
    fields = "account_name,account_id,campaign_id,campaign_name,date_start,date_stop,impressions,spend,ctr,actions"
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

            # Processa category (tudo antes do terceiro "_")
            parts = campaign_name.split('_')
            category = '_'.join(parts[:3]) if len(parts) >= 3 else campaign_name

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
            if actions and isinstance(actions, list):
                link_clicks = next((int(action['value']) for action in actions if action.get('action_type') == 'link_click'), 0)
            else:
                link_clicks = 0

            # Extrai data do campo `date_start`
            date = row.get('date_start', 'N/A')

            # Processa intervalos horários
            if isinstance(hourly_stats, str):
                processed_row = {
                    "category": category,
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
            ["category", "account_name", "account_id", "date", "time_interval"], as_index=False
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
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
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



