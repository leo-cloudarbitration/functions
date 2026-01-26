import asyncio
import aiohttp
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz


# Configurações da API
API_BASE_URL = "https://external-api.activeview.app"
API_KEY = "Bearer 4694ab00080e22a60b44d5ad01dc508eae87a8db68e158b8d73dd1327db1b07f:3e578f881a0d25d355f9"
HEADERS = {"Authorization": API_KEY}

# Configurações do BigQuery
PROJECT_ID = "data-v1-423414"
DATASET_ID = "test"
TABLE_ID = "cloud_gam_hour_historical"

# Lista de sites com seus respectivos network IDs
GAM_SITES = [
    {"network_id": "23152058020", "site": "onplif.com"},
    {"network_id": "23152058020", "site": "fintacle.com"},
    {"network_id": "23302708904", "site": "amigadamamae.com.br"},
    {"network_id": "23313676084", "site": "ifinane.com"},
    {"network_id": "23314451390", "site": "finr.com.br"},
    {"network_id": "22958804404", "site": "finanzco.com"},
    {"network_id": "22958804404", "site": "espacoextra.com.br"},
    {"network_id": "22958804404", "site": "vidadeproduto.com.br"},
    {"network_id": "22024304448", "site": "tecnologianocampo.com.br"},
    {"network_id": "22024304448", "site": "superinvestmentguide.com"},
    {"network_id": "23150219615", "site": "brasileirinho.blog.br"},
    {"network_id": "23295671757", "site": "bimviral.com"},
    {"network_id": "23123915180", "site": "investimentoagora.com.br"},
    {"network_id": "23124049988", "site": "vamosestudar.com.br"}
]

async def fetch_hourly_data_from_api_async(session, network_id, site_name):
    """
    Faz uma chamada assíncrona para a API e retorna os dados agregados por hora.
    """
    try:
        # Busca dados do dia de ontem
        br_tz = pytz.timezone("America/Sao_Paulo")
        start_date = end_date = (datetime.now(br_tz) - timedelta(days=1)).strftime("%Y-%m-%d")

        url = f"{API_BASE_URL}/report/gam/custom/{network_id}/{site_name}/from-gam"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "dimensions": "DATE,HOUR,DOMAIN",
            "metrics": (
                "AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS," 
                "AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS," 
                "AD_EXCHANGE_LINE_ITEM_LEVEL_CTR," 
                "AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE," 
                "AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE"
            )
        }
        print(f"URL gerada: {url}")
        print(f"Parâmetros: {params}")
        
        async with session.get(url, headers=HEADERS, params=params) as response:
            print(f"Status Code para {site_name}: {response.status}")
            response_text = await response.text()
            print(f"Resposta bruta para {site_name}: {response_text}")
            response.raise_for_status()
            data = (await response.json())["response"]
            print(f"Dados retornados pela API para {site_name}: {data}")
            return data
    except Exception as e:
        print(f"Erro ao buscar dados da API para {site_name}: {e}")
        raise RuntimeError(f"Erro na API para {site_name}: {e}")

async def fetch_all_sites_data_async():
    """
    Busca dados de todos os sites de forma assíncrona.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for site_config in GAM_SITES:
            network_id = site_config["network_id"]
            site_name = site_config["site"]
            print(f"Criando task para '{site_name}' (Network ID: {network_id})...")
            task = fetch_hourly_data_from_api_async(session, network_id, site_name)
            tasks.append((site_name, task))
        
        results = []
        for site_name, task in tasks:
            try:
                data = await task
                results.append((site_name, data))
                print(f"Dados coletados com sucesso para {site_name}")
            except Exception as e:
                print(f"Erro ao processar {site_name}: {e}")
                # Continua com o próximo site mesmo se houver erro
                continue
        
        return results

def prepare_hourly_data(data, site_name):
    """
    Prepara os dados para o formato do BigQuery, convertendo receita de micros para dólares e calculando ecpm.
    """
    result = []
    for record in data:
        impressions = record.get("AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS", 0)
        clicks = record.get("AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS", 0)
        ctr = record.get("AD_EXCHANGE_LINE_ITEM_LEVEL_CTR", 0.0)
        revenue = record.get("AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0) / 1_000_000
        viewable_rate = record.get("AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE", None)
        ecpm = (revenue * 1000 / impressions) if impressions > 0 else 0.0

        result.append({
            "date": record.get("DATE"),
            "hour": record.get("HOUR"),
            "domain": record.get("DOMAIN"),
            "impressions": impressions,
            "clicks": clicks,
            "ctr": ctr,
            "revenue": revenue,
            "ecpm": ecpm,
            "viewable_rate": viewable_rate,
            "site_name": site_name
        })
    return result

def write_to_bigquery(data):
    """
    Insere os dados no BigQuery no modo WRITE_TRUNCATE.
    """
    try:
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("hour", "INT64"),
                bigquery.SchemaField("domain", "STRING"),
                bigquery.SchemaField("impressions", "INT64"),
                bigquery.SchemaField("clicks", "INT64"),
                bigquery.SchemaField("ctr", "FLOAT64"),
                bigquery.SchemaField("revenue", "FLOAT64"),
                bigquery.SchemaField("ecpm", "FLOAT64"),
                bigquery.SchemaField("viewable_rate", "FLOAT64"),
                bigquery.SchemaField("site_name", "STRING"),
            ]
        )
        print(f"Linhas preparadas para inserção: {data}")
        job = client.load_table_from_json(data, table_id, job_config=job_config)
        job.result()
        print(f"{len(data)} registros inseridos com sucesso no BigQuery.")
    except Exception as e:
        print(f"Erro ao gravar no BigQuery: {e}")
        raise RuntimeError(f"Erro ao gravar no BigQuery: {e}")

async def run_code_async(event, context):
    """
    Função principal assíncrona para a Cloud Function via Trigger de Evento.
    """
    try:
        print("Iniciando execução da função assíncrona via trigger de evento...")
        
        # Busca dados de todos os sites de forma assíncrona
        sites_data = await fetch_all_sites_data_async()
        
        all_data = []
        
        # Prepara os dados coletados
        for site_name, site_data in sites_data:
            try:
                prepared_data = prepare_hourly_data(site_data, site_name)
                all_data.extend(prepared_data)
                print(f"Dados preparados para {site_name}: {len(prepared_data)} registros")
            except Exception as e:
                print(f"Erro ao preparar dados para {site_name}: {e}")
                continue
        
        if all_data:
            print(f"Inserindo {len(all_data)} registros no BigQuery...")
            write_to_bigquery(all_data)
            print("Execução assíncrona concluída com sucesso.")
        else:
            print("Nenhum dado foi coletado com sucesso.")
            
    except Exception as e:
        print(f"Erro geral: {e}")
        raise RuntimeError(f"Erro na função: {e}")

def run_code(event, context):
    """
    Função wrapper para compatibilidade com Cloud Functions.
    """
    return asyncio.run(run_code_async(event, context))

if __name__ == "__main__":
    asyncio.run(run_code_async(None, None)) 