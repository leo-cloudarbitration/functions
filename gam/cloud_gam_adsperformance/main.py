# -*- coding: utf-8 -*-
"""
GAM → BigQuery (Cloud Function) - DADOS DE PERFORMANCE GAM COM UTM_CONTENT
─────────────────────────────────────────────────────────────────────────
Coleta métricas diárias por site GAM com utm_content:
🚀 Editado via Cursor - Deploy automático funcionando!
📁 Estrutura: gam/cloud_gam_adsperformance/ ✅
- date, network_id, site_name, key, value, impressions, clicks, ctr, revenue, ecpm, match_rate

Resultado final = métricas por site GAM com utm_content
ADICIONA os dados no BigQuery (WRITE_APPEND)
"""

import requests
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import os
from pytz import timezone
from collections import defaultdict
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# Tabela única para todos os dados de performance GAM
TABLE_ID = "data-v1-423414.test.cloud_gam_adsperformance_historical"

# Configurações da API
API_BASE_URL = "https://external-api.activeview.app"
API_KEY = "Bearer 4694ab00080e22a60b44d5ad01dc508eae87a8db68e158b8d73dd1327db1b07f:3e578f881a0d25d355f9"
HEADERS = {"Authorization": API_KEY}

# Quantos dias antes de hoje usar (1=ontem, 2=anteontem). Pode ser sobrescrito por env var.
REPORT_DAYS_OFFSET = int(os.getenv("REPORT_DAYS_OFFSET", "1"))

# Lista de sites GAM
GAM_SITES = [
    {"network_id": "22958804404", "site": "finanzco.com"},
    {"network_id": "22958804404", "site": "espacoextra.com.br"},
    {"network_id": "22958804404", "site": "vidadeproduto.com.br"},
    {"network_id": "22024304448", "site": "tecnologianocampo.com.br"},
    {"network_id": "22024304448", "site": "superinvestmentguide.com"},
    {"network_id": "23150219615", "site": "brasileirinho.blog.br"},
    {"network_id": "23295671757", "site": "bimviral.com"},
    {"network_id": "23152058020", "site": "onplif.com"},
    {"network_id": "23302708904", "site": "amigadamamae.com.br"},
    {"network_id": "23123915180", "site": "investimentoagora.com.br"},
    {"network_id": "23124049988", "site": "vamosestudar.com.br"},
    {"network_id": "23313676084", "site": "ifinane.com"}
]

# ------------------------------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------------------------------
bq_client = None

def get_bq_client():
    """Lazy loading do cliente BigQuery para evitar problemas de inicialização."""
    global bq_client
    if bq_client is None:
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            
            # Verificar se estamos no GitHub Actions (variável de ambiente GOOGLE_APPLICATION_CREDENTIALS)
            if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
                # GitHub Actions: usar credenciais do ambiente
                logger.info("Usando credenciais do GitHub Actions via arquivo JSON")
                bq_client = bigquery.Client(project="data-v1-423414")
            else:
                # Cloud Function: usar Application Default Credentials
                logger.info("Usando Application Default Credentials para Cloud Function")
                bq_client = bigquery.Client(project="data-v1-423414")
            
            logger.info("BigQuery client configurado com sucesso!")
        except Exception as e:
            logger.error("Erro ao configurar BigQuery client: %s", str(e))
            bq_client = None
    return bq_client

def create_aggregation_dict():
    """
    Cria um dicionário de agregação com os tipos corretos.
    """
    return {
        "network_id": "",
        "site": "",
        "impressions": 0,
        "clicks": 0,
        "ctr_sum": 0.0,
        "ctr_weight": 0,
        "revenue": 0.0,
        "ecpm_sum": 0.0,
        "ecpm_weight": 0,
        "match_rate_sum": 0.0,
        "match_rate_weight": 0,
        "key": "",
        "value": ""
    }

def aggregate_kvp_data(data):
    """
    Consolida os dados retornados pela API, agregando por chave-valor (key-value).
    """
    aggregated = defaultdict(create_aggregation_dict)
    
    for record in data:
        key = str(record["key"])
        value = str(record["value"])
        network_id = str(record.get("network_id", ""))
        site = str(record.get("site", ""))
        impressions = int(record["ad_exchange_line_item_level_impressions"])
        clicks = int(record["ad_exchange_line_item_level_clicks"])
        revenue_micros = float(record["ad_exchange_line_item_level_revenue"])
        ctr = float(record["ad_exchange_line_item_level_ctr"])
        ecpm = (revenue_micros / impressions / 1_000) if impressions > 0 else 0.0
        match_rate = (float(record.get("ad_exchange_active_view_viewable_impressions", 0)) /
                      float(record.get("ad_exchange_line_item_level_impressions", 1))) if impressions > 0 else 0.0
        
        group_key = (network_id, site, key, value)
        aggregated[group_key]["impressions"] += impressions
        aggregated[group_key]["clicks"] += clicks
        aggregated[group_key]["revenue"] += revenue_micros / 1_000_000  # Converte micros para dólares
        aggregated[group_key]["ctr_sum"] += ctr * impressions
        aggregated[group_key]["ctr_weight"] += impressions
        aggregated[group_key]["ecpm_sum"] += ecpm * impressions
        aggregated[group_key]["ecpm_weight"] += impressions
        aggregated[group_key]["match_rate_sum"] += match_rate * impressions
        aggregated[group_key]["match_rate_weight"] += impressions
        aggregated[group_key]["key"] = key
        aggregated[group_key]["value"] = value
        aggregated[group_key]["network_id"] = network_id
        aggregated[group_key]["site"] = site
    
    # Calcula médias ponderadas e retorna a lista consolidada
    local_tz = timezone("America/Sao_Paulo")
    report_date = (datetime.now(local_tz) - timedelta(days=REPORT_DAYS_OFFSET)).strftime("%Y-%m-%d")
    
    result = []
    for (network_id, site, key, value), values in aggregated.items():
        impressions = int(values["impressions"])
        ctr_weight = int(values["ctr_weight"])
        ecpm_weight = int(values["ecpm_weight"])
        match_rate_weight = int(values["match_rate_weight"])
        
        result.append({
            "date": report_date,
            "network_id": network_id,
            "site_name": site,
            "key": key,
            "value": value,
            "impressions": impressions,
            "clicks": int(values["clicks"]),
            "ctr": float(values["ctr_sum"]) / ctr_weight if ctr_weight > 0 else 0.0,
            "revenue": float(values["revenue"]),
            "ecpm": float(values["ecpm_sum"]) / ecpm_weight if ecpm_weight > 0 else 0.0,
            "match_rate": float(values["match_rate_sum"]) / match_rate_weight if match_rate_weight > 0 else 0.0
        })
    
    logger.info(f"Dados consolidados: {len(result)} registros")
    return result

def create_gam_table(table_id: str):
    """Cria a tabela no BigQuery se não existir."""
    bq_client = get_bq_client()
    if not bq_client:
        logger.error("Cliente BigQuery não configurado")
        return False
    
    try:
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("network_id", "STRING"),
            bigquery.SchemaField("site_name", "STRING"),
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("ecpm", "FLOAT"),
            bigquery.SchemaField("match_rate", "FLOAT"),
            bigquery.SchemaField("imported_at", "DATETIME")
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        logger.info(f"Tabela {table_id} criada/verificada com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela {table_id}: {e}")
        return False

def upload_to_bigquery(df: pd.DataFrame, table_id: str):
    """Faz upload dos dados para o BigQuery."""
    from google.cloud import bigquery
    
    logger.info("🔍 [DEBUG] Iniciando upload_to_bigquery...")
    logger.info(f"🔍 [DEBUG] DataFrame: {df is not None}")
    logger.info(f"🔍 [DEBUG] DataFrame vazio: {df.empty if df is not None else 'N/A'}")
    logger.info(f"🔍 [DEBUG] Tabela: {table_id}")
    
    bq_client = get_bq_client()
    if df is None or bq_client is None:
        logger.error("❌ DataFrame nulo ou BigQuery não configurado.")
        logger.error(f"❌ df is None: {df is None}")
        logger.error(f"❌ bq_client is None: {bq_client is None}")
        return
    
    if df.empty:
        logger.info("DataFrame vazio - nenhum dado para upload")
        return
    
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("network_id", "STRING"),
        bigquery.SchemaField("site_name", "STRING"),
        bigquery.SchemaField("key", "STRING"),
        bigquery.SchemaField("value", "STRING"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("revenue", "FLOAT"),
        bigquery.SchemaField("ecpm", "FLOAT"),
        bigquery.SchemaField("match_rate", "FLOAT"),
        bigquery.SchemaField("imported_at", "DATETIME")
    ]
    
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema
    )
    
    try:
        logger.info("Enviando %s registros para %s...", len(df), table_id)
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
        job.result()
        logger.info("✅ Adicionados %s registros para %s", job.output_rows, table_id)
    except Exception as e:
        logger.error("❌ Erro ao adicionar dados ao BigQuery: %s", str(e))
        raise

async def fetch_kvp_data_from_api_async(session, network_id, site_name):
    """
    Faz uma chamada assíncrona para a API e retorna os dados agregados por KVP filtrados por 'utm_content'.
    """
    try:
        # Configura a data para hoje no horário local (GMT-3)
        local_tz = timezone("America/Sao_Paulo")
        now_local = datetime.now(local_tz)
        target_date = (now_local - timedelta(days=REPORT_DAYS_OFFSET)).strftime("%Y-%m-%d")
        start_date = end_date = target_date
        
        url = f"{API_BASE_URL}/report/kvp/{network_id}/{site_name}/from-gam"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "key": "utm_content"
        }
        
        logger.info(f"🔍 Buscando dados para {site_name} (network_id: {network_id})")
        logger.info(f"URL: {url}")
        logger.info(f"Parâmetros: {params}")
        
        async with session.get(url, headers=HEADERS, params=params) as response:
            logger.info(f"Status Code para {site_name}: {response.status}")
            response_text = await response.text()
            
            if response.status != 200:
                logger.warning(f"Erro ao buscar dados para {site_name} (network_id: {network_id}). Status: {response.status}")
                return []  # Retorna lista vazia em caso de erro
                
            data = (await response.json())["response"]
            # Anota cada registro com o network_id e o site de origem
            for item in data:
                item["network_id"] = network_id
                item["site"] = site_name
            
            logger.info(f"✅ Dados obtidos para {site_name}: {len(data)} registros")
            return data
    except Exception as e:
        logger.warning(f"Erro ao buscar dados da API para {site_name} (network_id: {network_id}): {e}")
        return []  # Retorna lista vazia em caso de erro

async def run_gam_collection():
    """
    Função principal assíncrona para buscar dados GAM com utm_content e salvar no BigQuery.
    """
    try:
        logger.info("🚀 Iniciando coleta de dados GAM com utm_content...")
        
        async with aiohttp.ClientSession() as session:
            # Cria tasks para todos os sites
            tasks = [
                fetch_kvp_data_from_api_async(session, site["network_id"], site["site"])
                for site in GAM_SITES
            ]
            
            logger.info(f"📊 Executando {len(tasks)} chamadas em paralelo...")
            
            # Executa todas as chamadas em paralelo
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Consolida os dados de todos os sites
            all_data = []
            for i, site_data in enumerate(results):
                if isinstance(site_data, list):  # Verifica se é uma lista válida
                    all_data.extend(site_data)
                    logger.info(f"✅ Site {i+1}: {len(site_data)} registros")
                else:
                    logger.warning(f"❌ Site {i+1}: Erro na coleta")
            
            if not all_data:
                logger.warning("⚠️ Nenhum dado foi obtido de nenhum site!")
                return
            
            logger.info(f"📈 Total de registros coletados: {len(all_data)}")
            
            combined_data = aggregate_kvp_data(all_data)
            
            if combined_data:
                # Filtrar apenas registros onde key = "utm_content"
                filtered_data = []
                for record in combined_data:
                    if record.get("key") == "utm_content":
                        filtered_data.append(record)
                
                logger.info(f"🔍 Filtrados {len(filtered_data)} registros com utm_content de {len(combined_data)} total")
                
                if filtered_data:
                    # Converter para DataFrame e adicionar timestamp
                    df = pd.DataFrame(filtered_data)
                    local_tz = timezone("America/Sao_Paulo")
                    df["imported_at"] = datetime.now(local_tz)
                    
                    # Converter coluna date para datetime
                    df["date"] = pd.to_datetime(df["date"])
                    
                    # Criar tabela se não existir
                    if create_gam_table(TABLE_ID):
                        # Upload para BigQuery
                        upload_to_bigquery(df, TABLE_ID)
                        logger.info("✅ Upload para BigQuery concluído com sucesso!")
                    else:
                        logger.error("❌ Falha ao criar tabela no BigQuery")
                else:
                    logger.warning("⚠️ Nenhum registro com utm_content encontrado")
            else:
                logger.warning("⚠️ Nenhum dado foi obtido da API")
            
            logger.info("🎉 Execução concluída com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro geral: {e}")
        raise RuntimeError(f"Erro na função: {e}")

def main():
    """
    Função principal para execução via GitHub Actions.
    """
    logger.info("🚀 Iniciando GAM Ads Performance Data Collection...")
    
    try:
        # Executa a coleta assíncrona
        asyncio.run(run_gam_collection())
        logger.info("✅ GAM Ads Performance Data Collection concluída com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro durante a execução: {e}")
        raise

if __name__ == "__main__":
    main()
