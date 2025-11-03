"""
Google Ads → BigQuery (Cloud Function) - DADOS HORÁRIOS
─────────────────────────────────────────────────────────────────
Coleta métricas horárias por campanha:
- date, hour, account_id, account_name, campaign_id, campaign_name
- spend, clicks, cpc, impressions, ctr, conversions, cost_per_conversion
- moeda, budget

Resultado final = métricas por hora por campanha
SOBRESCREVE os dados no BigQuery (WRITE_TRUNCATE)
"""

import json
import time
import os
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
import pytz
import pandas as pd
import logging

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# 🔹 Configuração do BigQuery
BIGQUERY_TABLE_ID = "data-v1-423414.test.cloud_googleads_hour"
sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

# Data de hoje em São Paulo
hoje = (datetime.now(sao_paulo_tz)).strftime('%Y-%m-%d')

# IDs das contas do Google Ads
CUSTOMER_IDS = [
    "9679496200",
    "2153708041",
    "1378108795",
    "5088162800",
    "7205935192"
]

# ------------------------------------------------------------------------------
# CONFIGURAÇÃO DE CREDENCIAIS
# ------------------------------------------------------------------------------
def get_google_credentials():
    """
    Obtém credenciais do Google Cloud a partir de variáveis de ambiente.
    Prioridade:
    1. SECRET_GOOGLE_SERVICE_ACCOUNT (JSON string)
    2. GOOGLE_APPLICATION_CREDENTIALS (caminho para arquivo JSON)
    3. Application Default Credentials
    """
    try:
        # Tentar obter do SECRET_GOOGLE_SERVICE_ACCOUNT (GitHub Actions)
        secret_json = os.getenv("SECRET_GOOGLE_SERVICE_ACCOUNT")
        if secret_json:
            logger.info("✅ Usando credenciais do SECRET_GOOGLE_SERVICE_ACCOUNT")
            service_account_info = json.loads(secret_json)
            return service_account.Credentials.from_service_account_info(service_account_info)
        
        # Tentar obter do GOOGLE_APPLICATION_CREDENTIALS (arquivo JSON)
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and os.path.exists(creds_path):
            logger.info("✅ Usando credenciais do arquivo: %s", creds_path)
            return service_account.Credentials.from_service_account_file(creds_path)
        
        # Fallback para Application Default Credentials
        logger.info("✅ Usando Application Default Credentials")
        return None  # BigQuery Client usará as credenciais padrão
        
    except Exception as e:
        logger.error("❌ Erro ao obter credenciais do Google: %s", str(e))
        raise

def get_google_ads_config():
    """
    Obtém configuração do Google Ads a partir de variável de ambiente.
    """
    ads_config_json = os.getenv("SECRET_GOOGLE_ADS_CONFIG")
    if not ads_config_json:
        raise ValueError("❌ SECRET_GOOGLE_ADS_CONFIG não encontrado nas variáveis de ambiente!")
    
    logger.info("✅ Usando configuração do Google Ads do SECRET_GOOGLE_ADS_CONFIG")
    return json.loads(ads_config_json)

# ------------------------------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------------------------------
bq_client = None

def get_bq_client():
    """Lazy loading do cliente BigQuery."""
    global bq_client
    if bq_client is None:
        try:
            credentials = get_google_credentials()
            if credentials:
                bq_client = bigquery.Client(
                    credentials=credentials, 
                    project=credentials.project_id if hasattr(credentials, 'project_id') else "data-v1-423414"
                )
            else:
                # Usar Application Default Credentials
                bq_client = bigquery.Client(project="data-v1-423414")
            
            logger.info("✅ BigQuery client configurado com sucesso!")
        except Exception as e:
            logger.error("❌ Erro ao configurar BigQuery client: %s", str(e))
            bq_client = None
    return bq_client

# ------------------------------------------------------------------------------
# GOOGLE ADS CLIENT
# ------------------------------------------------------------------------------
def get_google_ads_client():
    """Cria cliente Google Ads."""
    try:
        google_ads_config = get_google_ads_config()
        client = GoogleAdsClient.load_from_dict(google_ads_config)
        logger.info("✅ Google Ads client configurado com sucesso!")
        return client
    except Exception as e:
        logger.error("❌ Erro ao configurar Google Ads client: %s", str(e))
        raise

# ------------------------------------------------------------------------------
# FUNÇÕES DE PROCESSAMENTO
# ------------------------------------------------------------------------------
def check_table_exists():
    """Verifica e cria tabela no BigQuery se não existir."""
    bq_client = get_bq_client()
    try:
        bq_client.get_table(BIGQUERY_TABLE_ID)
        logger.info("✅ Tabela encontrada no BigQuery.")
    except:
        logger.warning("⚠️ Tabela não encontrada. Criando...")
        create_bigquery_table()

def create_bigquery_table():
    """Cria tabela no BigQuery."""
    bq_client = get_bq_client()
    dataset_id, table_name = BIGQUERY_TABLE_ID.split(".")[1:]
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)

    schema = [
        bigquery.SchemaField("account_name", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("hour", "INTEGER"),
        bigquery.SchemaField("moeda", "STRING"),
        bigquery.SchemaField("budget", "FLOAT"),
        bigquery.SchemaField("spend", "FLOAT"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("cpc", "FLOAT"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("conversions", "FLOAT"),
        bigquery.SchemaField("cost_per_conversion", "FLOAT"),
        bigquery.SchemaField("imported_at", "TIMESTAMP")
    ]

    table = bigquery.Table(table_ref, schema=schema)
    try:
        bq_client.create_table(table)
        logger.info("✅ Tabela %s criada com sucesso.", BIGQUERY_TABLE_ID)
        time.sleep(5)  # Aguarda propagação no BigQuery
    except Exception as e:
        logger.error("❌ Erro ao criar tabela: %s", e)

def get_google_ads_data(client, customer_id):
    """Busca dados do Google Ads para um customer_id."""
    query = f"""
        SELECT
            customer.id,
            customer.descriptive_name,
            campaign.id,
            campaign.name,
            segments.date,
            segments.hour,
            customer.currency_code,
            campaign_budget.amount_micros,
            metrics.cost_micros,
            metrics.clicks,
            metrics.average_cpc,
            metrics.impressions,
            metrics.ctr,
            metrics.conversions,
            metrics.cost_per_conversion
        FROM campaign
        WHERE segments.date = '{hoje}'
    """

    ga_service = client.get_service("GoogleAdsService", version="v17")
    response = ga_service.search(customer_id=customer_id, query=query)

    data = []
    # Timestamp de importação (hora de São Paulo)
    imported_at = datetime.now(sao_paulo_tz)
    
    for row in response:
        data.append({
            "account_name": row.customer.descriptive_name if hasattr(row.customer, "descriptive_name") else "",
            "account_id": str(row.customer.id) if hasattr(row.customer, "id") else "",
            "campaign_id": str(row.campaign.id) if hasattr(row.campaign, "id") else "",
            "campaign_name": row.campaign.name if hasattr(row.campaign, "name") else "",
            "date": str(row.segments.date) if hasattr(row.segments, "date") else "",
            "hour": int(row.segments.hour) if hasattr(row.segments, "hour") else 0,
            "moeda": row.customer.currency_code if hasattr(row.customer, "currency_code") else "",
            "budget": float(row.campaign_budget.amount_micros / 1_000_000) if hasattr(row, "campaign_budget") else 0.0,
            "spend": float(row.metrics.cost_micros / 1_000_000) if hasattr(row.metrics, "cost_micros") else 0.0,
            "clicks": int(row.metrics.clicks) if hasattr(row.metrics, "clicks") else 0,
            "cpc": float(row.metrics.average_cpc / 1_000_000) if hasattr(row.metrics, "average_cpc") else 0.0,
            "impressions": int(row.metrics.impressions) if hasattr(row.metrics, "impressions") else 0,
            "ctr": float(row.metrics.ctr) if hasattr(row.metrics, "ctr") else 0.0,
            "conversions": float(row.metrics.conversions) if hasattr(row.metrics, "conversions") else 0.0,
            "cost_per_conversion": float(row.metrics.cost_per_conversion / 1_000_000) if hasattr(row.metrics, "cost_per_conversion") else 0.0,
            "imported_at": imported_at
        })

    return data

def save_to_bigquery(data):
    """Salva dados no BigQuery."""
    if not data:
        logger.warning("⚠️ Nenhum dado para inserir.")
        return

    check_table_exists()

    # Converter para DataFrame
    df = pd.DataFrame(data)

    # ✅ Corrigir o tipo da coluna 'date'
    df["date"] = pd.to_datetime(df["date"]).dt.date
    
    # ✅ Garantir que imported_at seja timestamp
    df["imported_at"] = pd.to_datetime(df["imported_at"])

    # ✅ Reordenar colunas
    desired_order = [
        "account_name",
        "account_id",
        "campaign_id",
        "campaign_name",
        "date",
        "hour",
        "moeda",
        "budget",
        "spend",
        "clicks",
        "cpc",
        "impressions",
        "ctr",
        "conversions",
        "cost_per_conversion",
        "imported_at"
    ]
    df = df[desired_order]

    # Enviar ao BigQuery (WRITE_TRUNCATE - sobrescreve dados existentes)
    bq_client = get_bq_client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, BIGQUERY_TABLE_ID, job_config=job_config)
    job.result()

    logger.info("✅ Dados inseridos com sucesso no BigQuery!")

# ------------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ------------------------------------------------------------------------------
def ca_google_ads_today(event=None, context=None):
    """
    Função principal para coleta de dados do Google Ads.
    Compatível com Cloud Functions e GitHub Actions.
    """
    logger.info("🚀 Iniciando coleta de dados do Google Ads...")
    logger.info("📅 Data: %s", hoje)

    try:
        # Criar cliente Google Ads
        client = get_google_ads_client()

        all_data = []

        for customer_id in CUSTOMER_IDS:
            logger.info("🔍 Coletando dados do customer_id: %s", customer_id)
            try:
                data = get_google_ads_data(client, customer_id)
                if data:
                    logger.info("📊 %s registros extraídos de %s.", len(data), customer_id)
                    all_data.extend(data)
                else:
                    logger.warning("⚠️ Nenhum dado para %s.", customer_id)
            except Exception as e:
                logger.error("❌ Erro ao coletar dados de %s: %s", customer_id, e)

        if all_data:
            save_to_bigquery(all_data)
            logger.info("✅ Total de registros processados: %s", len(all_data))
        else:
            logger.warning("⚠️ Nenhum dado extraído de nenhuma conta.")

        return "✅ Processamento concluído com sucesso."
    
    except Exception as e:
        logger.error("❌ Erro crítico: %s", str(e))
        raise

# ------------------------------------------------------------------------------
# EXECUÇÃO LOCAL
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("🚀 INICIANDO EXECUÇÃO LOCAL - GOOGLE ADS HOURLY DATA")
    logger.info("=" * 80)
    
    result = ca_google_ads_today()
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    logger.info("=" * 80)
    logger.info("📊 RESUMO DA EXECUÇÃO")
    logger.info("=" * 80)
    logger.info("⏱️ Tempo total: %.2f segundos", execution_time)
    logger.info("✅ Status: %s", result)
    logger.info("=" * 80)

