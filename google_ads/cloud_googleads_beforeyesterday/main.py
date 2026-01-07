"""
Google Ads → BigQuery (Cloud Function) - DADOS DE ANTEONTEM
─────────────────────────────────────────────────────────────────
Coleta métricas diárias de ANTEONTEM (2 dias atrás) por campanha:
- date, account_id, account_name, campaign_id, campaign_name
- spend, clicks, cpc, impressions, ctr, conversions, cost_per_conversion
- moeda, budget

Resultado final = métricas do dia de anteontem por campanha
ADICIONA os dados no BigQuery (WRITE_APPEND)

⚠️ NOTAS SOBRE GRPC E GITHUB ACTIONS:
─────────────────────────────────────────────────────────────────
O Google Ads API usa GRPC por padrão, que pode ter problemas de rede
no GitHub Actions. Implementamos as seguintes soluções:

1. Retry logic com backoff exponencial (3 tentativas)
2. Delay de 1 segundo entre requisições de contas diferentes
3. Configurações de ambiente GRPC otimizadas
4. use_proto_plus=True (formato compatível)
5. Verificação prévia de todos os secrets
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
# Configurar variáveis de ambiente para melhorar estabilidade do GRPC
os.environ['GRPC_ENABLE_FORK_SUPPORT'] = '1'
os.environ['GRPC_POLL_STRATEGY'] = 'poll'
os.environ['GRPC_PYTHON_LOG_LEVEL'] = 'ERROR'

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# 🔹 Configuração do BigQuery
BIGQUERY_TABLE_ID = "data-v1-423414.test.ca_googleads_historical"
sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

# Data de ANTEONTEM em São Paulo (2 dias atrás)
anteontem = (datetime.now(sao_paulo_tz) - timedelta(days=2)).strftime('%Y-%m-%d')

# IDs das contas do Google Ads
CUSTOMER_IDS = [
    "9679496200", #001
    "1378108795", #002
    "2153708041", #003
    "5088162800", #004
    "7205935192", #005
    "4985450045", #006
    "4161586974", #007
    "5074252268", #008
    "8581304094", #009
    "2722606250", #010
]

# ------------------------------------------------------------------------------
# DIAGNÓSTICO DE VERSÕES
# ------------------------------------------------------------------------------
def log_library_versions():
    """
    Loga as versões das bibliotecas principais para debug.
    """
    logger.info("=" * 80)
    logger.info("📚 VERSÕES DAS BIBLIOTECAS INSTALADAS")
    logger.info("=" * 80)
    
    try:
        import pkg_resources
        ga_version = pkg_resources.get_distribution('google-ads').version
        logger.info(f"✅ google-ads: {ga_version}")
    except Exception as e:
        logger.warning(f"⚠️ google-ads: Erro ao obter versão - {e}")
    
    try:
        import grpc
        logger.info(f"✅ grpcio: {grpc.__version__}")
    except Exception as e:
        logger.warning(f"⚠️ grpcio: Erro ao obter versão - {e}")
    
    try:
        import google.api_core
        logger.info(f"✅ google-api-core: {google.api_core.__version__}")
    except Exception as e:
        logger.warning(f"⚠️ google-api-core: Erro ao obter versão - {e}")
    
    try:
        import google.cloud.bigquery
        logger.info(f"✅ google-cloud-bigquery: {google.cloud.bigquery.__version__}")
    except Exception as e:
        logger.warning(f"⚠️ google-cloud-bigquery: Erro ao obter versão - {e}")
    
    logger.info("=" * 80 + "\n")

# ------------------------------------------------------------------------------
# VERIFICAÇÃO DE SECRETS E CREDENCIAIS
# ------------------------------------------------------------------------------
def verify_secrets():
    """
    Verifica se todos os secrets necessários estão configurados corretamente.
    """
    logger.info("=" * 80)
    logger.info("🔍 VERIFICANDO CONFIGURAÇÃO DE SECRETS E CREDENCIAIS")
    logger.info("=" * 80)
    
    all_ok = True
    
    # 1. Verificar SECRET_GOOGLE_SERVICE_ACCOUNT
    logger.info("\n📋 1/3 - Verificando credenciais do Google Cloud...")
    secret_json = os.getenv("SECRET_GOOGLE_SERVICE_ACCOUNT")
    
    if secret_json:
        logger.info("   ✅ SECRET_GOOGLE_SERVICE_ACCOUNT encontrado")
        try:
            service_account_info = json.loads(secret_json)
            required_keys = ["type", "project_id", "private_key_id", "private_key", "client_email"]
            missing_keys = [key for key in required_keys if key not in service_account_info]
            if missing_keys:
                logger.error(f"   ❌ Campos faltando: {missing_keys}")
                all_ok = False
            else:
                logger.info(f"   ✅ JSON válido - Project ID: {service_account_info.get('project_id', 'N/A')}")
        except json.JSONDecodeError as e:
            logger.error(f"   ❌ Erro ao fazer parse: {e}")
            all_ok = False
    else:
        logger.error("   ❌ SECRET_GOOGLE_SERVICE_ACCOUNT não encontrado!")
        all_ok = False
    
    # 2. Verificar SECRET_GOOGLE_ADS_CONFIG
    logger.info("\n📋 2/3 - Verificando configuração do Google Ads...")
    ads_config_json = os.getenv("SECRET_GOOGLE_ADS_CONFIG")
    
    if not ads_config_json:
        logger.error("   ❌ SECRET_GOOGLE_ADS_CONFIG não encontrado!")
        all_ok = False
    else:
        logger.info(f"   ✅ SECRET_GOOGLE_ADS_CONFIG encontrado")
        try:
            config = json.loads(ads_config_json.replace(': True', ': true').replace(': False', ': false'))
            required_fields = ["developer_token", "client_id", "client_secret", "refresh_token", "login_customer_id"]
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                logger.error(f"   ❌ Campos faltando: {missing_fields}")
                all_ok = False
            else:
                logger.info("   ✅ Todos os campos obrigatórios presentes")
        except Exception as e:
            logger.error(f"   ❌ Erro ao validar: {e}")
            all_ok = False
    
    # 3. Verificar Customer IDs
    logger.info("\n📋 3/3 - Verificando Customer IDs...")
    logger.info(f"   ✅ {len(CUSTOMER_IDS)} Customer IDs configurados")
    
    logger.info("\n" + "=" * 80)
    if all_ok:
        logger.info("✅ TODOS OS SECRETS E CONFIGURAÇÕES ESTÃO OK!")
    else:
        logger.error("❌ PROBLEMAS ENCONTRADOS NA CONFIGURAÇÃO!")
    logger.info("=" * 80 + "\n")
    
    return all_ok

# ------------------------------------------------------------------------------
# CONFIGURAÇÃO DE CREDENCIAIS
# ------------------------------------------------------------------------------
def get_google_credentials():
    """
    Obtém credenciais do Google Cloud a partir de variáveis de ambiente.
    """
    try:
        secret_json = os.getenv("SECRET_GOOGLE_SERVICE_ACCOUNT")
        if secret_json:
            logger.info("✅ Usando credenciais do SECRET_GOOGLE_SERVICE_ACCOUNT")
            service_account_info = json.loads(secret_json)
            return service_account.Credentials.from_service_account_info(service_account_info)
        
        logger.info("✅ Usando Application Default Credentials")
        return None
        
    except Exception as e:
        logger.error("❌ Erro ao obter credenciais: %s", str(e))
        raise

def get_google_ads_config():
    """
    Obtém configuração do Google Ads a partir de variável de ambiente.
    """
    ads_config_json = os.getenv("SECRET_GOOGLE_ADS_CONFIG")
    if not ads_config_json:
        raise ValueError("❌ SECRET_GOOGLE_ADS_CONFIG não encontrado!")
    
    logger.info("✅ Carregando configuração do Google Ads")
    
    # Correção automática: Python → JSON
    ads_config_json = ads_config_json.replace(': True', ': true').replace(': False', ': false')
    ads_config_json = ads_config_json.replace(':True', ':true').replace(':False', ':false')
    
    try:
        config = json.loads(ads_config_json)
    except json.JSONDecodeError as e:
        logger.error(f"❌ Erro ao fazer parse do JSON: {e}")
        raise ValueError(f"JSON inválido: {e}")
    
    # Validar campos obrigatórios
    required_fields = ["developer_token", "client_id", "client_secret", "refresh_token", "login_customer_id"]
    missing_fields = [field for field in required_fields if field not in config]
    
    if missing_fields:
        raise ValueError(f"Campos obrigatórios faltando: {missing_fields}")
    
    # Configurar use_proto_plus
    config['use_proto_plus'] = True
    
    if 'token_uri' not in config:
        config['token_uri'] = "https://oauth2.googleapis.com/token"
    
    logger.info("✅ Configuração do Google Ads validada")
    return config

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
                bq_client = bigquery.Client(project="data-v1-423414")
            
            logger.info("✅ BigQuery client configurado!")
        except Exception as e:
            logger.error("❌ Erro ao configurar BigQuery: %s", str(e))
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
        logger.info("✅ Google Ads client configurado!")
        return client
    except Exception as e:
        logger.error("❌ Erro ao configurar Google Ads: %s", str(e))
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
        logger.info("✅ Tabela %s criada.", BIGQUERY_TABLE_ID)
        time.sleep(5)
    except Exception as e:
        logger.error("❌ Erro ao criar tabela: %s", e)

def get_google_ads_data(client, customer_id, max_retries=3):
    """
    Busca dados do Google Ads para um customer_id com retry logic.
    """
    query = f"""
        SELECT
            customer.id,
            customer.descriptive_name,
            campaign.id,
            campaign.name,
            segments.date,
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
        WHERE segments.date = '{anteontem}'
    """

    ga_service = client.get_service("GoogleAdsService")
    
    # Retry logic com backoff exponencial
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"   🔄 Tentativa {attempt}/{max_retries} para customer_id {customer_id}")
            
            response = ga_service.search(customer_id=customer_id, query=query)

            data = []
            imported_at = datetime.now(sao_paulo_tz)
            
            for row in response:
                budget = 0.0
                try:
                    if hasattr(row, "campaign_budget") and hasattr(row.campaign_budget, "amount_micros"):
                        budget = float(row.campaign_budget.amount_micros) / 1_000_000
                except Exception:
                    pass
                
                data.append({
                    "account_name": row.customer.descriptive_name if hasattr(row.customer, "descriptive_name") else "",
                    "account_id": str(row.customer.id) if hasattr(row.customer, "id") else "",
                    "campaign_id": str(row.campaign.id) if hasattr(row.campaign, "id") else "",
                    "campaign_name": row.campaign.name if hasattr(row.campaign, "name") else "",
                    "date": str(row.segments.date) if hasattr(row.segments, "date") else "",
                    "moeda": row.customer.currency_code if hasattr(row.customer, "currency_code") else "",
                    "budget": budget,
                    "spend": float(row.metrics.cost_micros / 1_000_000) if hasattr(row.metrics, "cost_micros") else 0.0,
                    "clicks": int(row.metrics.clicks) if hasattr(row.metrics, "clicks") else 0,
                    "cpc": float(row.metrics.average_cpc / 1_000_000) if hasattr(row.metrics, "average_cpc") else 0.0,
                    "impressions": int(row.metrics.impressions) if hasattr(row.metrics, "impressions") else 0,
                    "ctr": float(row.metrics.ctr) if hasattr(row.metrics, "ctr") else 0.0,
                    "conversions": float(row.metrics.conversions) if hasattr(row.metrics, "conversions") else 0.0,
                    "cost_per_conversion": float(row.metrics.cost_per_conversion / 1_000_000) if hasattr(row.metrics, "cost_per_conversion") else 0.0,
                    "imported_at": imported_at
                })

            logger.info(f"   ✅ Sucesso na tentativa {attempt}")
            return data
            
        except Exception as e:
            logger.warning(f"   ⚠️ Erro na tentativa {attempt}/{max_retries}: {str(e)}")
            
            if attempt == max_retries:
                logger.error(f"   ❌ Todas as {max_retries} tentativas falharam")
                raise
            
            wait_time = 2 ** attempt
            logger.info(f"   ⏳ Aguardando {wait_time} segundos...")
            time.sleep(wait_time)
    
    return []

def save_to_bigquery(data):
    """Salva dados no BigQuery."""
    if not data:
        logger.warning("⚠️ Nenhum dado para inserir.")
        return

    check_table_exists()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["imported_at"] = pd.to_datetime(df["imported_at"]).dt.tz_convert('UTC').dt.tz_localize(None)

    desired_order = [
        "account_name", "account_id", "campaign_id", "campaign_name",
        "date", "moeda", "budget", "spend", "clicks", "cpc",
        "impressions", "ctr", "conversions", "cost_per_conversion", "imported_at"
    ]
    df = df[desired_order]
    
    logger.info(f"   📊 Total de linhas: {len(df)}")

    bq_client = get_bq_client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq_client.load_table_from_dataframe(df, BIGQUERY_TABLE_ID, job_config=job_config)
    job.result()

    logger.info("✅ Dados inseridos com sucesso no BigQuery!")
    logger.info(f"   📊 Registros inseridos: {len(df)}")

# ------------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ------------------------------------------------------------------------------
def ca_google_ads_beforeyesterday(event=None, context=None):
    """
    Função principal para coleta de dados do Google Ads de ANTEONTEM.
    """
    logger.info("🚀 Iniciando coleta de dados do Google Ads (ANTEONTEM)...")
    logger.info("📅 Data: %s", anteontem)

    try:
        log_library_versions()
        
        logger.info("=" * 80)
        logger.info("🔐 VERIFICAÇÃO DE SECRETS")
        logger.info("=" * 80)
        
        if not verify_secrets():
            raise ValueError("❌ Falha na verificação de secrets")
        
        logger.info("✅ Secrets verificados!\n")
        
        logger.info("=" * 80)
        logger.info("🔧 CRIANDO CLIENTE GOOGLE ADS")
        logger.info("=" * 80)
        client = get_google_ads_client()
        logger.info("✅ Cliente criado!\n")

        logger.info("=" * 80)
        logger.info("📊 COLETANDO DADOS DAS CONTAS")
        logger.info("=" * 80)
        logger.info(f"Total de contas: {len(CUSTOMER_IDS)}\n")

        all_data = []
        success_count = 0
        error_count = 0
        errors_detail = []

        for idx, customer_id in enumerate(CUSTOMER_IDS, 1):
            logger.info(f"🔍 [{idx}/{len(CUSTOMER_IDS)}] Processando: {customer_id}")
            try:
                data = get_google_ads_data(client, customer_id, max_retries=3)
                if data:
                    logger.info(f"   ✅ {len(data)} registros extraídos")
                    all_data.extend(data)
                    success_count += 1
                else:
                    logger.warning(f"   ⚠️ Nenhum dado encontrado")
            except Exception as e:
                logger.error(f"   ❌ Erro: {str(e)}")
                error_count += 1
                errors_detail.append({"customer_id": customer_id, "error": str(e)})
            
            if idx < len(CUSTOMER_IDS):
                logger.info("   ⏳ Aguardando 1 segundo...")
                time.sleep(1)
            
            logger.info("")

        logger.info("=" * 80)
        logger.info("📈 RESUMO DA COLETA")
        logger.info("=" * 80)
        logger.info(f"✅ Sucesso: {success_count}/{len(CUSTOMER_IDS)}")
        logger.info(f"❌ Erros: {error_count}/{len(CUSTOMER_IDS)}")
        logger.info(f"📊 Total de registros: {len(all_data)}")
        logger.info("=" * 80 + "\n")

        if all_data:
            logger.info("=" * 80)
            logger.info("💾 SALVANDO NO BIGQUERY")
            logger.info("=" * 80)
            save_to_bigquery(all_data)
        else:
            logger.warning("⚠️ Nenhum dado para salvar")

        logger.info("=" * 80)
        logger.info("🎉 PROCESSAMENTO CONCLUÍDO")
        logger.info("=" * 80)
        return "✅ Processamento concluído"
    
    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("💥 ERRO CRÍTICO")
        logger.error("=" * 80)
        logger.error("Tipo: %s", type(e).__name__)
        logger.error("Mensagem: %s", str(e))
        logger.error("=" * 80)
        raise

# ------------------------------------------------------------------------------
# EXECUÇÃO LOCAL
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()
    logger.info("\n" + "=" * 80)
    logger.info("🚀 EXECUÇÃO LOCAL - GOOGLE ADS BEFOREYESTERDAY")
    logger.info("=" * 80)
    logger.info("📅 Data: %s", anteontem)
    logger.info("=" * 80 + "\n")
    
    try:
        result = ca_google_ads_beforeyesterday()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("🎯 RESUMO FINAL")
        logger.info("=" * 80)
        logger.info("✅ Status: SUCESSO")
        logger.info("⏱️ Tempo: %.2f segundos", execution_time)
        logger.info("📅 Data processada: %s", anteontem)
        logger.info("=" * 80)
        
    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.error("\n" + "=" * 80)
        logger.error("🎯 RESUMO FINAL")
        logger.error("=" * 80)
        logger.error("❌ Status: FALHA")
        logger.error("⏱️ Tempo: %.2f segundos", execution_time)
        logger.error("💥 Erro: %s", str(e))
        logger.error("=" * 80)
        raise
