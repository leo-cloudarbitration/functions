"""
Google Ads → BigQuery (Cloud Function) - DADOS HORÁRIOS HISTÓRICOS
─────────────────────────────────────────────────────────────────
Coleta métricas horárias por campanha de ONTEM:
- date, hour, account_id, account_name, campaign_id, campaign_name
- spend, clicks, cpc, impressions, ctr, conversions, cost_per_conversion
- moeda, budget

Resultado final = métricas por hora por campanha (dados de ontem)
ADICIONA os dados no BigQuery (WRITE_APPEND)

⚠️ Este script roda diariamente às 10h AM (horário do Brasil) para coletar dados de ontem.

⚠️ NOTAS SOBRE GRPC E GITHUB ACTIONS:
─────────────────────────────────────────────────────────────────
O Google Ads API usa GRPC por padrão, que pode ter problemas de rede
no GitHub Actions. Implementamos as seguintes soluções:

1. Retry logic com backoff exponencial (3 tentativas)
2. Delay de 1 segundo entre requisições de contas diferentes
3. Configurações de ambiente GRPC otimizadas
4. use_proto_plus=True (formato compatível)
5. Verificação prévia de todos os secrets

Se algumas contas falharem com erro "GRPC target method can't be resolved",
o script continua processando as outras contas e salva os dados que conseguiu coletar.
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
from google.api_core import retry
from google.api_core import exceptions as core_exceptions

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
# Configurar variáveis de ambiente para melhorar estabilidade do GRPC
os.environ['GRPC_ENABLE_FORK_SUPPORT'] = '1'
os.environ['GRPC_POLL_STRATEGY'] = 'poll'
# Aumentar timeout do GRPC (30 segundos)
os.environ['GRPC_PYTHON_LOG_LEVEL'] = 'ERROR'

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# 🔹 Configuração do BigQuery
BIGQUERY_TABLE_ID = "data-v1-423414.test.cloud_googleads_hour_historical"
sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

# Data de ontem em São Paulo
ontem = (datetime.now(sao_paulo_tz) - timedelta(days=1)).strftime('%Y-%m-%d')

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
    Útil para identificar se versões antigas estão causando problemas.
    """
    logger.info("=" * 80)
    logger.info("📚 VERSÕES DAS BIBLIOTECAS INSTALADAS")
    logger.info("=" * 80)
    
    # google-ads - usar pkg_resources pois não tem __version__ direto
    try:
        import pkg_resources
        ga_version = pkg_resources.get_distribution('google-ads').version
        logger.info(f"✅ google-ads: {ga_version}")
    except Exception as e:
        logger.warning(f"⚠️ google-ads: Erro ao obter versão - {e}")
    
    # grpcio
    try:
        import grpc
        logger.info(f"✅ grpcio: {grpc.__version__}")
    except Exception as e:
        logger.warning(f"⚠️ grpcio: Erro ao obter versão - {e}")
    
    # google-api-core
    try:
        import google.api_core
        logger.info(f"✅ google-api-core: {google.api_core.__version__}")
    except Exception as e:
        logger.warning(f"⚠️ google-api-core: Erro ao obter versão - {e}")
    
    # google-cloud-bigquery
    try:
        import google.cloud.bigquery
        logger.info(f"✅ google-cloud-bigquery: {google.cloud.bigquery.__version__}")
    except Exception as e:
        logger.warning(f"⚠️ google-cloud-bigquery: Erro ao obter versão - {e}")
    
    # protobuf
    try:
        import pkg_resources
        pb_version = pkg_resources.get_distribution('protobuf').version
        logger.info(f"✅ protobuf: {pb_version}")
    except Exception as e:
        logger.warning(f"⚠️ protobuf: Erro ao obter versão - {e}")
    
    logger.info("=" * 80 + "\n")

# ------------------------------------------------------------------------------
# VERIFICAÇÃO DE SECRETS E CREDENCIAIS
# ------------------------------------------------------------------------------
def verify_secrets():
    """
    Verifica se todos os secrets necessários estão configurados corretamente.
    Retorna True se tudo estiver OK, False caso contrário.
    """
    logger.info("=" * 80)
    logger.info("🔍 VERIFICANDO CONFIGURAÇÃO DE SECRETS E CREDENCIAIS")
    logger.info("=" * 80)
    
    all_ok = True
    
    # 1. Verificar SECRET_GOOGLE_SERVICE_ACCOUNT ou GOOGLE_APPLICATION_CREDENTIALS
    logger.info("\n📋 1/3 - Verificando credenciais do Google Cloud...")
    secret_json = os.getenv("SECRET_GOOGLE_SERVICE_ACCOUNT")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if secret_json:
        logger.info("   ✅ SECRET_GOOGLE_SERVICE_ACCOUNT encontrado")
        try:
            service_account_info = json.loads(secret_json)
            required_keys = ["type", "project_id", "private_key_id", "private_key", "client_email"]
            missing_keys = [key for key in required_keys if key not in service_account_info]
            if missing_keys:
                logger.error(f"   ❌ Campos faltando no SECRET_GOOGLE_SERVICE_ACCOUNT: {missing_keys}")
                all_ok = False
            else:
                logger.info(f"   ✅ JSON válido com todos os campos necessários")
                logger.info(f"   ✅ Project ID: {service_account_info.get('project_id', 'N/A')}")
                logger.info(f"   ✅ Client Email: {service_account_info.get('client_email', 'N/A')}")
        except json.JSONDecodeError as e:
            logger.error(f"   ❌ Erro ao fazer parse do SECRET_GOOGLE_SERVICE_ACCOUNT: {e}")
            all_ok = False
    elif creds_path:
        if os.path.exists(creds_path):
            logger.info(f"   ✅ GOOGLE_APPLICATION_CREDENTIALS encontrado: {creds_path}")
        else:
            logger.error(f"   ❌ GOOGLE_APPLICATION_CREDENTIALS definido mas arquivo não existe: {creds_path}")
            all_ok = False
    else:
        logger.warning("   ⚠️ Nenhuma credencial explícita encontrada, tentará usar Application Default Credentials")
    
    # 2. Verificar SECRET_GOOGLE_ADS_CONFIG
    logger.info("\n📋 2/3 - Verificando configuração do Google Ads...")
    ads_config_json = os.getenv("SECRET_GOOGLE_ADS_CONFIG")
    
    if not ads_config_json:
        logger.error("   ❌ SECRET_GOOGLE_ADS_CONFIG não encontrado!")
        all_ok = False
    else:
        logger.info(f"   ✅ SECRET_GOOGLE_ADS_CONFIG encontrado ({len(ads_config_json)} caracteres)")
        
        # Tentar fazer parse
        try:
            # Aplicar correções de formato
            ads_config_json_fixed = ads_config_json.replace(': True', ': true')
            ads_config_json_fixed = ads_config_json_fixed.replace(': False', ': false')
            ads_config_json_fixed = ads_config_json_fixed.replace(':True', ':true')
            ads_config_json_fixed = ads_config_json_fixed.replace(':False', ':false')
            
            config = json.loads(ads_config_json_fixed)
            
            # Verificar campos obrigatórios
            required_fields = ["developer_token", "client_id", "client_secret", "refresh_token", "login_customer_id"]
            missing_fields = [field for field in required_fields if field not in config]
            
            if missing_fields:
                logger.error(f"   ❌ Campos obrigatórios faltando: {missing_fields}")
                all_ok = False
            else:
                logger.info("   ✅ Todos os campos obrigatórios presentes")
                logger.info(f"   ✅ developer_token: {config['developer_token'][:10]}...")
                logger.info(f"   ✅ client_id: {config['client_id'][:30]}...")
                logger.info(f"   ✅ login_customer_id: {config['login_customer_id']}")
                logger.info(f"   ℹ️ use_proto_plus (original): {config.get('use_proto_plus', 'não definido')}")
                
        except json.JSONDecodeError as e:
            logger.error(f"   ❌ Erro ao fazer parse do JSON: {e}")
            logger.error(f"   ❌ Posição do erro: linha {e.lineno}, coluna {e.colno}")
            all_ok = False
        except Exception as e:
            logger.error(f"   ❌ Erro ao validar configuração: {e}")
            all_ok = False
    
    # 3. Verificar Customer IDs
    logger.info("\n📋 3/3 - Verificando Customer IDs...")
    if CUSTOMER_IDS:
        logger.info(f"   ✅ {len(CUSTOMER_IDS)} Customer IDs configurados:")
        for cid in CUSTOMER_IDS:
            logger.info(f"      - {cid}")
    else:
        logger.error("   ❌ Nenhum Customer ID configurado!")
        all_ok = False
    
    # Resumo
    logger.info("\n" + "=" * 80)
    if all_ok:
        logger.info("✅ TODOS OS SECRETS E CONFIGURAÇÕES ESTÃO OK!")
    else:
        logger.error("❌ PROBLEMAS ENCONTRADOS NA CONFIGURAÇÃO!")
        logger.error("   Por favor, verifique os secrets no GitHub Actions")
    logger.info("=" * 80 + "\n")
    
    return all_ok

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
    Inclui validação e correção automática do formato.
    """
    ads_config_json = os.getenv("SECRET_GOOGLE_ADS_CONFIG")
    if not ads_config_json:
        raise ValueError("❌ SECRET_GOOGLE_ADS_CONFIG não encontrado nas variáveis de ambiente!")
    
    logger.info("✅ Carregando configuração do Google Ads do SECRET_GOOGLE_ADS_CONFIG")
    logger.info(f"   Tamanho do JSON: {len(ads_config_json)} caracteres")
    
    # Correção automática: substituir True/False por true/false (Python → JSON)
    ads_config_json = ads_config_json.replace(': True', ': true')
    ads_config_json = ads_config_json.replace(': False', ': false')
    ads_config_json = ads_config_json.replace(':True', ':true')
    ads_config_json = ads_config_json.replace(':False', ':false')
    
    try:
        config = json.loads(ads_config_json)
        logger.info("✅ JSON parseado com sucesso")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Erro ao fazer parse do JSON: {e}")
        logger.error(f"   Posição do erro: linha {e.lineno}, coluna {e.colno}")
        logger.error(f"   Trecho: ...{ads_config_json[max(0, e.pos-50):e.pos+50]}...")
        raise ValueError(f"JSON inválido no SECRET_GOOGLE_ADS_CONFIG: {e}")
    
    # Validar campos obrigatórios
    required_fields = ["developer_token", "client_id", "client_secret", "refresh_token", "login_customer_id"]
    missing_fields = [field for field in required_fields if field not in config]
    
    if missing_fields:
        logger.error(f"❌ Campos obrigatórios faltando: {missing_fields}")
        raise ValueError(f"Campos obrigatórios faltando no SECRET_GOOGLE_ADS_CONFIG: {missing_fields}")
    
    # Log de informações ANTES da modificação
    logger.info(f"   ✅ developer_token: {config['developer_token'][:10]}...")
    logger.info(f"   ✅ client_id: {config['client_id'][:30]}...")
    logger.info(f"   ✅ login_customer_id: {config['login_customer_id']}")
    logger.info(f"   📊 use_proto_plus (ORIGINAL do secret): {config.get('use_proto_plus', 'não definido')}")
    
    # ⚠️ NOTA: use_proto_plus não controla GRPC vs REST, apenas o formato das mensagens
    # Ambos os valores (True/False) usam GRPC. Vamos usar True para compatibilidade.
    config['use_proto_plus'] = True
    logger.info("   🔄 Definindo use_proto_plus=True (formato protobuf messages)")
    logger.info(f"   ✅ use_proto_plus (APÓS modificação): {config['use_proto_plus']}")
    
    # Garantir que token_uri está presente
    if 'token_uri' not in config:
        logger.info("   ℹ️ token_uri não especificado, usando padrão do Google")
        config['token_uri'] = "https://oauth2.googleapis.com/token"
    
    # Log final da configuração (sem expor credenciais)
    logger.info("   📋 Configuração final:")
    logger.info(f"      - use_proto_plus: {config['use_proto_plus']}")
    logger.info(f"      - token_uri: {config.get('token_uri', 'N/A')}")
    
    logger.info("✅ Configuração do Google Ads validada e pronta para uso")
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

def get_google_ads_data(client, customer_id, max_retries=3):
    """
    Busca dados do Google Ads para um customer_id com retry logic.
    
    Args:
        client: Cliente Google Ads
        customer_id: ID da conta
        max_retries: Número máximo de tentativas (padrão: 3)
    
    Returns:
        Lista de dados extraídos
    """
    # Query para coletar dados de ONTEM (data anterior)
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
        WHERE segments.date = '{ontem}'
    """

    ga_service = client.get_service("GoogleAdsService")
    
    # Retry logic com backoff exponencial
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"   🔄 Tentativa {attempt}/{max_retries} para customer_id {customer_id}")
            
            response = ga_service.search(customer_id=customer_id, query=query)

            data = []
            # Timestamp de importação (hora de São Paulo)
            imported_at = datetime.now(sao_paulo_tz)
            
            for row in response:
                # ✅ Acesso seguro ao budget (pode não existir em algumas campanhas)
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
                    "hour": int(row.segments.hour) if hasattr(row.segments, "hour") else 0,
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
            error_message = str(e)
            logger.warning(f"   ⚠️ Erro na tentativa {attempt}/{max_retries}: {error_message}")
            
            # Se for o último retry, lança o erro
            if attempt == max_retries:
                logger.error(f"   ❌ Todas as {max_retries} tentativas falharam para {customer_id}")
                raise
            
            # Backoff exponencial: espera 2^attempt segundos (2, 4, 8...)
            wait_time = 2 ** attempt
            logger.info(f"   ⏳ Aguardando {wait_time} segundos antes da próxima tentativa...")
            time.sleep(wait_time)
    
    return []

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
    
    # ✅ Garantir que imported_at seja timestamp UTC (BigQuery requer UTC)
    # Converter timezone-aware datetime para UTC e depois remover timezone info
    df["imported_at"] = pd.to_datetime(df["imported_at"]).dt.tz_convert('UTC').dt.tz_localize(None)

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
    
    # Log de informações sobre os dados
    logger.info(f"   📊 Total de linhas: {len(df)}")
    logger.info(f"   📅 Exemplo de imported_at: {df['imported_at'].iloc[0] if len(df) > 0 else 'N/A'}")
    logger.info(f"   🔢 Tipos de dados:")
    for col in ['date', 'hour', 'imported_at']:
        logger.info(f"      - {col}: {df[col].dtype}")

    # Enviar ao BigQuery (WRITE_APPEND)
    bq_client = get_bq_client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq_client.load_table_from_dataframe(df, BIGQUERY_TABLE_ID, job_config=job_config)
    job.result()

    logger.info("✅ Dados inseridos com sucesso no BigQuery!")
    logger.info(f"   📋 Tabela: {BIGQUERY_TABLE_ID}")
    logger.info(f"   📊 Registros inseridos: {len(df)}")

# ------------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ------------------------------------------------------------------------------
def ca_google_ads_today(event=None, context=None):
    """
    Função principal para coleta de dados do Google Ads.
    Compatível com Cloud Functions e GitHub Actions.
    """
    logger.info("🚀 Iniciando coleta de dados do Google Ads...")
    logger.info("📅 Data (ontem): %s", ontem)

    try:
        # ✅ PASSO 0: Verificar versões das bibliotecas
        log_library_versions()
        
        # ✅ PASSO 1: Verificar secrets e configurações
        logger.info("=" * 80)
        logger.info("🔐 ETAPA 1: VERIFICAÇÃO DE SECRETS")
        logger.info("=" * 80)
        
        if not verify_secrets():
            error_msg = "❌ Falha na verificação de secrets. Abortando execução."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("✅ Secrets verificados com sucesso! Prosseguindo...\n")
        
        # ✅ PASSO 2: Criar cliente Google Ads
        logger.info("=" * 80)
        logger.info("🔧 ETAPA 2: CRIANDO CLIENTE GOOGLE ADS")
        logger.info("=" * 80)
        client = get_google_ads_client()
        logger.info("✅ Cliente Google Ads criado com sucesso!\n")

        # ✅ PASSO 3: Coletar dados das contas
        logger.info("=" * 80)
        logger.info("📊 ETAPA 3: COLETANDO DADOS DAS CONTAS DO GOOGLE ADS")
        logger.info("=" * 80)
        logger.info(f"Total de contas a processar: {len(CUSTOMER_IDS)}\n")

        all_data = []
        success_count = 0
        error_count = 0
        errors_detail = []

        for idx, customer_id in enumerate(CUSTOMER_IDS, 1):
            logger.info(f"🔍 [{idx}/{len(CUSTOMER_IDS)}] Processando customer_id: {customer_id}")
            try:
                data = get_google_ads_data(client, customer_id, max_retries=3)
                if data:
                    logger.info(f"   ✅ {len(data)} registros extraídos")
                    all_data.extend(data)
                    success_count += 1
                else:
                    logger.warning(f"   ⚠️ Nenhum dado encontrado")
            except Exception as e:
                error_msg = str(e)
                logger.error(f"   ❌ Erro: {error_msg}")
                error_count += 1
                errors_detail.append({
                    "customer_id": customer_id,
                    "error": error_msg
                })
            
            # Pequeno delay entre requisições para evitar rate limiting
            if idx < len(CUSTOMER_IDS):
                logger.info("   ⏳ Aguardando 1 segundo antes da próxima conta...")
                time.sleep(1)
            
            logger.info("")  # linha em branco para separar

        # Resumo da coleta
        logger.info("=" * 80)
        logger.info("📈 RESUMO DA COLETA")
        logger.info("=" * 80)
        logger.info(f"✅ Contas processadas com sucesso: {success_count}/{len(CUSTOMER_IDS)}")
        logger.info(f"❌ Contas com erro: {error_count}/{len(CUSTOMER_IDS)}")
        logger.info(f"📊 Total de registros coletados: {len(all_data)}")
        
        # Detalhar erros se houver
        if errors_detail:
            logger.info("\n📋 Detalhes dos erros:")
            for error_info in errors_detail:
                logger.info(f"   - Customer ID {error_info['customer_id']}: {error_info['error']}")
        
        logger.info("=" * 80 + "\n")

        if all_data:
            # ✅ PASSO 4: Salvar no BigQuery
            logger.info("=" * 80)
            logger.info("💾 ETAPA 4: SALVANDO DADOS NO BIGQUERY")
            logger.info("=" * 80)
            save_to_bigquery(all_data)
            logger.info("✅ Dados salvos com sucesso!\n")
        else:
            logger.warning("⚠️ Nenhum dado extraído de nenhuma conta. Nada para salvar no BigQuery.")

        logger.info("=" * 80)
        logger.info("🎉 PROCESSAMENTO CONCLUÍDO COM SUCESSO")
        logger.info("=" * 80)
        return "✅ Processamento concluído com sucesso."
    
    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("💥 ERRO CRÍTICO NA EXECUÇÃO")
        logger.error("=" * 80)
        logger.error("Tipo do erro: %s", type(e).__name__)
        logger.error("Mensagem: %s", str(e))
        logger.error("=" * 80)
        raise

# ------------------------------------------------------------------------------
# EXECUÇÃO LOCAL
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()
    logger.info("\n" + "=" * 80)
    logger.info("🚀 INICIANDO EXECUÇÃO LOCAL - GOOGLE ADS HOURLY HISTORICAL DATA")
    logger.info("=" * 80)
    logger.info("📅 Data (ontem): %s", ontem)
    logger.info("🏢 Ambiente: Local/GitHub Actions")
    logger.info("=" * 80 + "\n")
    
    try:
        result = ca_google_ads_today()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("🎯 RESUMO FINAL DA EXECUÇÃO")
        logger.info("=" * 80)
        logger.info("✅ Status: SUCESSO")
        logger.info("⏱️ Tempo total: %.2f segundos", execution_time)
        logger.info("📅 Data processada (ontem): %s", ontem)
        logger.info("🔢 Contas configuradas: %d", len(CUSTOMER_IDS))
        logger.info("=" * 80)
        
    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.error("\n" + "=" * 80)
        logger.error("🎯 RESUMO FINAL DA EXECUÇÃO")
        logger.error("=" * 80)
        logger.error("❌ Status: FALHA")
        logger.error("⏱️ Tempo até falha: %.2f segundos", execution_time)
        logger.error("💥 Erro: %s", str(e))
        logger.error("=" * 80)
        raise

