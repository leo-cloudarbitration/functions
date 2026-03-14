# -*- coding: utf-8 -*-
"""
Supabase → BigQuery - MAPEAMENTO DE CRIATIVOS
─────────────────────────────────────────────────────────────────────────
Sincroniza dados de mapeamento de criativos do Supabase para BigQuery:
🚀 Editado via Cursor - Deploy automático funcionando!
📁 Estrutura: helper/cloud_adsperformance_creative_mapping/ ✅

Busca dados da tabela Supabase e salva em:
- BigQuery: cloud_adsperformance_creative_mapping

SUBSTITUI os dados no BigQuery (WRITE_TRUNCATE) para manter sincronizado
"""

import os
import logging
import pandas as pd
from datetime import datetime
from pytz import timezone
from google.cloud import bigquery
from google.oauth2 import service_account

# Supabase
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None
    logging.warning("Biblioteca supabase não disponível. Instale com: pip install supabase")

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# Configurações Supabase (via environment variables)
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()  # Remove espaços em branco
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()  # Remove espaços em branco
SUPABASE_TABLE = "adsperfomance_creative_mapping"  # Nome fixo da tabela no Supabase

# Log de debug das configurações (sem expor a chave completa)
if SUPABASE_URL:
    logger.info(f"🔍 [CONFIG] SUPABASE_URL configurada: {SUPABASE_URL[:30]}...")
else:
    logger.error("❌ [CONFIG] SUPABASE_URL não configurada!")

if SUPABASE_KEY:
    logger.info(f"🔍 [CONFIG] SUPABASE_KEY configurada: {SUPABASE_KEY[:20]}... (len={len(SUPABASE_KEY)})")
    if SUPABASE_KEY.startswith("eyJ"):
        logger.warning("⚠️ [CONFIG] Detectada 'anon' key - use a 'service_role' key!")
    elif SUPABASE_KEY.startswith("sb_secret_"):
        logger.info("✅ [CONFIG] Secret key detectada (formato correto)")
    else:
        logger.warning(f"⚠️ [CONFIG] Formato de key desconhecido: {SUPABASE_KEY[:10]}...")
else:
    logger.error("❌ [CONFIG] SUPABASE_KEY não configurada!")

# Configurações BigQuery
BIGQUERY_PROJECT = "data-v1-423414"
BIGQUERY_DATASET = "test"
BIGQUERY_TABLE = "cloud_adsperformance_creative_mapping"
TABLE_ID = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

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
                bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
            else:
                # Cloud Function: usar Application Default Credentials
                logger.info("Usando Application Default Credentials para Cloud Function")
                bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
            
            logger.info("✅ BigQuery client configurado com sucesso!")
        except Exception as e:
            logger.error("❌ Erro ao configurar BigQuery client: %s", str(e))
            bq_client = None
    return bq_client

# ------------------------------------------------------------------------------
# SUPABASE CLIENT
# ------------------------------------------------------------------------------
def get_supabase_client() -> Client:
    """Cria e retorna cliente Supabase."""
    if not SUPABASE_AVAILABLE:
        raise ImportError("Biblioteca supabase não está instalada")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("❌ SUPABASE_URL ou SUPABASE_KEY não configurados")
        logger.error(f"   SUPABASE_URL presente: {bool(SUPABASE_URL)}")
        logger.error(f"   SUPABASE_KEY presente: {bool(SUPABASE_KEY)}")
        raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar configurados")
    
    try:
        logger.info(f"🔍 [DEBUG] Tentando conectar ao Supabase...")
        logger.info(f"🔍 [DEBUG] URL: {SUPABASE_URL[:30]}... (truncado)")
        logger.info(f"🔍 [DEBUG] Key: {SUPABASE_KEY[:20]}... (truncado)")
        
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Supabase client configurado com sucesso!")
        return supabase
    except Exception as e:
        logger.error(f"❌ Erro ao configurar Supabase client: {e}")
        logger.error(f"❌ Tipo do erro: {type(e).__name__}")
        raise

# ------------------------------------------------------------------------------
# FUNÇÕES PRINCIPAIS
# ------------------------------------------------------------------------------
def fetch_creative_mapping_from_supabase():
    """
    Busca dados de mapeamento de criativos do Supabase com paginação.
    """
    try:
        logger.info(f"🔍 Buscando dados da tabela '{SUPABASE_TABLE}' no Supabase...")
        
        supabase = get_supabase_client()
        
        # Buscar todos os registros com paginação
        all_data = []
        page_size = 1000  # Tamanho da página
        offset = 0
        
        while True:
            logger.info(f"   Buscando registros {offset} a {offset + page_size}...")
            response = supabase.table(SUPABASE_TABLE).select("*").range(offset, offset + page_size - 1).execute()
            
            if not response.data:
                break  # Não há mais dados
            
            all_data.extend(response.data)
            logger.info(f"   ✅ {len(response.data)} registros obtidos (total: {len(all_data)})")
            
            # Se retornou menos que o tamanho da página, chegamos ao fim
            if len(response.data) < page_size:
                break
            
            offset += page_size
        
        if not all_data:
            logger.warning("⚠️ Nenhum dado encontrado no Supabase")
            return []
        
        logger.info(f"✅ Total de {len(all_data)} registros obtidos do Supabase")
        return all_data
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do Supabase: {e}")
        raise

def create_creative_mapping_table(table_id: str):
    """Cria a tabela no BigQuery se não existir."""
    bq_client = get_bq_client()
    if not bq_client:
        logger.error("❌ Cliente BigQuery não configurado")
        return False
    
    try:
        # Schema baseado na estrutura real da tabela Supabase
        schema = [
            bigquery.SchemaField("facebook_ad_id", "STRING"),
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("creative_id", "STRING"),
            bigquery.SchemaField("creative_nome", "STRING"),
            bigquery.SchemaField("ad_account_id", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("imported_at", "DATETIME")
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        logger.info(f"✅ Tabela {table_id} criada/verificada com sucesso")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela {table_id}: {e}")
        return False

def upload_to_bigquery(df: pd.DataFrame, table_id: str):
    """
    Faz upload dos dados para o BigQuery.
    Usa WRITE_TRUNCATE para substituir todos os dados (sincronização completa).
    """
    from google.cloud import bigquery
    
    logger.info("🔍 [DEBUG] Iniciando upload_to_bigquery...")
    logger.info(f"🔍 [DEBUG] DataFrame: {df is not None}")
    logger.info(f"🔍 [DEBUG] DataFrame vazio: {df.empty if df is not None else 'N/A'}")
    logger.info(f"🔍 [DEBUG] Tabela: {table_id}")
    
    bq_client = get_bq_client()
    if df is None or bq_client is None:
        logger.error("❌ DataFrame nulo ou BigQuery não configurado.")
        return
    
    if df.empty:
        logger.info("⚠️ DataFrame vazio - nenhum dado para upload")
        return
    
    # Schema baseado na estrutura real da tabela Supabase
    schema = [
        bigquery.SchemaField("facebook_ad_id", "STRING"),
        bigquery.SchemaField("video_id", "STRING"),
        bigquery.SchemaField("creative_id", "STRING"),
        bigquery.SchemaField("creative_nome", "STRING"),
        bigquery.SchemaField("ad_account_id", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("imported_at", "DATETIME")
    ]
    
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Substitui todos os dados
        schema=schema
    )
    
    try:
        logger.info(f"📤 Enviando {len(df)} registros para {table_id}...")
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
        job.result()
        logger.info(f"✅ {job.output_rows} registros salvos em {table_id}")
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar dados ao BigQuery: {e}")
        raise

def sync_creative_mapping():
    """
    Função principal: sincroniza dados do Supabase para BigQuery.
    """
    try:
        logger.info("🚀 Iniciando sincronização de mapeamento de criativos...")
        
        # 1. Buscar dados do Supabase
        data = fetch_creative_mapping_from_supabase()
        
        if not data:
            logger.warning("⚠️ Nenhum dado para sincronizar")
            return
        
        # 2. Converter para DataFrame
        df = pd.DataFrame(data)
        
        # 3. Adicionar timestamp de importação
        local_tz = timezone("America/Sao_Paulo")
        df["imported_at"] = datetime.now(local_tz)
        
        # 4. Converter timestamps se necessário
        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"])
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"])
        
        logger.info(f"📊 DataFrame criado com {len(df)} registros")
        logger.info(f"📋 Colunas: {list(df.columns)}")
        
        # 5. Criar tabela se não existir
        if not create_creative_mapping_table(TABLE_ID):
            logger.error("❌ Falha ao criar tabela no BigQuery")
            return
        
        # 6. Upload para BigQuery
        upload_to_bigquery(df, TABLE_ID)
        
        logger.info("🎉 Sincronização concluída com sucesso!")
        
    except Exception as e:
        logger.error(f"❌ Erro durante sincronização: {e}")
        raise

def main():
    """
    Função principal para execução via GitHub Actions.
    """
    logger.info("🚀 Iniciando Creative Mapping Sync (Supabase → BigQuery)...")
    
    try:
        sync_creative_mapping()
        logger.info("✅ Creative Mapping Sync concluída com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro durante a execução: {e}")
        raise

if __name__ == "__main__":
    main()

