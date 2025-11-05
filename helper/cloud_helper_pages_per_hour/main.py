# -*- coding: utf-8 -*-
"""
Google Sheets → BigQuery - SNAPSHOT DE PÁGINAS POR HORA
─────────────────────────────────────────────────────────────────────────
Sincroniza dados de páginas por hora do Google Sheets para BigQuery:
🚀 Criado via Cursor - Deploy automático funcionando!
📁 Estrutura: helper/cloud_helper_pages_per_hour/ ✅

Busca dados do Google Sheets e salva em:
- BigQuery: cloud_snapshot_page_per_hour

SUBSTITUI os dados no BigQuery (WRITE_TRUNCATE) para manter sincronizado
"""

import os
import logging
import pandas as pd
from datetime import datetime
from pytz import timezone
from google.cloud import bigquery
from google.oauth2 import service_account

# Google Sheets
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    logging.warning("Biblioteca gspread não disponível. Instale com: pip install gspread oauth2client")

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# Configurações Google Sheets
SHEETS_ID = "1hEKsS5VtOw58OKnO6clcSjtZ25ckm5urJSC5EcIV_Oo"
SHEETS_RANGE = "Sheet1"  # Ajuste se necessário

# Configurações BigQuery
BIGQUERY_PROJECT = "data-v1-423414"
BIGQUERY_DATASET = "test"
BIGQUERY_TABLE = "cloud_helper_page_per_hour"
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
# GOOGLE SHEETS CLIENT
# ------------------------------------------------------------------------------
def get_sheets_client():
    """Cria e retorna cliente Google Sheets."""
    if not GSPREAD_AVAILABLE:
        raise ImportError("Biblioteca gspread não está instalada")
    
    try:
        # Define o escopo
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Verificar se estamos no GitHub Actions
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            # GitHub Actions: usar credenciais do arquivo JSON
            creds_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            logger.info(f"🔍 Usando credenciais do arquivo: {creds_file}")
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        else:
            # Cloud Function: criar credenciais do ambiente
            logger.info("🔍 Usando credenciais do ambiente")
            # Você pode precisar ajustar isso dependendo de como as credenciais são fornecidas
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json"), 
                scope
            )
        
        client = gspread.authorize(creds)
        logger.info("✅ Google Sheets client configurado com sucesso!")
        return client
        
    except Exception as e:
        logger.error(f"❌ Erro ao configurar Google Sheets client: {e}")
        logger.error(f"❌ Tipo do erro: {type(e).__name__}")
        raise

# ------------------------------------------------------------------------------
# FUNÇÕES PRINCIPAIS
# ------------------------------------------------------------------------------
def fetch_data_from_sheets():
    """
    Busca dados do Google Sheets.
    """
    try:
        logger.info(f"🔍 Buscando dados do Google Sheets: {SHEETS_ID}...")
        
        client = get_sheets_client()
        
        # Abrir a planilha
        sheet = client.open_by_key(SHEETS_ID)
        worksheet = sheet.get_worksheet(0)  # Primeira aba
        
        # Buscar todos os dados
        data = worksheet.get_all_records()
        
        if not data:
            logger.warning("⚠️ Nenhum dado encontrado no Google Sheets")
            return []
        
        logger.info(f"✅ {len(data)} registros obtidos do Google Sheets")
        return data
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do Google Sheets: {e}")
        raise

def create_pages_table(table_id: str):
    """Cria a tabela no BigQuery se não existir."""
    bq_client = get_bq_client()
    if not bq_client:
        logger.error("❌ Cliente BigQuery não configurado")
        return False
    
    try:
        # Schema conforme especificado: url, category, category_mae
        schema = [
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("category_mae", "STRING"),
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
    
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Substitui todos os dados
        autodetect=True  # Detecta automaticamente o schema
    )
    
    try:
        logger.info(f"📤 Enviando {len(df)} registros para {table_id}...")
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
        job.result()
        logger.info(f"✅ {job.output_rows} registros salvos em {table_id}")
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar dados ao BigQuery: {e}")
        raise

def sync_pages_per_hour():
    """
    Função principal: sincroniza dados do Google Sheets para BigQuery.
    """
    try:
        logger.info("🚀 Iniciando sincronização de páginas por hora...")
        
        # 1. Buscar dados do Google Sheets
        data = fetch_data_from_sheets()
        
        if not data:
            logger.warning("⚠️ Nenhum dado para sincronizar")
            return
        
        # 2. Converter para DataFrame
        df = pd.DataFrame(data)
        
        # 3. Adicionar timestamp de importação
        local_tz = timezone("America/Sao_Paulo")
        df["imported_at"] = datetime.now(local_tz)
        
        logger.info(f"📊 DataFrame criado com {len(df)} registros")
        logger.info(f"📋 Colunas: {list(df.columns)}")
        
        # 4. Criar tabela se não existir
        if not create_pages_table(TABLE_ID):
            logger.error("❌ Falha ao criar tabela no BigQuery")
            return
        
        # 5. Upload para BigQuery
        upload_to_bigquery(df, TABLE_ID)
        
        logger.info("🎉 Sincronização concluída com sucesso!")
        
    except Exception as e:
        logger.error(f"❌ Erro durante sincronização: {e}")
        raise

def main():
    """
    Função principal para execução via GitHub Actions.
    """
    logger.info("🚀 Iniciando Pages Per Hour Sync (Google Sheets → BigQuery)...")
    
    try:
        sync_pages_per_hour()
        logger.info("✅ Pages Per Hour Sync concluída com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro durante a execução: {e}")
        raise

if __name__ == "__main__":
    main()

