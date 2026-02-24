# -*- coding: utf-8 -*-
"""
Supabase → BigQuery - Currency Helper
─────────────────────────────────────────────────────────────────────────
Sincroniza dados de moeda das contas de anúncio do Supabase para BigQuery.

Campos da tabela:
- account_id: STRING
- account_name: STRING
- currency: STRING
- imported_at: TIMESTAMP

Execução: GitHub Actions (diário às 10:05h BRT)
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime
from pytz import timezone
from google.cloud import bigquery

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ---- Config por variáveis de ambiente ----
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://gcqhdzafdqtjxqvrqpqu.supabase.co")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_KEY", "")

# Configurações BigQuery
BIGQUERY_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "data-v1-423414")
BIGQUERY_DATASET = "test"
BIGQUERY_TABLE_NAME = "sheets_adaccount_currency"
TABLE_ID = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}"

# ------------------------------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------------------------------
bq_client = None

def get_bq_client():
    """Lazy loading do cliente BigQuery para evitar problemas de inicialização."""
    global bq_client
    if bq_client is None:
        try:
            if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
                logger.info("🔧 Usando credenciais do GitHub Actions via arquivo JSON")
                bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
            else:
                logger.info("🔧 Usando Application Default Credentials")
                bq_client = bigquery.Client(project=BIGQUERY_PROJECT)

            logger.info("✅ BigQuery client configurado com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao configurar BigQuery client: {e}")
            bq_client = None
    return bq_client


# ------------------------------------------------------------------------------
# SUPABASE DATA SOURCE
# ------------------------------------------------------------------------------
def get_supabase_currency_data():
    """Lê dados de currency do Supabase via REST API."""
    if not SUPABASE_SERVICE_KEY:
        raise ValueError("SUPABASE_KEY não configurado!")

    url = f"{SUPABASE_URL}/rest/v1/accounts"
    params = {"select": "conta_anuncio_id,conta_anuncio,currency"}
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }

    logger.info(f"📊 Acessando Supabase: {SUPABASE_URL}")

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    accounts = response.json()
    logger.info(f"✅ Supabase: {len(accounts)} contas obtidas")

    rows = []
    for account in accounts:
        currency = account.get("currency")
        if not currency:
            continue
        rows.append({
            "account_id": str(account.get("conta_anuncio_id", "")),
            "account_name": account.get("conta_anuncio", ""),
            "currency": currency,
        })

    logger.info(f"📋 {len(rows)} linhas de currency de {len(accounts)} contas")
    return rows


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte tipos de dados do DataFrame para os tipos esperados no BigQuery.

    Campos esperados:
    - account_id: STRING
    - account_name: STRING
    - currency: STRING
    """
    # Garantir que account_id seja string
    if "account_id" in df.columns:
        df["account_id"] = (
            df["account_id"]
            .astype(str)
            .str.replace(r"\.0+$", "", regex=True)
            .str.strip()
        )

    # Garantir que account_name seja string
    if "account_name" in df.columns:
        df["account_name"] = df["account_name"].astype(str).str.strip()

    # Garantir que currency seja string
    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str).str.strip().str.upper()

    # Remover linhas com valores nulos nos campos obrigatórios
    required_cols = [c for c in ["account_id", "currency"] if c in df.columns]
    if required_cols:
        df = df.dropna(subset=required_cols)

    # Adicionar timestamp de importação
    local_tz = timezone("America/Sao_Paulo")
    df["imported_at"] = datetime.now(local_tz)

    return df


def upload_to_bigquery(df: pd.DataFrame, table_id: str):
    """
    Faz upload dos dados para o BigQuery.
    Usa WRITE_TRUNCATE para substituir todos os dados (sincronização completa).
    """
    client = get_bq_client()

    if df is None or df.empty:
        logger.warning("⚠️ DataFrame vazio. Pulando upload para BQ.")
        return

    if client is None:
        logger.error("❌ Cliente BigQuery não configurado")
        return

    # Schema da tabela
    schema = [
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("account_name", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("imported_at", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    try:
        logger.info(f"📤 Enviando {len(df)} registros para {table_id}...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"✅ {job.output_rows} registros salvos em {table_id}")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar dados para BigQuery: {e}")
        raise

# ------------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ------------------------------------------------------------------------------
def sync_currency_data():
    """
    Função principal: sincroniza dados de currency do Supabase para BigQuery.
    """
    try:
        logger.info("🚀 Iniciando sincronização de Currency (Supabase → BigQuery)...")

        # 1. Buscar dados do Supabase
        rows = get_supabase_currency_data()

        if not rows:
            logger.warning("⚠️ Nenhum dado de currency encontrado no Supabase")
            return

        # 2. Converter para DataFrame
        df = pd.DataFrame(rows)
        logger.info(f"📋 Colunas encontradas: {list(df.columns)}")

        # 3. Coerção de tipos
        df = coerce_types(df)
        logger.info(f"📊 DataFrame processado: {len(df)} registros")

        # 4. Upload para BigQuery
        upload_to_bigquery(df, TABLE_ID)

        logger.info("🎉 Sincronização concluída com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro durante sincronização: {e}")
        raise


def main():
    """
    Função principal para execução via GitHub Actions.
    """
    logger.info("🚀 Iniciando Currency Helper (Supabase → BigQuery)...")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone('America/Sao_Paulo'))}")

    try:
        sync_currency_data()
        logger.info("✅ Currency Helper concluído com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro durante a execução: {e}")
        raise


# === Entrypoint: CloudEvent de Pub/Sub (Gen2 / Cloud Run) ===
def run_code(cloud_event):
    """
    Handler para Eventarc/Cloud Pub/Sub (CloudEvent).
    Mantido para compatibilidade com Cloud Functions.
    """
    try:
        sync_currency_data()
        return "OK"
    except Exception as e:
        logger.exception(f"Processing error: {e}")
        return "Error"


if __name__ == "__main__":
    main()
