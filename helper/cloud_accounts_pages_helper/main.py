# -*- coding: utf-8 -*-
"""
Supabase → BigQuery - Accounts Pages Helper
─────────────────────────────────────────────────────────────────────────
Sincroniza dados de accounts_pages do Supabase para BigQuery.
Normaliza facebook_tokens array → 1 row per token × page.

Campos da tabela:
- page_supabase_id: STRING  (UUID from Supabase)
- page_name: STRING
- facebook_id: STRING       (numeric Facebook ID)
- fb_token_key: STRING      (token key: CASFA, CASFB, etc)
- enable: BOOLEAN
- facebook_status: STRING   (Live/Cool Down/Blocked)
- ads_running: INTEGER
- ads_limit: INTEGER
- ads_with_issues: INTEGER
- has_capacity: BOOLEAN     (ads_running < ads_limit * 0.80)
- imported_at: TIMESTAMP

Execução: GitHub Actions (diário)
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
BIGQUERY_TABLE_NAME = "supabase_accounts_pages"
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
                logger.info("Usando credenciais do GitHub Actions via arquivo JSON")
                bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
            else:
                logger.info("Usando Application Default Credentials")
                bq_client = bigquery.Client(project=BIGQUERY_PROJECT)

            logger.info("BigQuery client configurado com sucesso!")
        except Exception as e:
            logger.error(f"Erro ao configurar BigQuery client: {e}")
            bq_client = None
    return bq_client


# ------------------------------------------------------------------------------
# SUPABASE DATA SOURCE (com paginação)
# ------------------------------------------------------------------------------
def fetch_supabase_paginated(table, select, page_size=1000):
    """Lê dados do Supabase via REST API com paginação automática."""
    if not SUPABASE_SERVICE_KEY:
        raise ValueError("SUPABASE_KEY não configurado!")

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Prefer": "count=exact",
    }

    all_rows = []
    offset = 0

    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        params = {
            "select": select,
            "limit": page_size,
            "offset": offset,
        }

        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        batch = response.json()
        if not batch:
            break

        all_rows.extend(batch)
        logger.info(f"  Supabase {table}: offset={offset}, batch={len(batch)}, total={len(all_rows)}")

        if len(batch) < page_size:
            break

        offset += page_size

    return all_rows


def get_accounts_pages_data():
    """Lê accounts_pages do Supabase e normaliza facebook_tokens → 1 row por token."""
    logger.info(f"Acessando Supabase: {SUPABASE_URL}")

    select = "id,name,facebook_id,facebook_tokens,enable,facebook_status,ads_running,ads_limit,ads_with_issues"
    pages = fetch_supabase_paginated("accounts_pages", select)
    logger.info(f"Supabase: {len(pages)} pages obtidas")

    rows = []
    for page in pages:
        facebook_tokens = page.get("facebook_tokens") or []
        # Se não tem tokens, ainda cria uma row com fb_token_key vazio
        if not facebook_tokens:
            facebook_tokens = [None]

        ads_running = page.get("ads_running") or 0
        ads_limit = page.get("ads_limit") or 0

        for token_key in facebook_tokens:
            rows.append({
                "page_supabase_id": str(page.get("id", "")),
                "page_name": page.get("name", ""),
                "facebook_id": str(page.get("facebook_id", "") or ""),
                "fb_token_key": token_key,
                "enable": bool(page.get("enable", False)),
                "facebook_status": page.get("facebook_status", ""),
                "ads_running": int(ads_running),
                "ads_limit": int(ads_limit),
                "ads_with_issues": int(page.get("ads_with_issues") or 0),
                "has_capacity": ads_running < ads_limit * 0.80 if ads_limit > 0 else False,
            })

    logger.info(f"{len(rows)} linhas normalizadas de {len(pages)} pages")
    return rows


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Converte tipos de dados do DataFrame para os tipos esperados no BigQuery."""

    if "page_supabase_id" in df.columns:
        df["page_supabase_id"] = df["page_supabase_id"].astype(str).str.strip()

    if "page_name" in df.columns:
        df["page_name"] = df["page_name"].astype(str).str.strip()

    if "facebook_id" in df.columns:
        df["facebook_id"] = (
            df["facebook_id"]
            .astype(str)
            .str.replace(r"\.0+$", "", regex=True)
            .str.strip()
        )

    if "fb_token_key" in df.columns:
        df["fb_token_key"] = df["fb_token_key"].astype(str).str.strip()
        # Replace "None" string (from None values) with empty string
        df["fb_token_key"] = df["fb_token_key"].replace("None", "")

    if "enable" in df.columns:
        df["enable"] = df["enable"].astype(bool)

    if "facebook_status" in df.columns:
        df["facebook_status"] = df["facebook_status"].fillna("").astype(str).str.strip()

    for int_col in ["ads_running", "ads_limit", "ads_with_issues"]:
        if int_col in df.columns:
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype(int)

    if "has_capacity" in df.columns:
        df["has_capacity"] = df["has_capacity"].astype(bool)

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
        logger.warning("DataFrame vazio. Pulando upload para BQ.")
        return

    if client is None:
        logger.error("Cliente BigQuery não configurado")
        return

    schema = [
        bigquery.SchemaField("page_supabase_id", "STRING"),
        bigquery.SchemaField("page_name", "STRING"),
        bigquery.SchemaField("facebook_id", "STRING"),
        bigquery.SchemaField("fb_token_key", "STRING"),
        bigquery.SchemaField("enable", "BOOLEAN"),
        bigquery.SchemaField("facebook_status", "STRING"),
        bigquery.SchemaField("ads_running", "INTEGER"),
        bigquery.SchemaField("ads_limit", "INTEGER"),
        bigquery.SchemaField("ads_with_issues", "INTEGER"),
        bigquery.SchemaField("has_capacity", "BOOLEAN"),
        bigquery.SchemaField("imported_at", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    try:
        logger.info(f"Enviando {len(df)} registros para {table_id}...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"{job.output_rows} registros salvos em {table_id}")
    except Exception as e:
        logger.error(f"Erro ao enviar dados para BigQuery: {e}")
        raise


# ------------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ------------------------------------------------------------------------------
def sync_accounts_pages():
    """Função principal: sincroniza accounts_pages do Supabase para BigQuery."""
    try:
        logger.info("Iniciando sincronização de Accounts Pages (Supabase -> BigQuery)...")

        # 1. Buscar dados do Supabase (com paginação)
        rows = get_accounts_pages_data()

        if not rows:
            logger.warning("Nenhum dado de accounts_pages encontrado no Supabase")
            return

        # 2. Converter para DataFrame
        df = pd.DataFrame(rows)
        logger.info(f"Colunas encontradas: {list(df.columns)}")

        # 3. Coerção de tipos
        df = coerce_types(df)
        logger.info(f"DataFrame processado: {len(df)} registros")

        # 4. Upload para BigQuery
        upload_to_bigquery(df, TABLE_ID)

        logger.info("Sincronização concluída com sucesso!")

    except Exception as e:
        logger.error(f"Erro durante sincronização: {e}")
        raise


def main():
    """Função principal para execução via GitHub Actions."""
    logger.info("Iniciando Accounts Pages Helper (Supabase -> BigQuery)...")
    logger.info(f"Timestamp: {datetime.now(timezone('America/Sao_Paulo'))}")

    try:
        sync_accounts_pages()
        logger.info("Accounts Pages Helper concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        raise


# === Entrypoint: CloudEvent de Pub/Sub (Gen2 / Cloud Run) ===
def run_code(cloud_event):
    """
    Handler para Eventarc/Cloud Pub/Sub (CloudEvent).
    Mantido para compatibilidade com Cloud Functions.
    """
    try:
        sync_accounts_pages()
        return "OK"
    except Exception as e:
        logger.exception(f"Processing error: {e}")
        return "Error"


if __name__ == "__main__":
    main()
