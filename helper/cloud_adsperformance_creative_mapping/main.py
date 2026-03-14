# -*- coding: utf-8 -*-
"""
Supabase → BigQuery - MAPEAMENTO DE CRIATIVOS
Sincroniza adsperfomance_creative_mapping do Supabase para BigQuery (WRITE_TRUNCATE).
Usa REST API direto (requests) para evitar timeout do SDK supabase.
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime
from pytz import timezone
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Configurações
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
SUPABASE_TABLE = "adsperfomance_creative_mapping"

BIGQUERY_PROJECT = "data-v1-423414"
BIGQUERY_DATASET = "test"
BIGQUERY_TABLE = "cloud_adsperformance_creative_mapping"
TABLE_ID = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

SCHEMA = [
    bigquery.SchemaField("facebook_ad_id", "STRING"),
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("creative_id", "STRING"),
    bigquery.SchemaField("creative_nome", "STRING"),
    bigquery.SchemaField("ad_account_id", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("imported_at", "DATETIME"),
]

# BigQuery client (lazy)
bq_client = None

def get_bq_client():
    global bq_client
    if bq_client is None:
        bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
        logger.info("BigQuery client configurado")
    return bq_client


def fetch_creative_mapping():
    """Busca todos os registros via REST API com paginação por Range header."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar configurados")

    rpc_url = f"{SUPABASE_URL}/rest/v1/rpc/get_creative_mapping"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    logger.info("  Chamando RPC get_creative_mapping...")
    resp = requests.post(rpc_url, headers=headers, json={}, timeout=60)
    if resp.status_code >= 400:
        logger.error(f"  HTTP {resp.status_code}: {resp.text[:300]}")
        resp.raise_for_status()
    all_data = resp.json()

    logger.info(f"Total: {len(all_data)} registros do Supabase")
    return all_data


def upload_to_bigquery(df):
    client = get_bq_client()
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=SCHEMA,
    )
    logger.info(f"Enviando {len(df)} registros para {TABLE_ID}...")
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_cfg)
    job.result()
    logger.info(f"{job.output_rows} registros salvos em {TABLE_ID}")


def main():
    logger.info("Iniciando Creative Mapping Sync (Supabase -> BigQuery)...")

    data = fetch_creative_mapping()
    if not data:
        logger.warning("Nenhum dado para sincronizar")
        return

    df = pd.DataFrame(data)
    df["imported_at"] = datetime.now(timezone("America/Sao_Paulo"))
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"])

    logger.info(f"DataFrame: {len(df)} registros, colunas: {list(df.columns)}")
    upload_to_bigquery(df)
    logger.info("Sincronizacao concluida!")


if __name__ == "__main__":
    main()
