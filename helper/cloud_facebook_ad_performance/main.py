# -*- coding: utf-8 -*-
"""
Facebook Ad-Level Performance → BigQuery (pipeline principal)
─────────────────────────────────────────────────────────────────────────
Busca spend/clicks/cpc por ad_id diário via Facebook Marketing API.
Contas e tokens dinâmicos do Supabase (sem hardcode).
Grava em: data-v1-423414.test.cloud_facebook_adsperformance_historical

Write mode: DELETE range + WRITE_APPEND (dedup seguro).

Execução:
  - GitHub Actions (diário, 13:00 UTC / 10:00 BRT)
  - Local: python main.py
  - Env: LOOKBACK_DAYS=1 (default), SUPABASE_URL, SUPABASE_KEY
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from pytz import timezone
from google.cloud import bigquery

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Supabase (para buscar contas e tokens)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://gcqhdzafdqtjxqvrqpqu.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# Facebook
FB_API_VERSION = "v24.0"
FB_BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"

# BigQuery
BIGQUERY_PROJECT = "data-v1-423414"
BIGQUERY_DATASET = "test"
BIGQUERY_TABLE = "cloud_facebook_adsperformance_historical"
TABLE_ID = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# Lookback window (days) — default 1 = yesterday only
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "1"))

# Parallelism
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

# ------------------------------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------------------------------
bq_client = None


def get_bq_client():
    global bq_client
    if bq_client is None:
        bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
        logger.info("BigQuery client configured")
    return bq_client


# ------------------------------------------------------------------------------
# SUPABASE HELPERS
# ------------------------------------------------------------------------------
def supabase_get(table, params=""):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    r = requests.get(url, headers=SUPABASE_HEADERS)
    if r.status_code != 200:
        raise RuntimeError(f"Supabase GET {table} failed ({r.status_code}): {r.text[:300]}")
    return r.json()


def build_token_map_from_secret():
    """Build account_id → token map from SECRET_FACEBOOK_GROUPS_CONFIG."""
    raw = os.getenv("SECRET_FACEBOOK_GROUPS_CONFIG", "")
    if not raw:
        return {}
    groups = json.loads(raw)
    token_map = {}
    for group_data in groups.values():
        token = group_data.get("token", "")
        if not token:
            continue
        for acct in group_data.get("accounts", []):
            acct_id = acct.replace("act_", "")
            token_map[acct_id] = token
    logger.info(f"Loaded {len(token_map)} account tokens from SECRET_FACEBOOK_GROUPS_CONFIG")
    return token_map


def get_accounts_and_tokens():
    """Get all ad accounts with tokens (secret priority, Supabase fallback)."""
    # 1. Token map from GitHub secret (priority)
    secret_tokens = build_token_map_from_secret()

    # 2. Account list + metadata from Supabase
    accounts = supabase_get("accounts", "select=conta_anuncio,conta_anuncio_id,fb_token_key,currency&limit=200")

    # 3. Supabase tokens (fallback)
    tokens_raw = supabase_get("accounts_tokens", "select=fb_token_key,token")
    supabase_token_map = {t["fb_token_key"]: t["token"] for t in tokens_raw if t.get("token")}

    # 4. Merge: secret has priority, Supabase is fallback
    result = []
    secret_count = 0
    supabase_count = 0
    for acct in accounts:
        acct_id = str(acct.get("conta_anuncio_id", ""))
        token = secret_tokens.get(acct_id)
        if token:
            secret_count += 1
        else:
            tk = acct.get("fb_token_key", "")
            token = supabase_token_map.get(tk)
            if token:
                supabase_count += 1
        if token:
            acct["token"] = token
            result.append(acct)

    logger.info(f"Found {len(result)} accounts with valid tokens "
                f"({secret_count} from secret, {supabase_count} from Supabase, "
                f"{len(accounts)} total)")
    return result


# ------------------------------------------------------------------------------
# FACEBOOK API
# ------------------------------------------------------------------------------
def fetch_ad_insights(account_id, token, currency, account_name, date_start, date_end):
    """Fetch daily ad-level insights from Facebook Marketing API."""
    all_rows = []
    url = f"{FB_BASE_URL}/act_{account_id}/insights"

    params = {
        "access_token": token,
        "level": "ad",
        "fields": "ad_id,ad_name,campaign_name,campaign_id,spend,clicks,cpc,impressions",
        "time_increment": 1,  # daily breakdown
        "time_range": f'{{"since":"{date_start}","until":"{date_end}"}}',
        "limit": 500,
        "filtering": '[{"field":"spend","operator":"GREATER_THAN","value":"0"}]',
    }

    while True:
        r = requests.get(url, params=params)

        if r.status_code == 400:
            error = r.json().get("error", {})
            if error.get("code") == 17:  # Rate limit
                logger.warning(f"Rate limited on {account_id}, waiting 120s...")
                time.sleep(120)
                continue
            logger.error(f"Facebook API error for {account_id}: {error.get('message', r.text[:200])}")
            return all_rows

        if r.status_code != 200:
            logger.error(f"HTTP {r.status_code} for {account_id}: {r.text[:200]}")
            return all_rows

        data = r.json()
        for row in data.get("data", []):
            all_rows.append({
                "date_start": row.get("date_start"),
                "ad_id": row.get("ad_id", ""),
                "ad_name": row.get("ad_name", ""),
                "campaign_id": row.get("campaign_id", ""),
                "campaign_name": row.get("campaign_name", ""),
                "account_id": str(account_id),
                "account_name": account_name,
                "spend": float(row.get("spend", 0)),
                "clicks": int(row.get("clicks", 0)),
                "cpc": float(row.get("cpc", 0)) if row.get("cpc") else 0.0,
                "impressions": int(row.get("impressions", 0)),
                "currency": currency,
            })

        # Pagination
        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        url = next_url
        params = {}  # next URL includes all params

    return all_rows


# ------------------------------------------------------------------------------
# DATA PROCESSING
# ------------------------------------------------------------------------------
def process_data(all_rows):
    """Convert raw rows to DataFrame with proper types."""
    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Convert date_start to datetime (table schema is DATETIME)
    df["date_start"] = pd.to_datetime(df["date_start"])

    # Add import timestamp
    local_tz = timezone("America/Sao_Paulo")
    df["imported_at"] = datetime.now(local_tz)

    # Ensure types
    df["ad_id"] = df["ad_id"].astype(str)
    df["ad_name"] = df["ad_name"].astype(str)
    df["campaign_id"] = df["campaign_id"].astype(str)
    df["campaign_name"] = df["campaign_name"].astype(str)
    df["account_id"] = df["account_id"].astype(str)
    df["account_name"] = df["account_name"].astype(str)
    df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0).astype(int)
    df["cpc"] = pd.to_numeric(df["cpc"], errors="coerce").fillna(0)
    df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0).astype(int)
    df["currency"] = df["currency"].astype(str)

    return df


def delete_date_range(client, start_date, end_date):
    """Delete existing rows in the date range before inserting new ones."""
    query = f"""
    DELETE FROM `{TABLE_ID}`
    WHERE CAST(date_start AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    """
    logger.info(f"Deleting existing data for {start_date} to {end_date}...")
    job = client.query(query)
    job.result()
    logger.info(f"Deleted {job.num_dml_affected_rows} rows")


def upload_to_bigquery(df, table_id):
    """Upload DataFrame to BigQuery using DELETE+APPEND (dedup seguro)."""
    if df is None or df.empty:
        logger.info("No data to upload")
        return

    client = get_bq_client()

    schema = [
        bigquery.SchemaField("date_start", "DATETIME"),
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("ad_name", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("account_name", "STRING"),
        bigquery.SchemaField("spend", "FLOAT64"),
        bigquery.SchemaField("clicks", "INT64"),
        bigquery.SchemaField("cpc", "FLOAT64"),
        bigquery.SchemaField("impressions", "INT64"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("imported_at", "DATETIME"),
    ]

    # Delete existing data in the range first
    dates = df["date_start"].unique()
    start = str(min(dates))[:10]
    end = str(max(dates))[:10]
    delete_date_range(client, start, end)

    # Append new data (schema_update_options allows adding new columns like cpc, currency)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    logger.info(f"Uploading {len(df)} rows to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info(f"Uploaded {job.output_rows} rows to {table_id}")


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    """Fetch ad-level performance from all accounts and upload to BigQuery."""
    logger.info("Starting Facebook Ad Performance sync (historical pipeline)...")

    # Date range: yesterday by default (LOOKBACK_DAYS=1)
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    logger.info(f"Date range: {start_date} to {end_date}")

    # Get accounts and tokens
    accounts = get_accounts_and_tokens()
    if not accounts:
        logger.error("No accounts found with valid tokens")
        return

    # Fetch insights for each account (parallel)
    def fetch_account(acct):
        account_id = acct["conta_anuncio_id"]
        account_name = acct.get("conta_anuncio", str(account_id))
        token = acct["token"]
        currency = acct.get("currency", "BRL")
        logger.info(f"Fetching {account_name} ({account_id}, {currency})...")
        rows = fetch_ad_insights(account_id, token, currency, account_name, start_date, end_date)
        logger.info(f"  {account_name}: {len(rows)} rows")
        return rows

    all_rows = []
    logger.info(f"Fetching {len(accounts)} accounts with {MAX_WORKERS} workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_account, acct): acct for acct in accounts}
        for future in as_completed(futures):
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                acct = futures[future]
                logger.error(f"Error fetching {acct.get('conta_anuncio', '?')}: {e}")

    logger.info(f"Total rows fetched: {len(all_rows)}")

    # Process and upload
    df = process_data(all_rows)
    if df.empty:
        logger.warning("No data to upload")
        return

    upload_to_bigquery(df, TABLE_ID)
    logger.info("Facebook Ad Performance sync completed!")


def run_code(cloud_event=None):
    """Cloud Function entrypoint."""
    main()


if __name__ == "__main__":
    main()
