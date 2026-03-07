"""
cloud_av_adunit_hour_yesterday — Snapshot de performance do dia ANTERIOR + regras de preço atuais.

Captura performance de yesterday (dados completos com delay resolvido)
+ regras de preço atuais. Fecha os dados do dia anterior.

Grava em 2 tabelas BigQuery (WRITE_APPEND):
- test.cloud_av_adunit_hour (performance)
- test.cloud_av_pricingrules_hour (regras)

Roda 1x/dia às 03:00 BRT (06:00 UTC).
"""

import asyncio
import aiohttp
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz


# Configurações da API
API_BASE_URL = "https://external-api.activeview.app"
API_KEY = "Bearer 4694ab00080e22a60b44d5ad01dc508eae87a8db68e158b8d73dd1327db1b07f:3e578f881a0d25d355f9"
HEADERS = {"Authorization": API_KEY}

# Configurações do BigQuery
PROJECT_ID = "data-v1-423414"
DATASET_ID = "test"
TABLE_PERFORMANCE = "cloud_av_adunit_hour"
TABLE_RULES = "cloud_av_pricingrules_hour"

# 14 sites GAM
GAM_SITES = [
    {"network_id": "23152058020", "site": "onplif.com"},
    {"network_id": "23152058020", "site": "fintacle.com"},
    {"network_id": "23302708904", "site": "amigadamamae.com.br"},
    {"network_id": "23313676084", "site": "ifinane.com"},
    {"network_id": "23314451390", "site": "finr.com.br"},
    {"network_id": "22958804404", "site": "finanzco.com"},
    {"network_id": "22958804404", "site": "espacoextra.com.br"},
    {"network_id": "22958804404", "site": "vidadeproduto.com.br"},
    {"network_id": "22024304448", "site": "tecnologianocampo.com.br"},
    {"network_id": "22024304448", "site": "superinvestmentguide.com"},
    {"network_id": "23150219615", "site": "brasileirinho.blog.br"},
    {"network_id": "23295671757", "site": "bimviral.com"},
    {"network_id": "23123915180", "site": "investimentoagora.com.br"},
    {"network_id": "23124049988", "site": "vamosestudar.com.br"},
]

BRT = pytz.timezone("America/Sao_Paulo")


async def fetch_performance(session, network_id, site, date_str):
    """Busca performance por ad_unit via report endpoint (from-gam)."""
    url = f"{API_BASE_URL}/report/gam/custom/{network_id}/{site}/from-gam"
    params = {
        "start_date": date_str,
        "end_date": date_str,
        "dimensions": "DATE,SITE_NAME,URL_NAME,AD_UNIT_NAME",
        "metrics": (
            "AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS,"
            "AD_EXCHANGE_LINE_ITEM_LEVEL_CTR,"
            "AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE,"
            "AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM,"
            "PROGRAMMATIC_MATCH_RATE,"
            "AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE,"
            "AD_EXCHANGE_TOTAL_REQUESTS"
        ),
    }
    try:
        async with session.get(url, headers=HEADERS, params=params) as resp:
            resp.raise_for_status()
            data = (await resp.json())["response"]
            print(f"  ✓ Performance {site}: {len(data)} rows")
            return data
    except Exception as e:
        print(f"  ✗ Performance {site}: {e}")
        return []


async def fetch_rules(session, network_id, site):
    """Busca regras de preço atuais via rules endpoint."""
    url = f"{API_BASE_URL}/rules/{network_id}/{site}"
    try:
        async with session.get(url, headers=HEADERS) as resp:
            resp.raise_for_status()
            raw = await resp.json()
            data = raw.get("response", raw) if isinstance(raw, dict) else raw
            print(f"  ✓ Rules {site}: {len(data)} rules")
            return data
    except Exception as e:
        print(f"  ✗ Rules {site}: {e}")
        return []


async def fetch_site(session, network_id, site, date_str):
    """Busca performance + rules de um site em paralelo."""
    perf, rules = await asyncio.gather(
        fetch_performance(session, network_id, site, date_str),
        fetch_rules(session, network_id, site),
    )
    return site, network_id, perf, rules


def prepare_performance(data, captured_at):
    """Converte dados de performance para formato BigQuery."""
    rows = []
    for r in data:
        revenue = r.get("AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0) / 1_000_000
        ecpm = r.get("AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM", 0) / 1_000_000
        rows.append({
            "captured_at": captured_at,
            "date": r.get("DATE"),
            "site_name": r.get("SITE_NAME"),
            "url_name": r.get("URL_NAME"),
            "ad_unit_name": r.get("AD_UNIT_NAME"),
            "impressions": r.get("AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS", 0),
            "ctr": r.get("AD_EXCHANGE_LINE_ITEM_LEVEL_CTR", 0.0),
            "revenue": revenue,
            "average_ecpm": ecpm,
            "programmatic_match_rate": r.get("PROGRAMMATIC_MATCH_RATE"),
            "viewable_impressions_rate": r.get("AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE"),
            "total_requests": r.get("AD_EXCHANGE_TOTAL_REQUESTS", 0),
        })
    return rows


def prepare_rules(data, network_id, site, captured_at):
    """Converte dados de regras para formato BigQuery."""
    rows = []
    for r in data:
        rows.append({
            "captured_at": captured_at,
            "network_id": network_id,
            "site": site,
            "ad_unit": r.get("ad_unit"),
            "aggressiveness": r.get("aggressiveness"),
            "country": r.get("country"),
            "desired_match_rate": r.get("desired_match_rate"),
            "device": r.get("device"),
            "domain": r.get("domain"),
            "ecpm": r.get("ecpm"),
            "impressions": r.get("impressions"),
            "match_rate": r.get("match_rate"),
            "request_uri": r.get("request_uri"),
            "revenue": r.get("revenue"),
            "rule": r.get("rule"),
            "state": r.get("state"),
            "utm_source": r.get("utm_source"),
        })
    return rows


def write_to_bigquery(data, table_name, schema):
    """Insere dados no BigQuery (WRITE_APPEND)."""
    if not data:
        print(f"  Nenhum dado para {table_name}, pulando.")
        return

    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )
    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()
    print(f"  ✓ {len(data)} rows → {table_name}")


SCHEMA_PERFORMANCE = [
    bigquery.SchemaField("captured_at", "TIMESTAMP"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("site_name", "STRING"),
    bigquery.SchemaField("url_name", "STRING"),
    bigquery.SchemaField("ad_unit_name", "STRING"),
    bigquery.SchemaField("impressions", "INT64"),
    bigquery.SchemaField("ctr", "FLOAT64"),
    bigquery.SchemaField("revenue", "FLOAT64"),
    bigquery.SchemaField("average_ecpm", "FLOAT64"),
    bigquery.SchemaField("programmatic_match_rate", "FLOAT64"),
    bigquery.SchemaField("viewable_impressions_rate", "FLOAT64"),
    bigquery.SchemaField("total_requests", "INT64"),
]

SCHEMA_RULES = [
    bigquery.SchemaField("captured_at", "TIMESTAMP"),
    bigquery.SchemaField("network_id", "STRING"),
    bigquery.SchemaField("site", "STRING"),
    bigquery.SchemaField("ad_unit", "STRING"),
    bigquery.SchemaField("aggressiveness", "FLOAT64"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("desired_match_rate", "FLOAT64"),
    bigquery.SchemaField("device", "STRING"),
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("ecpm", "FLOAT64"),
    bigquery.SchemaField("impressions", "INT64"),
    bigquery.SchemaField("match_rate", "FLOAT64"),
    bigquery.SchemaField("request_uri", "STRING"),
    bigquery.SchemaField("revenue", "FLOAT64"),
    bigquery.SchemaField("rule", "FLOAT64"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("utm_source", "STRING"),
]


async def run_async():
    """Função principal assíncrona."""
    now_brt = datetime.now(BRT)
    captured_at = now_brt.isoformat()
    # Busca dados de ONTEM (yesterday)
    yesterday = (now_brt - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"=== AV Snapshot YESTERDAY — {captured_at} ===")
    print(f"Date (yesterday): {yesterday}")

    all_perf = []
    all_rules = []

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_site(session, s["network_id"], s["site"], yesterday)
            for s in GAM_SITES
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            print(f"  ✗ Exceção: {result}")
            continue
        site, network_id, perf_data, rules_data = result
        all_perf.extend(prepare_performance(perf_data, captured_at))
        all_rules.extend(prepare_rules(rules_data, network_id, site, captured_at))

    print(f"\nTotal: {len(all_perf)} perf rows, {len(all_rules)} rules rows")

    write_to_bigquery(all_perf, TABLE_PERFORMANCE, SCHEMA_PERFORMANCE)
    write_to_bigquery(all_rules, TABLE_RULES, SCHEMA_RULES)

    print("✓ Snapshot concluído.")


def run_code(event, context):
    """Wrapper para compatibilidade com Cloud Functions."""
    return asyncio.run(run_async())


if __name__ == "__main__":
    asyncio.run(run_async())
