import os
import json
import logging
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# ---- Logging ----
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---- Config por variáveis de ambiente ----
SHEET_ID = os.getenv("SHEET_ID", "1Fsq0xbVtjZ71SajCyR9WDLr1S_tWHm_yhtRBqeJOpGM")
WORKSHEET = os.getenv("WORKSHEET", "adxfee")  # troque se a aba tiver outro nome
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "data-v1-423414.test.sheets_adxfee")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "data-v1-423414")

def get_service_account_credentials():
    """Carrega credenciais da service account via GitHub Actions ou arquivo local."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]

    # Opção 1: GitHub Actions Secret (SECRET_GOOGLE_SERVICE_ACCOUNT)
    service_account_json = os.getenv("SECRET_GOOGLE_SERVICE_ACCOUNT")
    if service_account_json:
        logger.info("✅ Carregando credenciais do SECRET_GOOGLE_SERVICE_ACCOUNT (GitHub Actions)")
        info = json.loads(service_account_json)
        return Credentials.from_service_account_info(info, scopes=scopes)

    # Opção 2: Arquivo via GOOGLE_APPLICATION_CREDENTIALS (GitHub Actions também pode usar)
    credentials_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_file and os.path.exists(credentials_file):
        logger.info(f"✅ Carregando credenciais do arquivo: {credentials_file}")
        return Credentials.from_service_account_file(credentials_file, scopes=scopes)

    # Opção 3: Arquivo local (desenvolvimento)
    service_account_file = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
    if os.path.exists(service_account_file):
        logger.info(f"✅ Carregando credenciais do arquivo local: {service_account_file}")
        return Credentials.from_service_account_file(service_account_file, scopes=scopes)

    raise FileNotFoundError(
        "❌ Nenhuma credencial encontrada!\n"
        "Configure uma das opções:\n"
        "  - GitHub Actions: adicione o secret 'SECRET_GOOGLE_SERVICE_ACCOUNT'\n"
        "  - Local: defina GOOGLE_APPLICATION_CREDENTIALS ou SERVICE_ACCOUNT_FILE"
    )


def get_google_sheet_data(credentials):
    """Lê dados do Google Sheets usando gspread."""
    import gspread
    from gspread.exceptions import SpreadsheetNotFound, APIError

    logger.info(f"📊 Acessando Google Sheets...")
    logger.info(f"   Sheet ID: {SHEET_ID}")
    logger.info(f"   Worksheet: {WORKSHEET}")
    
    try:
        client = gspread.authorize(credentials)
        logger.info("✅ Cliente gspread autorizado com sucesso")
        
        try:
            spreadsheet = client.open_by_key(SHEET_ID)
            logger.info(f"✅ Planilha encontrada: {spreadsheet.title}")
        except SpreadsheetNotFound:
            logger.error(f"❌ Planilha não encontrada! Sheet ID: {SHEET_ID}")
            logger.error("   Verifique se:")
            logger.error("   1. O SHEET_ID está correto")
            logger.error("   2. A Service Account tem acesso à planilha")
            logger.error("   3. A planilha não foi deletada ou movida")
            raise
        except APIError as e:
            logger.error(f"❌ Erro da API do Google Sheets: {e}")
            logger.error("   Verifique se a Service Account tem as permissões necessárias")
            raise
        
        try:
            ws = spreadsheet.worksheet(WORKSHEET)
            logger.info(f"✅ Aba '{WORKSHEET}' encontrada")
        except Exception as e:
            logger.error(f"❌ Aba '{WORKSHEET}' não encontrada na planilha!")
            logger.error(f"   Erro: {e}")
            logger.error("   Verifique se o nome da aba está correto")
            raise
        
        rows = ws.get_all_records()
        logger.info(f"✅ Sheet '{WORKSHEET}' @ {SHEET_ID}: {len(rows)} linhas obtidas")
        return rows
    except Exception as e:
        logger.error(f"❌ Erro ao acessar Google Sheets: {e}")
        raise


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Renomear xrate para adxfee se necessário (compatibilidade com schema BigQuery)
    if "xrate" in df.columns and "adxfee" not in df.columns:
        df["adxfee"] = pd.to_numeric(df["xrate"], errors="coerce")
        # Remover coluna xrate original se existir
        if "xrate" in df.columns:
            df = df.drop(columns=["xrate"])
    elif "adxfee" in df.columns:
        df["adxfee"] = pd.to_numeric(df["adxfee"], errors="coerce")

    if "xrate_eom" in df.columns:
        df["xrate_eom"] = pd.to_numeric(df["xrate_eom"], errors="coerce")

    if "network_code" in df.columns:
        df["network_code"] = (
            df["network_code"]
            .astype(str)                       # garante string
            .str.replace(r"\.0+$", "", regex=True)  # remove sufixo .0 se vier de número
            .str.strip()                       # remove espaços
        )

    # mantém só linhas válidas (verifica adxfee)
    required_cols = [c for c in ["date", "adxfee"] if c in df.columns]
    df = df.dropna(subset=required_cols)
    df["imported_at"] = pd.Timestamp.now(tz="UTC")
    return df


def upload_to_bigquery(df, table_id, client):
    if df is None or df.empty:
        logger.warning("Empty DataFrame. Skipping BQ load.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("adxfee", "FLOAT64"),
            bigquery.SchemaField("network_code", "STRING"),
            bigquery.SchemaField("imported_at", "TIMESTAMP"),
        ],
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info(f"BQ loaded {job.output_rows} rows into {table_id}")

# === Entrypoint: CloudEvent de Pub/Sub (Gen2 / Cloud Run) ===
def run_code(cloud_event):
    """
    Handler para Eventarc/Cloud Pub/Sub (CloudEvent).
    O payload pode ser lido em cloud_event.data['message']['data'] (base64) se precisar.
    """
    try:
        creds = get_service_account_credentials()
        rows = get_google_sheet_data(creds)

        if not rows:
            logger.warning("No data from sheet.")
            return "No data."

        df = pd.DataFrame(rows)
        df = coerce_types(df)

        bq = bigquery.Client(credentials=creds, project=PROJECT_ID)
        upload_to_bigquery(df, BIGQUERY_TABLE, bq)
        return "OK"
    except Exception as e:
        logger.exception(f"Processing error: {e}")
        return "Error"


# === Execução local / GitHub Actions ===
def main():
    """Função principal para execução local ou GitHub Actions."""
    try:
        logger.info("🚀 Iniciando coleta de dados do Google Sheets...")
        logger.info(f"📋 Configuração:")
        logger.info(f"   SHEET_ID: {SHEET_ID}")
        logger.info(f"   WORKSHEET: {WORKSHEET}")
        logger.info(f"   BIGQUERY_TABLE: {BIGQUERY_TABLE}")
        logger.info(f"   PROJECT_ID: {PROJECT_ID}")
        
        creds = get_service_account_credentials()
        rows = get_google_sheet_data(creds)

        if not rows:
            logger.warning("⚠️ Nenhum dado encontrado na planilha.")
            return

        df = pd.DataFrame(rows)
        logger.info(f"📊 DataFrame criado: {len(df)} linhas")
        
        df = coerce_types(df)
        logger.info(f"✅ Dados processados: {len(df)} linhas válidas")

        bq = bigquery.Client(credentials=creds, project=PROJECT_ID)
        upload_to_bigquery(df, BIGQUERY_TABLE, bq)
        logger.info("🎉 Processamento concluído com sucesso!")
    except Exception as e:
        logger.exception(f"❌ Erro no processamento: {e}")
        raise


if __name__ == "__main__":
    main()
