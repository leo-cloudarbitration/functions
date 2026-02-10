# -*- coding: utf-8 -*-
"""
Facebook → BigQuery (Cloud Function) - DADOS DE HOJE
─────────────────────────────────────────────────────
Coleta:
1. Métricas diárias (/insights) – nível campaign
2. daily_budget nas campanhas (CBO)
3. daily_budget somado dos adsets (quando não é CBO)

Resultado final = métricas + orçamento (já convertido de centavos p/ moeda)
SOBRESCREVE os dados no BigQuery (WRITE_TRUNCATE)
"""

import os
import json
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import pytz
from google.cloud import bigquery
from google.oauth2 import service_account

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# CARREGAMENTO DE CONFIGURAÇÕES (JSON)
# ------------------------------------------------------------------------------
def load_config_from_json(json_path: str, config_name: str, env_var: str = None):
    """Carrega configuração de arquivo JSON ou variável de ambiente.
    
    Args:
        json_path: Caminho para o arquivo JSON
        config_name: Nome da configuração (para logs)
        env_var: Nome da variável de ambiente (opcional)
    """
    # Primeiro, tentar carregar de variável de ambiente (GitHub Actions/Cloud Function)
    if env_var:
        env_value = os.getenv(env_var)
        if env_value:
            try:
                loaded_config = json.loads(env_value)
                logger.info("✅ Carregado %s da variável de ambiente %s", config_name, env_var)
                return loaded_config
            except json.JSONDecodeError as e:
                raise ValueError(f"Erro ao decodificar JSON de {config_name} da variável {env_var}: {str(e)}")
    
    # Se não houver variável de ambiente, tentar carregar do arquivo
    normalized_path = os.path.abspath(os.path.normpath(json_path))
    
    if not os.path.exists(normalized_path):
        raise FileNotFoundError(f"Arquivo {config_name} não encontrado: {normalized_path}")
    
    try:
        with open(normalized_path, 'r', encoding='utf-8') as f:
            loaded_config = json.load(f)
            logger.info("✅ Carregado %s de %s", config_name, normalized_path)
            return loaded_config
    except json.JSONDecodeError as e:
        raise ValueError(f"Erro ao decodificar JSON de {config_name}: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Erro ao carregar {config_name} de {normalized_path}: {str(e)}")

# Carregar GROUPS de groups_config_utc.json ou variável de ambiente
GROUPS = load_config_from_json(
    json_path=os.path.join(os.path.dirname(__file__), "..", "groups_config_utc.json"),
    config_name="GROUPS",
    env_var="SECRET_FACEBOOK_GROUPS_CONFIG_UTC"
)

# Tabela do BigQuery onde todos os dados serão salvos
BIGQUERY_TABLE_ID = "data-v1-423414.test.cloud_facebook_today_utc_adjustments"

# Dados sempre agrupados por campanha + hora do dia
INCLUDE_HOURLY = True

# ------------------------------------------------------------------------------
# CONFIG PARA THREADS
# ------------------------------------------------------------------------------
# Número máximo de workers rodando em paralelo (threads)
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20"))
# Número máximo de tentativas de checagem do relatório no GAM
MAX_CHECKS = int(os.getenv("MAX_CHECKS", "18"))
# Tempo (em segundos) entre cada checagem
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "3"))
# Delay entre requisições para evitar rate limiting (em segundos)
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))
# Delay entre contas para distribuir a carga (em segundos)
ACCOUNT_DELAY = float(os.getenv("ACCOUNT_DELAY", "1.5"))

# ------------------------------------------------------------------------------
# FACEBOOK API HELPERS
# ------------------------------------------------------------------------------
# Campos básicos (sem conversions)
INSIGHTS_FIELDS_BASIC = (
    "account_id,account_name,campaign_id,campaign_name,"
    "date_start,date_stop,spend,objective,cpc,ctr,frequency,"
    "impressions,reach"
)

# Campos completos (com conversions)
INSIGHTS_FIELDS_FULL = (
    "account_id,account_name,campaign_id,campaign_name,"
    "date_start,date_stop,spend,objective,cpc,ctr,frequency,"
    "impressions,reach,conversions"
)

# Contas problemáticas que não suportam conversions ou têm dados excessivos
PROBLEMATIC_ACCOUNTS = {
    "act_1408181320053598",  # Não suporta conversions
    "act_1727835544421228",  # Dados excessivos - Cloud 010
    "act_517255721392274",   # Dados excessivos - CASF A 005
    "act_1230275922480187",  # Rate limit frequente
}


def fb_get(url: str, params: dict, retries: int = 0, context: str = "", max_rate_limit_retries: int = 3):
    """GET com back-off exponencial simples.
    
    Args:
        url: URL da requisição
        params: Parâmetros da requisição
        retries: Número de tentativas (0 = usar MAX_CHECKS)
        context: Contexto adicional para logs (ex: account_id)
        max_rate_limit_retries: Máximo de tentativas para rate limit (padrão: 3)
    """
    if retries == 0:
        retries = MAX_CHECKS
    
    # Delay inicial para evitar sobrecarregar a API
    time.sleep(REQUEST_DELAY)
    
    context_prefix = f"[{context}] " if context else ""
    rate_limit_attempts = 0
    
    for attempt in range(retries):
        resp = requests.get(url, params=params, timeout=60)
        if resp.ok:
            return resp.json()
        
        # Verificar se é rate limit
        is_rate_limit = False
        if resp.status_code == 400:
            error_msg = resp.text
            if "User request limit reached" in error_msg or "error_subcode" in error_msg:
                is_rate_limit = True
                rate_limit_attempts += 1
                logger.error("%sErro 400 - Rate limit atingido na conta (tentativa %s/%s): %s", 
                           context_prefix, rate_limit_attempts, max_rate_limit_retries, error_msg)
                
                # Se excedeu o limite de tentativas para rate limit, retornar None
                if rate_limit_attempts >= max_rate_limit_retries:
                    logger.warning("%sRate limit persistente após %s tentativas. Pulando esta requisição.", 
                                 context_prefix, max_rate_limit_retries)
                    return None
            else:
                logger.error("%sErro 400 - Parâmetros inválidos: %s", context_prefix, resp.text)
        elif resp.status_code == 401:
            logger.error("%sErro 401 - Token inválido ou expirado: %s", context_prefix, resp.text)
        elif resp.status_code == 403:
            logger.error("%sErro 403 - Sem permissão para acessar esta conta: %s", context_prefix, resp.text)
        elif resp.status_code == 404:
            logger.error("%sErro 404 - Recurso não encontrado: %s", context_prefix, resp.text)
        elif resp.status_code == 429:
            is_rate_limit = True
            rate_limit_attempts += 1
            logger.warning("%sErro 429 - Rate limit atingido (tentativa %s/%s), aguardando...: %s", 
                         context_prefix, rate_limit_attempts, max_rate_limit_retries, resp.text)
            
            # Se excedeu o limite de tentativas para rate limit, retornar None
            if rate_limit_attempts >= max_rate_limit_retries:
                logger.warning("%sRate limit persistente após %s tentativas. Pulando esta requisição.", 
                             context_prefix, max_rate_limit_retries)
                return None
        else:
            logger.warning("%sErro %s – tentativa %s/%s: %s",
                           context_prefix, resp.status_code, attempt + 1, retries, resp.text)
        
        # Back-off exponencial, com delay maior para rate limit
        if is_rate_limit:
            time.sleep(SLEEP_SECONDS * (2 ** attempt) * 2)  # Delay maior para rate limit
        else:
            time.sleep(SLEEP_SECONDS * (2 ** attempt))
    
    return None


# ---------- INSIGHTS ----------------------------------------------------------
def get_insights_page(account_id: str, token: str, after: str | None = None, is_lifetime: bool = False, use_smaller_limit: bool = False):
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    url = f"https://graph.facebook.com/v24.0/{account_id}/insights"
    
    # Escolher campos baseado na conta
    if account_id in PROBLEMATIC_ACCOUNTS:
        fields = INSIGHTS_FIELDS_BASIC
        logger.info("📊 [%s] Usando campos básicos (sem conversions) - conta problemática", account_id)
    else:
        fields = INSIGHTS_FIELDS_FULL
        logger.info("📊 [%s] Usando campos completos (com conversions)", account_id)
    
    params = {
        "access_token": token,
        "fields": fields,
        "time_increment": "all_days" if is_lifetime else "1",
        "date_preset": "today",  # Dados de hoje
        "level": "campaign",
        "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",  # Sempre incluir dados por hora
        "limit": 25 if use_smaller_limit else 50,  # Reduzido drasticamente: 50 para 25, 500 para 50
    }
    
    if after:
        params["after"] = after
    return fb_get(url, params, context=account_id)


def fetch_insights_all_accounts(accounts: list, token: str, is_lifetime: bool = False):
    rows = []

    def process_account(acc):
        acc_rows = []
        after = None
        
        # Delay entre contas para evitar sobrecarregar a API
        time.sleep(ACCOUNT_DELAY)
        logger.info("🔄 [INSIGHTS] Processando insights da conta %s...", acc)
        
        try:
            # Começar com limite pequeno para evitar dados excessivos
            use_smaller_limit = True
            while True:
                data = get_insights_page(acc, token, after, is_lifetime, use_smaller_limit=use_smaller_limit)
                if not data:
                    break
                acc_rows.extend(data.get("data", []))
                after = data.get("paging", {}).get("cursors", {}).get("after")
                if not after:
                    break
                # Se a primeira página funcionou, continuar com limite pequeno para ser conservador
                use_smaller_limit = True
        except Exception as e:
            logger.error("❌ [INSIGHTS] Erro ao processar conta %s: %s. Continuando com outras contas...", acc, str(e))
            # Retornar lista vazia para não quebrar o processamento
            return []
        
        return acc_rows

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_account, acc): acc for acc in accounts}
        for future in as_completed(futures):
            acc = futures[future]
            try:
                result = future.result()
                rows.extend(result)
                if len(result) > 0:
                    logger.info("✅ [INSIGHTS] Conta %s: %s registros de insights processados", acc, len(result))
                else:
                    logger.warning("⚠️ [INSIGHTS] Conta %s: nenhum dado processado (pode ter tido erros)", acc)
            except Exception as e:
                logger.error("❌ [INSIGHTS] Erro na conta %s: %s. Continuando com outras contas...", acc, str(e))

    return rows


# ---------- BUDGETS -----------------------------------------------------------
def get_campaign_budgets(account_id: str, token: str):
    url = f"https://graph.facebook.com/v24.0/{account_id}/campaigns"
    params = {
        "access_token": token,
        "fields": "id,name,daily_budget,lifetime_budget,stop_time,status",
        "limit": 50,  # Reduzido drasticamente de 500 para 50
    }
    data = fb_get(url, params, context=account_id) or {}
    return data.get("data", [])


def get_adset_budgets(account_id: str, token: str):
    url = f"https://graph.facebook.com/v24.0/{account_id}/adsets"
    params = {
        "access_token": token,
        "fields": "id,name,campaign_id,daily_budget",
        "limit": 50,  # Reduzido drasticamente de 500 para 50
    }
    data = fb_get(url, params, context=account_id) or {}
    return data.get("data", [])


def fetch_budgets_all_accounts(accounts: list, token: str):
    camp_rows, adset_rows = [], []

    def process_account(acc):
        # Delay entre contas para evitar sobrecarregar a API
        time.sleep(ACCOUNT_DELAY)
        
        try:
            logger.info("🔄 [BUDGETS] Processando campanhas da conta %s...", acc)
            campaigns = get_campaign_budgets(acc, token)
            logger.info("✅ [BUDGETS] Conta %s: %s campanhas processadas", acc, len(campaigns))
            
            logger.info("🔄 [BUDGETS] Processando adsets da conta %s...", acc)
            adsets = get_adset_budgets(acc, token)
            logger.info("✅ [BUDGETS] Conta %s: %s adsets processados", acc, len(adsets))
            
            return campaigns, adsets
        except Exception as e:
            logger.error("❌ [BUDGETS] Erro ao processar conta %s: %s. Continuando com outras contas...", acc, str(e))
            # Retornar listas vazias para não quebrar o processamento
            return [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_account, acc): acc for acc in accounts}
        for future in as_completed(futures):
            acc = futures[future]
            try:
                camps, adsets = future.result()
                camp_rows.extend(camps)
                adset_rows.extend(adsets)
                if len(camps) > 0 or len(adsets) > 0:
                    logger.info("Conta %s: %s campanhas, %s adsets",
                                acc, len(camps), len(adsets))
                else:
                    logger.warning("⚠️ [BUDGETS] Conta %s: nenhum dado processado (pode ter tido erros)", acc)
            except Exception as e:
                logger.error("❌ [BUDGETS] Erro na conta %s: %s. Continuando com outras contas...", acc, str(e))

    return pd.DataFrame(camp_rows), pd.DataFrame(adset_rows)


# ------------------------------------------------------------------------------
# PROCESSAMENTO COMPLETO
# ------------------------------------------------------------------------------
def process_all(accounts: list, token: str):
    # -- Orçamentos ------------------------------------------------------------
    df_camp, df_adset = fetch_budgets_all_accounts(accounts, token)

    if not df_camp.empty:
        df_camp["daily_budget"] = pd.to_numeric(df_camp.get("daily_budget", 0), errors="coerce")
        df_camp["daily_budget"] = df_camp["daily_budget"].fillna(0)
        df_camp["lifetime_budget"] = pd.to_numeric(df_camp.get("lifetime_budget", 0), errors="coerce")
        df_camp["lifetime_budget"] = df_camp["lifetime_budget"].fillna(0)
        df_camp.rename(
            columns={
                "id": "campaign_id",
                "daily_budget": "daily_budget_campaign",
                "lifetime_budget": "lifetime_budget_campaign",
                "stop_time": "campaign_end_time",
                "status": "campaign_status"
            },
            inplace=True,
        )
        # Garantir que campaign_end_time existe
        if "campaign_end_time" not in df_camp.columns:
            df_camp["campaign_end_time"] = None
        else:
            df_camp["campaign_end_time"] = pd.to_datetime(df_camp["campaign_end_time"], errors='coerce')

    # -- Métricas --------------------------------------------------------------
    insights_raw = fetch_insights_all_accounts(accounts, token, is_lifetime=False)
    df_insights = pd.DataFrame(insights_raw)

    # Se não há insights, criar um DataFrame vazio com o schema correto
    if df_insights.empty:
        logger.warning("Nenhum dado de insights retornado. Criando tabela vazia.")
        df_insights = pd.DataFrame({
            "account_id": pd.Series(dtype='string'),
            "account_name": pd.Series(dtype='string'),
            "campaign_id": pd.Series(dtype='string'),
            "campaign_name": pd.Series(dtype='string'),
            "date_start": pd.Series(dtype='datetime64[ns]'),
            "date_stop": pd.Series(dtype='datetime64[ns]'),
            "spend": pd.Series(dtype='float64'),
            "objective": pd.Series(dtype='string'),
            "cpc": pd.Series(dtype='float64'),
            "ctr": pd.Series(dtype='float64'),
            "frequency": pd.Series(dtype='float64'),
            "impressions": pd.Series(dtype='float64'),
            "reach": pd.Series(dtype='float64'),
            "conversions": pd.Series(dtype='float64')
        })

    tz = pytz.timezone("America/Sao_Paulo")
    df_insights["imported_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # Processar dados horários (sempre habilitado)
    if not df_insights.empty:
        logger.info("🕐 Processando dados por hora do dia...")
        expanded_rows = []
        
        for _, row in df_insights.iterrows():
            hourly_stats = row.get("hourly_stats_aggregated_by_advertiser_time_zone")
            
            # Se não há breakdown por hora, usar o registro original
            if not hourly_stats or pd.isna(hourly_stats):
                # Criar uma cópia do registro sem o campo hourly_stats
                row_dict = row.to_dict()
                row_dict.pop("hourly_stats_aggregated_by_advertiser_time_zone", None)
                row_dict["hour"] = None  # Sem informação de hora
                expanded_rows.append(row_dict)
            else:
                # Se há breakdown por hora, criar um registro para cada hora
                # O campo hourly_stats_aggregated_by_advertiser_time_zone é uma string
                # que representa o intervalo de hora (ex: "00:00-01:00")
                row_dict = row.to_dict()
                row_dict.pop("hourly_stats_aggregated_by_advertiser_time_zone", None)
                row_dict["hour"] = str(hourly_stats)  # Manter a string do intervalo
                expanded_rows.append(row_dict)
        
        if expanded_rows:
            df_insights = pd.DataFrame(expanded_rows)
            logger.info("✅ Dados expandidos por hora: %s registros", len(df_insights))
        else:
            logger.warning("⚠️ Nenhum dado horário expandido")

    # conversions → pega primeiro valor da lista
    def _first_val(lst):
        if isinstance(lst, list) and lst and isinstance(lst[0], dict):
            return float(lst[0].get("value", 0))
        return None
    if "conversions" in df_insights.columns:
        df_insights["conversions"] = df_insights["conversions"].apply(_first_val)

    # -- Merge final -----------------------------------------------------------
    if not df_camp.empty:
        # Garantir que todas as colunas existem antes do merge
        camp_cols = ["campaign_id", "daily_budget_campaign", "lifetime_budget_campaign", "campaign_status"]
        if "campaign_end_time" in df_camp.columns:
            camp_cols.append("campaign_end_time")
        
        df_final = df_insights.merge(
            df_camp[camp_cols],
            on="campaign_id",
            how="left",
        )
        
        # Garantir que campaign_end_time existe no resultado final
        if "campaign_end_time" not in df_final.columns:
            df_final["campaign_end_time"] = None
    else:
        df_final = df_insights.copy()
        # Garantir que campaign_end_time existe mesmo quando não há dados de campanha
        df_final["campaign_end_time"] = None
        df_final["daily_budget_campaign"] = 0
        df_final["lifetime_budget_campaign"] = 0
        df_final["campaign_status"] = None

    # budgets em moeda
    for col in ["daily_budget_campaign", "lifetime_budget_campaign"]:
        df_final[col] = pd.to_numeric(df_final[col], errors="coerce")
        df_final[col] = df_final[col].fillna(0)
    df_final["daily_budget"] = df_final["daily_budget_campaign"] / 100
    df_final["lifetime_budget"] = df_final["lifetime_budget_campaign"] / 100

    # Limpeza
    df_final.drop(columns=["daily_budget_campaign", "lifetime_budget_campaign"], inplace=True)

    # Garantir que o schema final corresponda ao BigQuery
    # Renomear e reorganizar colunas conforme necessário
    if "amount_spent" not in df_final.columns:
        df_final["amount_spent"] = df_final.get("spend", 0)
    
    # Garantir que todas as colunas do schema estejam presentes
    expected_columns = [
        "account_name", "account_id", "campaign_id", "campaign_name", 
        "date_start", "date_stop", "conversions", "spend", "objective", 
        "cpc", "ctr", "frequency", "impressions", "reach", "imported_at", 
        "daily_budget", "lifetime_budget", "amount_spent", "campaign_end_time", "campaign_status",
        "hour"  # Sempre incluir coluna de hora
    ]
    
    # Adicionar colunas que podem estar faltando
    for col in expected_columns:
        if col not in df_final.columns:
            if col in ["daily_budget", "lifetime_budget", "amount_spent", "spend", "conversions", "cpc", "ctr", "frequency", "impressions", "reach"]:
                df_final[col] = 0.0
            elif col in ["date_start", "date_stop", "campaign_end_time", "imported_at"]:
                df_final[col] = None
            elif col == "hour":
                df_final[col] = None  # Coluna de hora pode ser None se não houver breakdown
            else:
                df_final[col] = ""
    
    # Reorganizar colunas na ordem correta
    df_final = df_final[expected_columns]
    
    # Converter tipos de dados para corresponder ao schema
    # STRING fields
    string_fields = ["account_name", "account_id", "campaign_id", "campaign_name", "objective", "campaign_status", "hour"]
    for field in string_fields:
        if field in df_final.columns:
            df_final[field] = df_final[field].astype(str)
    
    # DATETIME fields
    datetime_fields = ["date_start", "date_stop", "campaign_end_time"]
    for field in datetime_fields:
        if field in df_final.columns:
            df_final[field] = pd.to_datetime(df_final[field], errors='coerce')
    
    # TIMESTAMP field
    if "imported_at" in df_final.columns:
        df_final["imported_at"] = pd.to_datetime(df_final["imported_at"], errors='coerce')
    
    # FLOAT fields
    float_fields = ["conversions", "spend", "cpc", "ctr", "frequency", "daily_budget", "lifetime_budget", "amount_spent"]
    for field in float_fields:
        if field in df_final.columns:
            df_final[field] = pd.to_numeric(df_final[field], errors='coerce')
            df_final[field] = df_final[field].fillna(0.0)
    
    # INTEGER fields
    integer_fields = ["impressions", "reach"]
    for field in integer_fields:
        if field in df_final.columns:
            df_final[field] = pd.to_numeric(df_final[field], errors='coerce')
            df_final[field] = df_final[field].fillna(0).astype(int)

    logger.info("Linhas finais: %s", len(df_final))
    logger.info("Colunas finais: %s", list(df_final.columns))
    return df_final


def verify_account_access(accounts: list, token: str, group_name: str):
    """Verifica o acesso às contas de anúncio e token."""
    logger.info("Verificando acesso para o grupo: %s", group_name)
    
    # Verificar se o token é válido testando uma conta
    if accounts:
        test_account = accounts[0]
        url = f"https://graph.facebook.com/v24.0/{test_account}"
        params = {"access_token": token, "fields": "id,name"}
        
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 401:
            logger.error("❌ TOKEN INVÁLIDO para o grupo %s - Erro 401: %s", group_name, resp.text)
            return False
        elif resp.status_code == 403:
            logger.error("❌ SEM PERMISSÃO para acessar contas do grupo %s - Erro 403: %s", group_name, resp.text)
            return False
        elif resp.status_code == 200:
            logger.info("✅ Token válido para o grupo: %s", group_name)
        else:
            logger.warning("⚠️ Problema desconhecido com token do grupo %s - Status: %s", group_name, resp.status_code)
    
    # Verificar acesso a cada conta
    accessible_accounts = []
    inaccessible_accounts = []
    
    for account in accounts:
        url = f"https://graph.facebook.com/v24.0/{account}"
        params = {"access_token": token, "fields": "id,name"}
        
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            accessible_accounts.append(account)
        elif resp.status_code == 403:
            inaccessible_accounts.append(account)
            logger.warning("⚠️ Sem acesso à conta %s no grupo %s", account, group_name)
        else:
            inaccessible_accounts.append(account)
            logger.warning("⚠️ Problema com conta %s no grupo %s - Status: %s", account, group_name, resp.status_code)
    
    logger.info("📊 Resumo de acesso para grupo %s:", group_name)
    logger.info("   ✅ Contas acessíveis: %s/%s", len(accessible_accounts), len(accounts))
    logger.info("   ❌ Contas inacessíveis: %s/%s", len(inaccessible_accounts), len(accounts))
    
    if inaccessible_accounts:
        logger.warning("   📋 Contas inacessíveis: %s", inaccessible_accounts)
    
    return len(accessible_accounts) > 0


def process_group(group_name: str, group_config: dict):
    """Processa um grupo específico de contas e retorna os dados sem fazer upload."""
    logger.info("Iniciando processamento do grupo: %s", group_name)
    start_time = time.time()
    
    # Verificar acesso antes de processar
    has_access = verify_account_access(group_config["accounts"], group_config["token"], group_name)
    
    if not has_access:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.error("❌ Grupo %s não pode ser processado - problemas de acesso", group_name)
        return {"group": group_name, "records": 0, "time": execution_time, "status": "access_denied", "data": None}
    
    df_final = process_all(group_config["accounts"], group_config["token"])
    end_time = time.time()
    execution_time = end_time - start_time
    
    if df_final is not None and not df_final.empty:
        logger.info("✅ Grupo %s processado com sucesso em %.2f segundos", group_name, execution_time)
        logger.info("Performance: %.2f registros/segundo", len(df_final) / execution_time)
        return {
            "group": group_name, 
            "records": len(df_final), 
            "time": execution_time, 
            "status": "success",
            "data": df_final,
            "table_id": BIGQUERY_TABLE_ID
        }
    else:
        logger.warning("⚠️ Grupo %s processado - sem dados em %.2f segundos", group_name, execution_time)
        return {
            "group": group_name, 
            "records": 0, 
            "time": execution_time, 
            "status": "no_data",
            "data": df_final,
            "table_id": BIGQUERY_TABLE_ID
        }


def consolidate_and_upload_by_table(results: list):
    """Consolida dados por tabela e faz upload consolidado."""
    logger.info("Consolidando dados por tabela...")
    
    # Agrupar resultados por table_id
    table_groups = {}
    for result in results:
        # Verificar se o resultado tem table_id antes de acessar
        if "table_id" in result and result.get("table_id"):
            table_id = result["table_id"]
            if result["status"] in ["success", "no_data"]:
                if table_id not in table_groups:
                    table_groups[table_id] = []
                table_groups[table_id].append(result)
    
    # Processar cada tabela
    upload_results = []
    for table_id, group_results in table_groups.items():
        logger.info("Processando tabela: %s", table_id)
        
        # Filtrar apenas grupos com dados
        groups_with_data = [r for r in group_results if r["status"] == "success" and r["data"] is not None and not r["data"].empty]
        
        if groups_with_data:
            # Concatenar todos os DataFrames
            dfs_to_merge = [r["data"] for r in groups_with_data]
            consolidated_df = pd.concat(dfs_to_merge, ignore_index=True)
            
            logger.info("Tabela %s: consolidando %s grupos com %s registros totais", 
                       table_id, len(groups_with_data), len(consolidated_df))
            
            # Fazer upload consolidado
            upload_to_bigquery(consolidated_df, table_id)
            
            upload_results.append({
                "table_id": table_id,
                "groups": [r["group"] for r in groups_with_data],
                "total_records": len(consolidated_df),
                "status": "success"
            })
        else:
            # Se nenhum grupo tem dados, criar tabela vazia
            logger.info("Tabela %s: nenhum grupo com dados, criando tabela vazia", table_id)
            empty_df = pd.DataFrame()
            upload_to_bigquery(empty_df, table_id)
            
            upload_results.append({
                "table_id": table_id,
                "groups": [r["group"] for r in group_results],
                "total_records": 0,
                "status": "table_cleared"
            })
    
    return upload_results


# ------------------------------------------------------------------------------
# BIGQUERY
# ------------------------------------------------------------------------------
# Carregar GOOGLE_CREDENTIALS de credentials.json
GOOGLE_CREDENTIALS = load_config_from_json(
    json_path=os.path.join(os.path.dirname(__file__), "..", "credentials.json"),
    config_name="GOOGLE_CREDENTIALS"
)

try:
    creds = service_account.Credentials.from_service_account_info(GOOGLE_CREDENTIALS)
    bq_client = bigquery.Client(credentials=creds)
    logger.info("BigQuery client configurado com sucesso!")
except Exception as e:
    logger.error("Erro ao configurar BigQuery client: %s", str(e))
    bq_client = None


def upload_to_bigquery(df: pd.DataFrame, table_id: str):

    if df is None or bq_client is None:
        logger.error("DataFrame nulo ou BigQuery não configurado.")
        return
    
    # Definir schema explícito para o BigQuery SEMPRE
    # Isso garante que os tipos sejam consistentes em todas as tabelas
    schema = [
        bigquery.SchemaField("account_name", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("date_start", "DATETIME"),
        bigquery.SchemaField("date_stop", "DATETIME"),
        bigquery.SchemaField("conversions", "FLOAT"),
        bigquery.SchemaField("spend", "FLOAT"),
        bigquery.SchemaField("objective", "STRING"),
        bigquery.SchemaField("cpc", "FLOAT"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("frequency", "FLOAT"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("reach", "INTEGER"),
        bigquery.SchemaField("imported_at", "DATETIME"),
        bigquery.SchemaField("daily_budget", "FLOAT"),
        bigquery.SchemaField("lifetime_budget", "FLOAT"),
        bigquery.SchemaField("amount_spent", "FLOAT"),
        bigquery.SchemaField("campaign_end_time", "TIMESTAMP"),
        bigquery.SchemaField("campaign_status", "STRING"),
        bigquery.SchemaField("hour", "STRING")  # Sempre incluir coluna de hora
    ]
    
    # Aceitar DataFrames vazios para zerar a tabela
    if df.empty:
        logger.info("DataFrame vazio - zerando tabela %s", table_id)
        # Criar um DataFrame com schema explícito para garantir tipos corretos
        schema_df = pd.DataFrame({
            "account_name": [""],
            "account_id": [""],
            "campaign_id": [""],
            "campaign_name": [""],
            "date_start": [pd.Timestamp.now()],
            "date_stop": [pd.Timestamp.now()],
            "conversions": [0.0],
            "spend": [0.0],
            "objective": [""],
            "cpc": [0.0],
            "ctr": [0.0],
            "frequency": [0.0],
            "impressions": [0],
            "reach": [0],
            "imported_at": [pd.Timestamp.now()],
            "daily_budget": [0.0],
            "lifetime_budget": [0.0],
            "amount_spent": [0.0],
            "campaign_end_time": [pd.Timestamp.now()],
            "campaign_status": [""],
            "hour": [None]  # Sempre incluir coluna de hora
        })
        # Remover a linha de dados, mantendo apenas o schema
        df = schema_df.iloc[0:0]
    
    # Usar schema explícito SEMPRE para garantir consistência
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema
    )
    
    try:
        logger.info("Enviando %s registros para %s...", len(df), table_id)
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
        job.result()
        logger.info("Adicionados %s registros para %s", job.output_rows, table_id)
    except Exception as e:
        logger.error("Erro ao adicionar dados ao BigQuery: %s", str(e))
        raise


def upload_execution_metadata(results: list, execution_time: float, script_name: str):
    """Salva metadados da execução na tabela cloud_facebook_executions."""
    if bq_client is None:
        logger.warning("BigQuery não configurado - pulando upload de metadados")
        return
    
    tz = pytz.timezone("America/Sao_Paulo")
    execution_timestamp = datetime.now(tz)
    execution_id = execution_timestamp.strftime("%Y%m%d_%H%M%S")
    
    # Calcular estatísticas
    total_groups = len(GROUPS)  # Usar len(GROUPS) para o total de grupos configurados
    successful_groups = [r for r in results if r["status"] == "success"]
    no_data_groups = [r for r in results if r["status"] == "no_data"]
    access_denied_groups = [r for r in results if r["status"] == "access_denied"]
    error_groups = [r for r in results if r["status"] == "error"]
    failed_groups = access_denied_groups + error_groups
    total_records = sum(r["records"] for r in results)
    
    # Determinar status geral
    # "success" quando não há problemas reais (access_denied ou error)
    # Grupos sem dados (no_data) são considerados normais, não são erro
    if len(failed_groups) == 0:
        status = "success"
    else:
        status = "error"
    
    # Criar resumo de erros (apenas problemas reais, não incluir no_data)
    error_summary_parts = []
    if access_denied_groups:
        error_summary_parts.append(f"Access denied: {', '.join([g['group'] for g in access_denied_groups])}")
    if error_groups:
        error_summary_parts.append(f"Errors: {', '.join([g['group'] for g in error_groups])}")
    
    error_summary = "; ".join(error_summary_parts) if error_summary_parts else None
    
    # Criar DataFrame com metadados
    metadata_df = pd.DataFrame([{
        "execution_id": execution_id,
        "execution_timestamp": execution_timestamp,
        "script_name": script_name,
        "total_groups": total_groups,
        "successful_groups": len(successful_groups),
        "failed_groups": len(failed_groups),
        "no_data_groups": len(no_data_groups),
        "total_records": total_records,
        "execution_time_seconds": execution_time,
        "status": status,
        "error_summary": error_summary
    }])
    
    # Schema da tabela de execuções
    schema = [
        bigquery.SchemaField("execution_id", "STRING"),
        bigquery.SchemaField("execution_timestamp", "DATETIME"),
        bigquery.SchemaField("script_name", "STRING"),
        bigquery.SchemaField("total_groups", "INTEGER"),
        bigquery.SchemaField("successful_groups", "INTEGER"),
        bigquery.SchemaField("failed_groups", "INTEGER"),
        bigquery.SchemaField("no_data_groups", "INTEGER"),
        bigquery.SchemaField("total_records", "INTEGER"),
        bigquery.SchemaField("execution_time_seconds", "FLOAT"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("error_summary", "STRING")
    ]
    
    # Configurar job para APPEND (acumular histórico)
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema
    )
    
    executions_table_id = "data-v1-423414.test.cloud_facebook_executions"
    
    try:
        logger.info("Salvando metadados de execução em %s...", executions_table_id)
        job = bq_client.load_table_from_dataframe(metadata_df, executions_table_id, job_config=job_cfg)
        job.result()
        logger.info("✅ Metadados de execução salvos com sucesso")
    except Exception as e:
        logger.error("Erro ao salvar metadados de execução: %s", str(e))
        # Não fazer raise para não interromper o fluxo principal


# ------------------------------------------------------------------------------
# ENTRYPOINT – Cloud Function
# ------------------------------------------------------------------------------
def execute_notebook(event, context):
    import base64
    msg = base64.b64decode(event["data"]).decode("utf-8")
    logger.info("Mensagem recebida: %s", msg)

    # Processar todos os grupos em paralelo
    results = []
    with ThreadPoolExecutor(max_workers=len(GROUPS)) as executor:
        futures = {executor.submit(process_group, group_name, group_config): group_name 
                  for group_name, group_config in GROUPS.items()}
        
        for future in as_completed(futures):
            group_name = futures[future]
            try:
                result = future.result()
                results.append(result)
                logger.info("Grupo %s processado: %s registros", result["group"], result["records"])
            except Exception as e:
                logger.error("Erro no grupo %s: %s", group_name, str(e))
                results.append({"group": group_name, "records": 0, "time": 0, "status": "error", "table_id": BIGQUERY_TABLE_ID})

    # Consolidar e fazer upload por tabela
    logger.info("Iniciando consolidação e upload por tabela...")
    upload_results = consolidate_and_upload_by_table(results)
    
    # Calcular tempo de execução e salvar metadados
    execution_time = sum(r.get("time", 0) for r in results)
    upload_execution_metadata(results, execution_time, "cloud_facebook_today_complete")
    
    logger.info("Todos os grupos processados e consolidados por tabela.")
    return "Execução concluída."


# ------------------------------------------------------------------------------
# EXECUÇÃO LOCAL
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()
    logger.info("Iniciando execução local do script (DADOS DE HOJE - COM DADOS POR HORA)...")
    logger.info("Configuração: MAX_WORKERS=%s, REQUEST_DELAY=%s, ACCOUNT_DELAY=%s", 
                MAX_WORKERS, REQUEST_DELAY, ACCOUNT_DELAY)
    
    # Processar todos os grupos em paralelo
    results = []
    with ThreadPoolExecutor(max_workers=len(GROUPS)) as executor:
        futures = {executor.submit(process_group, group_name, group_config): group_name 
                  for group_name, group_config in GROUPS.items()}
        
        for future in as_completed(futures):
            group_name = futures[future]
            try:
                result = future.result()
                results.append(result)
                status_emoji = "✅" if result["status"] == "success" else "⚠️" if result["status"] == "no_data" else "❌"
                logger.info("%s Grupo %s processado: %s registros em %.2f segundos (Status: %s)", 
                           status_emoji, result["group"], result["records"], result["time"], result["status"])
            except Exception as e:
                logger.error("❌ Erro no grupo %s: %s", group_name, str(e))
                results.append({"group": group_name, "records": 0, "time": 0, "status": "error", "table_id": BIGQUERY_TABLE_ID})

    # Consolidar e fazer upload por tabela
    logger.info("Iniciando consolidação e upload por tabela...")
    upload_results = consolidate_and_upload_by_table(results)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Resumo final detalhado
    logger.info("=" * 80)
    logger.info("📊 RESUMO FINAL DA EXECUÇÃO (DADOS DE HOJE)")
    logger.info("=" * 80)
    
    total_records = sum(r["records"] for r in results)
    successful_groups = [r for r in results if r["status"] == "success"]
    no_data_groups = [r for r in results if r["status"] == "no_data"]
    access_denied_groups = [r for r in results if r["status"] == "access_denied"]
    error_groups = [r for r in results if r["status"] == "error"]
    
    logger.info("⏱️ Tempo total de execução: %.2f segundos", execution_time)
    logger.info("📈 Performance geral: %.2f registros/segundo", total_records / execution_time if execution_time > 0 else 0)
    logger.info("📊 Total de registros processados: %s", total_records)
    logger.info("")
    logger.info("📋 Status por grupo:")
    logger.info("   ✅ Grupos com sucesso: %s/%s", len(successful_groups), len(GROUPS))
    logger.info("   ⚠️ Grupos sem dados: %s/%s", len(no_data_groups), len(GROUPS))
    logger.info("   ❌ Grupos com problemas de acesso: %s/%s", len(access_denied_groups), len(GROUPS))
    logger.info("   💥 Grupos com erro: %s/%s", len(error_groups), len(GROUPS))
    
    if successful_groups:
        logger.info("")
        logger.info("✅ Grupos processados com sucesso:")
        for group in successful_groups:
            logger.info("   • %s: %s registros em %.2f segundos", 
                       group["group"], group["records"], group["time"])
    
    if no_data_groups:
        logger.info("")
        logger.info("⚠️ Grupos sem dados:")
        for group in no_data_groups:
            logger.info("   • %s: 0 registros em %.2f segundos", 
                       group["group"], group["time"])
    
    if access_denied_groups:
        logger.info("")
        logger.info("❌ Grupos com problemas de acesso:")
        for group in access_denied_groups:
            logger.info("   • %s: Token inválido ou sem permissão", group["group"])
    
    if error_groups:
        logger.info("")
        logger.info("💥 Grupos com erro:")
        for group in error_groups:
            logger.info("   • %s: Erro durante processamento", group["group"])
    
    logger.info("")
    logger.info("📊 RESUMO DE UPLOAD POR TABELA:")
    for upload_result in upload_results:
        if upload_result["status"] == "success":
            logger.info("   ✅ %s: %s registros de %s grupos", 
                       upload_result["table_id"], upload_result["total_records"], len(upload_result["groups"]))
        else:
            logger.info("   🗑️ %s: tabela zerada (%s grupos)", 
                       upload_result["table_id"], len(upload_result["groups"]))
    
    # Salvar metadados de execução
    upload_execution_metadata(results, execution_time, "cloud_facebook_today_utc")
    
    logger.info("=" * 80) 