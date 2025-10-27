# -*- coding: utf-8 -*-
"""
Facebook → BigQuery (Cloud Function) - DADOS DE ANÚNCIOS DE ONTEM
─────────────────────────────────────────────────────────────────
Coleta métricas diárias por ad_id:
🚀 Editado via Cursor - Deploy automático funcionando!
a- date, ad_id, campaign_id, campaign_name, account_id, account_name
- impressões, cliques, spend, ctr, cpm

Resultado final = métricas por anúncio (ad_id)
SOBRESCREVE os dados no BigQuery (WRITE_TRUNCATE)
"""

import os
import json
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# Tabela única para todos os dados de performance de anúncios
TABLE_ID = "data-v1-423414.test.cloud_facebook_adsperformance_historical"

# CONFIGURAÇÃO DOS GRUPOS
GROUPS = {
    "casf_a": {
        "token": "EAASZBzOX0YzUBPYC8f9HyAqsKtEHYPmxaw9LHSnoZAZAGQdhhsFrMlYzZBpCuUMTFVSGv102y1KNU8zTB6SCCHCD8FPbWc9X4XZCC2SSX5rljMZASgXCbd6Od3vwRec4Op5poHpmUoGWZARIbQt7ZBUYPnDeonfMt6EnxlrszboZCz1RjkNTUwI9xuVIHYbs87Ml9ijU4qjMQ",
        "accounts": [
            "act_2086793801756487",  # A 001
            "act_981724707176063",   # A 002
            "act_1336438444153855",  # A 004
            "act_1391292178903463",  # A 006
            "act_1299109444484986",  # A 007
            "act_1031835115731749",  # CASF A - 021
            "act_1452046959544076",  # CASF A - 022
            ]
    },       

    "casf_a2": {
        "token": "EAASZBzOX0YzUBP5sBgBYlA9vhVnZBO1W3oQjAiLQlMxRUqto6zjZBtvNbrO12PUndFqi5GDR0Sr8xyoG1yLzwbN1dGA7mrL5CO3DhfJOb527bi1NcDWwSRGFPF1um4mre0XB5wa2zXiISSV8vkrda93h3ZA21TO4WoybnAAuLXmZBGYoazfGYW0hsjtJ8PFzhgAZDZD",
        "accounts": [
            "act_4004026033209415",  # A 003
            "act_517255721392274",   # A 005
            "act_671403681969823",   # A 008
            "act_1651254315530600",  # A 009
            "act_1673050973599968",  # A 010
            "act_1263612182017570",  # CASF A - 023
            "act_1474738367239405",  # CASF A - 024
            "act_1412982356688062",  # CASF A - 025
            ]
    }, 

    "casf_b": {
        "token": "EAASZBzOX0YzUBPZAXfpZBXtYACx0WIAYnxo0zEUYWnygpDGKI8XkPwcNjLZBcZATCm6DHcKADc8oKkZACsIpKRuQXWCjw6WAN5kDdKACjM0QchkYdHPQBSLfOepuwNW4pQOFGptrVZAY7dLIUuiBFdf0dG6hp5ZAL4UzmyJZASEjsTNZBq2tItZAwH7ngaYFTAJOzuZACbf7n1gZC",
        "accounts": [
            "act_1346308366549889",  # B 001
            "act_1370098907466943",  # B 003
            "act_606104969244156",   # B 004
            "act_1068778325288092",  # B 005
            "act_625690570258598",   # B 007
            "act_1200321581769557",  # B 009
            "act_1337378787572363",  # B 010
            "act_974681131291021",   # CASF B - 011
            "act_1378638580101069",  # CASF B - 012
            "act_1761670824725907",  # CASF B - 013
            "act_1772051913713580",  # CASF B - 014
            "act_1072725700887714",  # CASF B - 015
            "act_1471023010727298",  # CASF B - 016
            "act_695055153401445",   # CASF B - 017
            "act_1057740166376634",  # CASF B - 018
            "act_1455601052101135",  # CASF B - 019
            "act_622738277534907",   # CASF B - 020
            "act_574063968902469",   # CASF B - 021
            "act_1394320888505693",  # CASF B - 022
            "act_1433314247709835",  # CASF B - 023
            "act_2288442174883294",  # CASF B - 024
        ]
    },        

    "casf_b_2": {
        "token": "EAASZBzOX0YzUBPxTYvJSVzlZC9EtZBGX1qxhTasbsHzaRxG0HtEG1l0Op0hMXZBjx2OBbFCkcY1uD7UJIbNfBlukDx1DO3C0otwD2aQt8f2zg0HZAfSSxLPYDCL0TG1YUckSuOWvYNhbf82KdIc38EXV0LV6Ot5bpZByVvWhY9ZBhSJsFBKDyrLx2PH2LJmqipI3FKsjTil",
        "accounts": [
            "act_1346308366549889",  # B 001
            "act_955177216218047",  # B 002
            "act_517874211015842",   # B 006
            "act_1797415574539841",  # CASF B - 025
            "act_717862000867606",   # CASF B - 026
            "act_770019508926452",   # CASF B - 027
            "act_796006246705409",   # CASF B - 028
            "act_992601502899291",   # CASF B - 029
            "act_1312677630226374",  # CASF B - 030
            "act_1259041578841388",  # CASF B - 031
            "act_1121606749849488",  # CASF B - 032
            "act_1915342352364684",  # CASF B - 033
            "act_3192500777579255",  # CASF B - 034
        ]
    },      

    "casf_c": {
        "token": "EAASZBzOX0YzUBPRWInZAaZAZAOrSa7A8fYzDIeP6eZCieGMNoeoyhXdV7dF5R0W8YHNcYxEcXZCO3WiOf1kTMImk43JUZA8yCCxSkJPvMtouK2d8YdXYhpZALpoAxgHBfuulFrIDCaG5l11FgZBdUjhBzZCiXVsrI1PYeJaB94BVk9uX1iKpbcZAJAK0b2SHgXoxX2qmAgTNBft",
        "accounts": [
            "act_1032538255591656",  # C 006
            "act_4151573991833128",  # CASF C - 018
            "act_1066574724914885",  # CASF C - 020
            "act_1392970961753541",  # CASF C - 021
            "act_1150534793775622",  # CASF C - 022
            "act_2653324861542109",  # CASF C - 023
            "act_1892401481539404",  # CASF C - 024
            "act_10026762610765790", # CASF C - 025
            "act_1271027548048483",  # CASF C - 026
            "act_4338176139795210",  # CASF C - 028
            "act_1454996898848132",  # CASF C - 029
            "act_709652395012874",   # CASF C - 030
            "act_3012814918876991",  # CASF C - 031
            "act_705327572107397",   # CASF C - 032
            "act_1030061175965269",  # CASF C - 042
            "act_1235931794248240",  # CASF C - 043
            "act_1018444543789280",  # CASF C - 044
            "act_1109560931044202",  # CASF C - 045
        ]
    },

    "casf_c_2": {
        "token": "EAASZBzOX0YzUBPXZChB2TzcnXAodDwhLzZBAs1WyLkgUZBjZBQRGortVGngigowZAmrR6FbvCeo258D3W1HihYzEMPN5F5335doPhA2T3njzG6CI67BmwLqf1Xm15LZBPrwKGZAe6xrL7ut18NZBLt3AcED46KwRhoo0KhGuH2xvFAzlfn3biXZBrPZBRQcmAMxPQlZCdFTjruBF",
        "accounts": [
            "act_1584150448951619",  # C 007
            "act_1749850525573536",  # C 008
            "act_681898871185195",   # C 009
            "act_536441699129952",   # C 010
            "act_1953684948703370",  # C 011
            "act_1664780364223146",  # C 012
            "act_3086929154792389",  # C 013
            "act_1054819642836727",  # C 014
            "act_720259200649418",   # C 015
            "act_752173674044463",   # C 016
            "act_3086929154792389",  # CASF C - 013
            "act_1054819642836727",  # CASF C - 014
            "act_720259200649418",   # CASF C - 015
            "act_923457749913500",   # CASF C - 017
            "act_3039326342887144",  # CASF C - 033
            "act_2988355044704974",  # CASF C - 034
            "act_2199444827150117",  # CASF C - 035
            "act_947568777447505",   # CASF C - 036
            "act_740142858498998",   # CASF C - 037
        ]
    },

     "casf_c_3": {
        "token": "EAASZBzOX0YzUBP71gRyC7bWDNCpps0eN0TXFAKNZCZB3z8TW7TzXLc1ZBhBSqpk4Ry6pjb58XPkBMU48nXUcZA8FHgHfm8q5ZCXKIi4ZA6xiZAcreHesRct13ndClM3ucvFyNtnDenkuxFLykEVkfsRtmg2bL7nSzG4DHGNa3jVpCovnBIvzj4DIg4Ihk371CmZAyZCeZClSkrw",
        "accounts": [
            "act_3938176226452279",  # C 001
            "act_3983890891844511",  # C 002
            "act_1080376004101758",  # C 003
            "act_1733528333951100",  # CASF C - 038
            "act_1464456568314306",  # CASF C - 039
            "act_1724766664815078",  # CASF C - 040
            "act_1045985000997960",  # CASF C - 041
        ]
    },

    "cloudarbitration": {
        "token": "EAALBhI66tYsBO7sZCaopQMDlbWZCBjK42JuNUdvrVQZAmdZBZBLA7I6Eh364Tjo2S7uBbn6sn0zZAsyXCD0b3iDUf4ki9CZAL0ZAkN2ZBeVabl7gP5tTLYNWSUCJNBgIEriRS1ZA2MhjHhZCsYHo3vFKKIQdiqxdHZBzwOvxI4XW039iLBhhSr7DoR2ksr1MJ9FoP09jD",
        "accounts": [
            'act_1408181320053598',#Cloud 002
            'act_2400057770189729',#Cloud 009
            'act_678596297812481', #Cloud 005
        ]
    },
     "cloudarbitration_2": {
         "token": "EAAKZBEOZANWJABPWKRZAJ1uJW4QAnPd8a35ZAGrtbu2OoMZBEns4VCvNarH3ZB37sRZCrS1xLEauH2NrlVgNZA8l8h6KlgCeAPQqSvzrHPI4DHU0SD1dZCuLBoNk1RxRithFrbND4USa84bkvMzgZA2dKnNyGWcCIX9UHwc6STVkrhgfgNc7zGyIZCf6rZB4t1XZCSIWhlErvIfBu",
         "accounts": [
            'act_901745041151181', #Cloud 003
            'act_384373767863365', #Cloud 006
            'act_341062168314061', #Cloud 007
            'act_1609550796495814', #Cloud 008
            'act_1727835544421228',#Cloud 010
         ]
     },

     "cloudarbitration_3": {
         "token":"EAAQHDrkrpocBP98Fh68zaIU8FTjGaERnCq5bxVE7ZBZBdT0AJCCG9zm6xJkDu0JXQNPDcr9LOX68WglAeXjYyP5TguEw7sXE6pHIvmvznJvVKxHuApMPhUwYRcQyHtCoWkIqTlZBBKexGXhCqPGM5cyBFR73E4bUZC4lBVNMulTolhep7Q0Aegk6Jg16zB9d4wZDZD",
         "accounts": [
            'act_336914685607657',#Cloud 004
         ],
    },

     "caag_a": {
         "token": "EAAaHkg81HaQBPAD97uZChFeLcZBUogRVjyUhqa0pXVmPfOgXW5r8O4JAb8HkwQPdz3DOEaE4oXJtWDg9ZBe4DZAbKrfZCxQv9sdUTv1zqS2JFNm63dRyZAuZByOZAZAkmd2IpBJ2afIj2JevKZAGI8AkdZBb2aMdPGiaO072K8OxVD94fl2Q0HT6zN2xhPzq7aZCYzSSHAZDZD",
         "accounts": [
             'act_618898480729472' # CAAG A 002 - TESTE CRIATIVOS DINÂMICOS
         ]
     },
          "caag_b": {
         "token": "EAAYq23wDZCIwBPiyFZA8K62w4WXO5VeATZBZCmZChOotxjRQicDrssBa0ZBzXknPOULfUcXZAZCM67ikBxs3ZBoVToYdpWWVkPQfahXKSW17dvS4ZBLq63lyLWm5dgLZCAJObRueXeo2IVbY0gwtHEuz3SV80qcvoOG5QgZBZBTvT6ZAhmZCIzI6ZC8s2pNkZChLjMZBCeHu8bxQZDZD",
         "accounts": [
             'act_756342057114932', # CAAG B 001
         ],
     }
}

# ------------------------------------------------------------------------------
# CONFIG PARA THREADS
# ------------------------------------------------------------------------------
# Número máximo de workers rodando em paralelo (threads)
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "15"))  # Aumentado para 15 para velocidade 
# Número máximo de tentativas de checagem do relatório no GAM
MAX_CHECKS = int(os.getenv("MAX_CHECKS", "8"))
# Tempo (em segundos) entre cada checagem
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "5"))
# Delay entre requisições para evitar rate limiting (em segundos)
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.2"))  # Reduzido para 0.2s para velocidade
# Delay entre contas para distribuir a carga (em segundos)
ACCOUNT_DELAY = float(os.getenv("ACCOUNT_DELAY", "0.5"))  # Reduzido para 0.5s para velocidade
# Delay extra para rate limit (em segundos)
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "30.0"))  # Mantido em 30s
# Delay entre grupos para evitar application rate limit
GROUP_DELAY = float(os.getenv("GROUP_DELAY", "3.0"))  # Reduzido para 3 segundos entre grupos

# ------------------------------------------------------------------------------
# FACEBOOK API HELPERS
# ------------------------------------------------------------------------------
# Campos para insights de anúncios
INSIGHTS_FIELDS_ADS = (
    "account_id,account_name,campaign_id,campaign_name,ad_id,ad_name,"
    "date_start,date_stop,spend,impressions,clicks,ctr,cpm"
)

# Contas problemáticas que não suportam conversions ou têm dados excessivos
PROBLEMATIC_ACCOUNTS = {
    "act_1408181320053598",  # Não suporta conversions
    "act_1727835544421228",  # Dados excessivos - Cloud 010
}

def fb_get(url: str, params: dict, retries: int = 0):
    """GET com back-off exponencial melhorado para rate limiting."""
    import requests
    
    if retries == 0:
        retries = MAX_CHECKS
    
    # Delay inicial para evitar sobrecarregar a API
    time.sleep(REQUEST_DELAY)
    
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=60)
            
            if resp.ok:
                return resp.json()
            
            # Logs específicos para diferentes tipos de erro
            if resp.status_code == 400:
                error_data = resp.json() if resp.text else {}
                error_code = error_data.get("error", {}).get("code")
                
                if error_code == 17:  # User request limit reached
                    logger.warning("🚫 Rate limit atingido (código 17) - aguardando %s segundos...", RATE_LIMIT_DELAY)
                    time.sleep(RATE_LIMIT_DELAY)
                    continue
                else:
                    logger.error("Erro 400 - Parâmetros inválidos: %s", resp.text)
                    
            elif resp.status_code == 401:
                logger.error("Erro 401 - Token inválido ou expirado: %s", resp.text)
                return None  # Não retry para token inválido
                
            elif resp.status_code == 403:
                logger.error("Erro 403 - Sem permissão para acessar esta conta: %s", resp.text)
                return None  # Não retry para permissão negada
                
            elif resp.status_code == 404:
                logger.error("Erro 404 - Recurso não encontrado: %s", resp.text)
                return None  # Não retry para recurso não encontrado
                
            elif resp.status_code == 429:
                logger.warning("Erro 429 - Rate limit atingido, aguardando...: %s", resp.text)
                time.sleep(RATE_LIMIT_DELAY)
                continue
            elif resp.status_code == 403:
                error_data = resp.json() if resp.text else {}
                error_code = error_data.get("error", {}).get("code")
                error_subcode = error_data.get("error", {}).get("error_subcode")
                
                if error_code == 4 and error_subcode == 1504022:  # Application request limit
                    logger.warning("🚫 Application rate limit atingido (código 4, subcódigo 1504022) - aguardando %s segundos...", RATE_LIMIT_DELAY)
                    time.sleep(RATE_LIMIT_DELAY)  # Aguardar tempo normal
                    continue
                else:
                    logger.error("Erro 403 - Sem permissão para acessar esta conta: %s", resp.text)
                    return None  # Não retry para permissão negada
                
            elif resp.status_code == 500:
                error_data = resp.json() if resp.text else {}
                error_msg = error_data.get("error", {}).get("message", "")
                
                if "reduce the amount of data" in error_msg.lower():
                    # Extrair o nome do grupo da URL para identificar qual grupo está com problema
                    group_name = "desconhecido"
                    account_id = "desconhecido"
                    if "/act_" in url:
                        account_id = url.split("/act_")[1].split("/")[0] if "/act_" in url else "desconhecido"
                        # Encontrar o grupo que contém esta conta
                        for g_name, g_config in GROUPS.items():
                            if account_id in g_config.get("accounts", []):
                                group_name = g_name
                                break
                    
                    # Log detalhado do erro
                    logger.error("🚨 [GRUPO: %s] DADOS EXCESSIVOS na conta %s", group_name, account_id)
                    logger.error("🚨 URL: %s", url)
                    logger.error("🚨 Parâmetros: %s", params)
                    logger.error("🚨 Tentativa: %s/%s", attempt + 1, retries)
                    
                    # Aguardar mais tempo para este erro específico
                    logger.info("[GRUPO: %s] Aguardando %s segundos antes da próxima tentativa...", group_name, RATE_LIMIT_DELAY * 2)
                    time.sleep(RATE_LIMIT_DELAY * 2)
                    continue
                else:
                    # Extrair o nome do grupo da URL para identificar qual grupo está com problema
                    group_name = "desconhecido"
                    if "/act_" in url:
                        account_id = url.split("/act_")[1].split("/")[0] if "/act_" in url else "desconhecido"
                        # Encontrar o grupo que contém esta conta
                        for g_name, g_config in GROUPS.items():
                            if account_id in g_config.get("accounts", []):
                                group_name = g_name
                                break
                    
                    logger.warning("[GRUPO: %s] Erro 500 – tentativa %s/%s: %s",
                                   group_name, attempt + 1, retries, resp.text)
            else:
                logger.warning("Erro %s – tentativa %s/%s: %s",
                               resp.status_code, attempt + 1, retries, resp.text)
            
            # Back-off exponencial com delay mínimo
            delay = max(SLEEP_SECONDS * (2 ** attempt), 5)  # Mínimo 5 segundos
            logger.info("Aguardando %s segundos antes da próxima tentativa...", delay)
            time.sleep(delay)
            
        except requests.exceptions.Timeout:
            logger.warning("Timeout na tentativa %s/%s", attempt + 1, retries)
            time.sleep(SLEEP_SECONDS * (2 ** attempt))
        except requests.exceptions.RequestException as e:
            logger.warning("Erro de conexão na tentativa %s/%s: %s", attempt + 1, retries, str(e))
            time.sleep(SLEEP_SECONDS * (2 ** attempt))
    
    logger.error("❌ Todas as tentativas falharam para: %s", url)
    return None

# ---------- INSIGHTS DE ANÚNCIOS ----------------------------------------------------------
def get_ads_insights_page(account_id: str, token: str, after: str | None = None, use_smaller_limit: bool = False):
    import pytz
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    url = f"https://graph.facebook.com/v22.0/{account_id}/insights"
    
    params = {
        "access_token": token,
        "fields": INSIGHTS_FIELDS_ADS,
        "time_increment": "1",
        "date_preset": "yesterday",  # Dados de ontem
        "level": "ad",  # Nível de anúncio
        "limit": 25 if use_smaller_limit else 50,
    }
    if after:
        params["after"] = after
    return fb_get(url, params)

def fetch_ads_insights_all_accounts(accounts: list, token: str):
    rows = []

    def process_account(acc):
        acc_rows = []
        after = None
        
        # Delay entre contas para evitar sobrecarregar a API
        time.sleep(ACCOUNT_DELAY)
        logger.info("🔄 [ADS INSIGHTS] Processando insights de anúncios da conta %s...", acc)
        
        # Começar com limite pequeno para evitar dados excessivos
        use_smaller_limit = True
        while True:
            data = get_ads_insights_page(acc, token, after, use_smaller_limit=use_smaller_limit)
            if not data:
                break
            acc_rows.extend(data.get("data", []))
            after = data.get("paging", {}).get("cursors", {}).get("after")
            if not after:
                break
            # Se a primeira página funcionou, continuar com limite pequeno para ser conservador
            use_smaller_limit = True
        
        return acc_rows

    # Processar contas em lotes maiores para velocidade
    batch_size = 15  # Aumentado para 15 contas por vez para velocidade
    for i in range(0, len(accounts), batch_size):
        batch = accounts[i:i + batch_size]
        logger.info("Processando lote %s/%s: %s contas", 
                   (i // batch_size) + 1, (len(accounts) + batch_size - 1) // batch_size, len(batch))
        
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
            futures = {executor.submit(process_account, acc): acc for acc in batch}
            for future in as_completed(futures):
                acc = futures[future]
                try:
                    result = future.result()
                    rows.extend(result)
                    logger.info("✅ [ADS INSIGHTS] Conta %s: %s registros de anúncios processados", acc, len(result))
                except Exception as e:
                    logger.error("Erro na conta %s: %s", acc, str(e))
        
        # Delay entre lotes para distribuir carga
        if i + batch_size < len(accounts):
            logger.info("Aguardando %s segundos antes do próximo lote...", ACCOUNT_DELAY)
            time.sleep(ACCOUNT_DELAY)

    return rows

# ------------------------------------------------------------------------------
# PROCESSAMENTO COMPLETO
# ------------------------------------------------------------------------------
def is_dynamic_creative_campaign(campaign_id: str, token: str) -> bool:
    """Detecta se uma campanha usa criativos dinâmicos verificando apenas o primeiro creative."""
    try:
        # Buscar apenas alguns anúncios da campanha para análise rápida
        url = f"https://graph.facebook.com/v22.0/{campaign_id}/ads"
        params = {"access_token": token, "fields": "id,name,creative", "limit": 5}  # Apenas 5 anúncios
        
        data = fb_get(url, params, retries=1)  # Reduzido retries
        if data and "data" in data:
            ads = data["data"]
            
            if len(ads) == 0:
                return False
            
            # Verificar apenas o primeiro creative para velocidade
            first_ad = ads[0]
            creative = first_ad.get("creative", {})
            creative_id = creative.get("id", "")
            
            if creative_id:
                is_dynamic = check_creative_for_dynamic_features(creative_id, token)
                if is_dynamic:
                    logger.info(f"🎯 Campanha {campaign_id} usa criativos dinâmicos")
                    return True
            
            return False
        
        return False
    except Exception as e:
        logger.warning(f"Erro ao verificar criativos dinâmicos para campanha {campaign_id}: {e}")
        return False

def check_creative_for_dynamic_features(creative_id: str, token: str) -> bool:
    """Verifica se um creative específico tem características de criativo dinâmico (versão otimizada)."""
    try:
        # Buscar apenas campos essenciais para velocidade
        url = f"https://graph.facebook.com/v22.0/{creative_id}"
        params = {"access_token": token, "fields": "asset_feed_spec,name"}  # Apenas campos essenciais
        
        data = fb_get(url, params, retries=1)  # Reduzido retries
        if not data:
            return False
        
        # Verificação rápida: apenas asset_feed_spec e nome
        # 1. Verificar se tem asset_feed_spec (indicador mais confiável)
        if "asset_feed_spec" in data and data["asset_feed_spec"]:
            return True
        
        # 2. Verificar nome do creative (verificação rápida)
        creative_name = data.get("name", "").lower()
        if any(keyword in creative_name for keyword in ["dynamic", "dinâmico", "auto", "template"]):
            return True
        
        return False
        
    except Exception as e:
        logger.warning(f"Erro ao verificar creative {creative_id}: {e}")
        return False

def get_creative_id_for_ad(ad_id: str, token: str, campaign_id: str = None) -> str:
    """Busca o creative_id de um ad_id específico, considerando criativos dinâmicos."""
    try:
        # Se temos campaign_id, verificar se usa criativos dinâmicos primeiro
        if campaign_id:
            if is_dynamic_creative_campaign(campaign_id, token):
                logger.info(f"🎯 Campanha {campaign_id} usa criativos dinâmicos - retornando dynamic_creative")
                return "dynamic_creative"
        
        # Se não é dinâmico ou não temos campaign_id, buscar creative_id normal
        url = f"https://graph.facebook.com/v22.0/{ad_id}"
        params = {"access_token": token, "fields": "creative"}
        
        data = fb_get(url, params, retries=2)
        if data and "creative" in data:
            creative_data = data["creative"]
            if isinstance(creative_data, dict) and "id" in creative_data:
                return creative_data["id"]
            elif isinstance(creative_data, str):
                return creative_data
        
        return ""
    except Exception as e:
        logger.warning(f"Erro ao buscar creative_id para ad_id {ad_id}: {e}")
        return ""

def process_all(accounts: list, token: str):
    import pandas as pd
    import pytz
    
    # -- Métricas de Anúncios --------------------------------------------------------------
    ads_insights_raw = fetch_ads_insights_all_accounts(accounts, token)
    df_ads_insights = pd.DataFrame(ads_insights_raw)
    
    # Buscar creative_id para cada ad_id
    if not df_ads_insights.empty and 'ad_id' in df_ads_insights.columns:
        logger.info("🔍 Buscando creative_id para %s anúncios...", len(df_ads_insights))
        
        # Buscar creative_ids em paralelo para melhor performance
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 8)) as executor:
            # Criar futures para buscar creative_id de cada ad_id com campaign_id
            future_to_ad_data = {}
            for _, row in df_ads_insights.iterrows():
                ad_id = row['ad_id']
                campaign_id = row.get('campaign_id', '')
                future = executor.submit(get_creative_id_for_ad, ad_id, token, campaign_id)
                future_to_ad_data[future] = ad_id
            
            # Criar mapeamento ad_id -> creative_id
            ad_to_creative = {}
            for future in as_completed(future_to_ad_data):
                ad_id = future_to_ad_data[future]
                try:
                    creative_id = future.result()
                    ad_to_creative[ad_id] = creative_id
                except Exception as e:
                    logger.warning(f"Erro ao buscar creative_id para {ad_id}: {e}")
                    ad_to_creative[ad_id] = ""
        
        # Aplicar o mapeamento ao DataFrame
        df_ads_insights['creative_id'] = df_ads_insights['ad_id'].map(ad_to_creative).fillna("")
        
        # Contar quantos são dinâmicos
        dynamic_count = (df_ads_insights['creative_id'] == 'dynamic_creative').sum()
        logger.info("✅ Creative_ids obtidos para %s anúncios (%s dinâmicos)", len(df_ads_insights), dynamic_count)

    # Se não há insights, criar um DataFrame vazio com o schema correto
    if df_ads_insights.empty:
        logger.warning("Nenhum dado de insights de anúncios retornado. Criando tabela vazia.")
        df_ads_insights = pd.DataFrame({
            "account_id": pd.Series(dtype='string'),
            "account_name": pd.Series(dtype='string'),
            "campaign_id": pd.Series(dtype='string'),
            "campaign_name": pd.Series(dtype='string'),
            "ad_id": pd.Series(dtype='string'),
            "ad_name": pd.Series(dtype='string'),
            "creative_id": pd.Series(dtype='string'),
            "date_start": pd.Series(dtype='datetime64[ns]'),
            "date_stop": pd.Series(dtype='datetime64[ns]'),
            "spend": pd.Series(dtype='float64'),
            "impressions": pd.Series(dtype='int64'),
            "clicks": pd.Series(dtype='int64'),
            "ctr": pd.Series(dtype='float64'),
            "cpm": pd.Series(dtype='float64')
        })

    tz = pytz.timezone("America/Sao_Paulo")
    df_ads_insights["imported_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # Garantir que todas as colunas do schema estejam presentes
    expected_columns = [
        "date_start", "ad_id", "campaign_id", "campaign_name", "account_id", "account_name", "creative_id",
        "impressions", "clicks", "spend", "ctr", "cpm", "ad_name", "date_stop", "imported_at"
    ]
    
    # Adicionar colunas que podem estar faltando
    for col in expected_columns:
        if col not in df_ads_insights.columns:
            if col in ["spend", "ctr", "cpm"]:
                df_ads_insights[col] = 0.0
            elif col in ["impressions", "clicks"]:
                df_ads_insights[col] = 0
            elif col in ["date_start", "date_stop", "imported_at"]:
                df_ads_insights[col] = None
            else:
                df_ads_insights[col] = ""
    
    # Reorganizar colunas na ordem correta
    df_ads_insights = df_ads_insights[expected_columns]
    
    # Converter tipos de dados para corresponder ao schema
    # STRING fields
    string_fields = ["ad_id", "campaign_id", "campaign_name", "account_id", "account_name", "creative_id", "ad_name"]
    for field in string_fields:
        if field in df_ads_insights.columns:
            df_ads_insights[field] = df_ads_insights[field].astype(str)
    
    # DATETIME fields
    datetime_fields = ["date_start", "date_stop"]
    for field in datetime_fields:
        if field in df_ads_insights.columns:
            df_ads_insights[field] = pd.to_datetime(df_ads_insights[field], errors='coerce')
    
    # TIMESTAMP field
    if "imported_at" in df_ads_insights.columns:
        df_ads_insights["imported_at"] = pd.to_datetime(df_ads_insights["imported_at"], errors='coerce')
    
    # FLOAT fields
    float_fields = ["spend", "ctr", "cpm"]
    for field in float_fields:
        if field in df_ads_insights.columns:
            df_ads_insights[field] = pd.to_numeric(df_ads_insights[field], errors='coerce').fillna(0.0)
    
    # INTEGER fields
    integer_fields = ["impressions", "clicks"]
    for field in integer_fields:
        if field in df_ads_insights.columns:
            df_ads_insights[field] = pd.to_numeric(df_ads_insights[field], errors='coerce').fillna(0).astype(int)

    logger.info("Linhas finais: %s", len(df_ads_insights))
    logger.info("Colunas finais: %s", list(df_ads_insights.columns))
    
    return df_ads_insights

def verify_account_access(accounts: list, token: str, group_name: str):
    """Verifica o acesso às contas de anúncio e token."""
    logger.info("Verificando acesso para o grupo: %s", group_name)
    
    # Verificar se o token é válido testando uma conta
    if accounts:
        test_account = accounts[0]
        url = f"https://graph.facebook.com/v22.0/{test_account}"
        params = {"access_token": token, "fields": "id,name"}
        
        # Usar fb_get para ter retry automático
        data = fb_get(url, params, retries=3)
        if data is None:
            logger.error("❌ TOKEN INVÁLIDO ou SEM PERMISSÃO para o grupo %s", group_name)
            return False
        else:
            logger.info("✅ Token válido para o grupo: %s", group_name)
    
    # Verificar acesso a cada conta (limitado a 5 para não sobrecarregar)
    accessible_accounts = []
    inaccessible_accounts = []
    
    # Verificar apenas as primeiras 5 contas para economizar API calls
    accounts_to_check = accounts[:5]
    
    for account in accounts_to_check:
        url = f"https://graph.facebook.com/v22.0/{account}"
        params = {"access_token": token, "fields": "id,name"}
        
        data = fb_get(url, params, retries=2)
        if data is not None:
            accessible_accounts.append(account)
        else:
            inaccessible_accounts.append(account)
            logger.warning("⚠️ Problema com conta %s no grupo %s", account, group_name)
    
    # Assumir que as outras contas são acessíveis se pelo menos uma foi verificada
    if accessible_accounts:
        remaining_accounts = [acc for acc in accounts if acc not in accounts_to_check]
        accessible_accounts.extend(remaining_accounts)
    
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
    
    # Verificar se é um grupo com múltiplos tokens
    if "tokens" in group_config:
        logger.info("🔄 Grupo %s usa múltiplos tokens - dividindo contas...", group_name)
        return process_group_with_multiple_tokens(group_name, group_config)
    
    # Processamento normal com um token
    token = group_config["token"]
    
    # Verificar acesso antes de processar
    has_access = verify_account_access(group_config["accounts"], token, group_name)
    
    if not has_access:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.error("❌ Grupo %s não pode ser processado - problemas de acesso", group_name)
        return {"group": group_name, "records": 0, "time": execution_time, "status": "access_denied", "data": None}
    
    df_final = process_all(group_config["accounts"], token)
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
            "table_id": TABLE_ID
        }
    else:
        logger.warning("⚠️ Grupo %s processado - sem dados em %.2f segundos", group_name, execution_time)
        return {
            "group": group_name, 
            "records": 0, 
            "time": execution_time, 
            "status": "no_data",
            "data": df_final,
            "table_id": TABLE_ID
        }

def process_group_with_multiple_tokens(group_name: str, group_config: dict):
    """Processa um grupo dividindo as contas entre múltiplos tokens."""
    tokens = group_config["tokens"]
    accounts = group_config["accounts"]
    
    # Dividir contas entre tokens
    accounts_per_token = len(accounts) // len(tokens)
    remainder = len(accounts) % len(tokens)
    
    logger.info("📊 Dividindo %s contas entre %s tokens:", len(accounts), len(tokens))
    
    all_dataframes = []
    total_records = 0
    total_time = 0
    
    start_idx = 0
    for i, token in enumerate(tokens):
        # Calcular quantas contas este token vai processar
        if i < remainder:
            num_accounts = accounts_per_token + 1
        else:
            num_accounts = accounts_per_token
        
        end_idx = start_idx + num_accounts
        token_accounts = accounts[start_idx:end_idx]
        
        logger.info("   Token %s: %s contas (%s a %s)", i + 1, len(token_accounts), start_idx + 1, end_idx)
        
        # Verificar acesso para este token
        has_access = verify_account_access(token_accounts, token, f"{group_name}_token_{i+1}")
        
        if not has_access:
            logger.warning("⚠️ Token %s do grupo %s não tem acesso - pulando", i + 1, group_name)
            start_idx = end_idx
            continue
        
        # Processar contas deste token
        token_start_time = time.time()
        df_token = process_all(token_accounts, token)
        token_end_time = time.time()
        token_execution_time = token_end_time - token_start_time
        
        if df_token is not None and not df_token.empty:
            all_dataframes.append(df_token)
            total_records += len(df_token)
            logger.info("   ✅ Token %s: %s registros em %.2f segundos", i + 1, len(df_token), token_execution_time)
        else:
            logger.info("   ⚠️ Token %s: 0 registros em %.2f segundos", i + 1, token_execution_time)
        
        total_time += token_execution_time
        start_idx = end_idx
    
    # Combinar todos os DataFrames
    if all_dataframes:
        import pandas as pd
        df_final = pd.concat(all_dataframes, ignore_index=True)
        logger.info("✅ Grupo %s processado com sucesso em %.2f segundos", group_name, total_time)
        logger.info("Performance: %.2f registros/segundo", total_records / total_time if total_time > 0 else 0)
        return {
            "group": group_name, 
            "records": total_records, 
            "time": total_time, 
            "status": "success",
            "data": df_final,
            "table_id": TABLE_ID
        }
    else:
        logger.warning("⚠️ Grupo %s processado - sem dados em %.2f segundos", group_name, total_time)
        return {
            "group": group_name, 
            "records": 0, 
            "time": total_time, 
            "status": "no_data",
            "data": pd.DataFrame(),
            "table_id": TABLE_ID
        }

def consolidate_and_upload_by_table(results: list):
    """Consolida dados de todos os grupos e faz upload para a tabela única."""
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2 import service_account
    
    logger.info("🔍 [DEBUG] Consolidando dados para tabela única: %s", TABLE_ID)
    logger.info(f"🔍 [DEBUG] Total de resultados: {len(results)}")
    
    for i, result in enumerate(results):
        logger.info(f"🔍 [DEBUG] Resultado {i+1}: grupo={result.get('group')}, status={result.get('status')}, records={result.get('records')}")
    
    # Filtrar apenas grupos com dados
    groups_with_data = [r for r in results if r["status"] == "success" and r["data"] is not None and not r["data"].empty]
    
    if groups_with_data:
        # Concatenar todos os DataFrames
        dfs_to_merge = [r["data"] for r in groups_with_data]
        
        # Garantir que todos os DataFrames tenham os mesmos tipos de dados antes da concatenação
        for i, df in enumerate(dfs_to_merge):
            if not df.empty:
                # Converter tipos de dados para corresponder ao schema
                # STRING fields
                string_fields = ["ad_id", "campaign_id", "campaign_name", "account_id", "account_name", "creative_id", "ad_name"]
                for field in string_fields:
                    if field in df.columns:
                        df[field] = df[field].astype(str)
                
                # DATETIME fields
                datetime_fields = ["date_start", "date_stop"]
                for field in datetime_fields:
                    if field in df.columns:
                        df[field] = pd.to_datetime(df[field], errors='coerce')
                
                # TIMESTAMP field
                if "imported_at" in df.columns:
                    df["imported_at"] = pd.to_datetime(df["imported_at"], errors='coerce')
                
                # FLOAT fields
                float_fields = ["spend", "ctr", "cpm"]
                for field in float_fields:
                    if field in df.columns:
                        df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0.0)
                
                # INTEGER fields
                integer_fields = ["impressions", "clicks"]
                for field in integer_fields:
                    if field in df.columns:
                        df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).astype(int)
        
        consolidated_df = pd.concat(dfs_to_merge, ignore_index=True)
        
        logger.info("Consolidando %s grupos com %s registros totais para %s", 
                   len(groups_with_data), len(consolidated_df), TABLE_ID)
        
        # Fazer upload consolidado
        upload_to_bigquery(consolidated_df, TABLE_ID)
        
        return [{
            "table_id": TABLE_ID,
            "groups": [r["group"] for r in groups_with_data],
            "total_records": len(consolidated_df),
            "status": "success"
        }]
    else:
        # Se nenhum grupo tem dados, criar tabela vazia
        logger.info("Nenhum grupo com dados, criando tabela vazia: %s", TABLE_ID)
        empty_df = pd.DataFrame()
        upload_to_bigquery(empty_df, TABLE_ID)
        
        return [{
            "table_id": TABLE_ID,
            "groups": [r["group"] for r in results],
            "total_records": 0,
            "status": "table_cleared"
        }]

# ------------------------------------------------------------------------------
# BIGQUERY
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
                # GitHub Actions: usar arquivo de credenciais
                logger.info("Usando credenciais do GitHub Actions via arquivo JSON")
                bq_client = bigquery.Client()
            else:
                # Cloud Function: usar Application Default Credentials
                logger.info("Usando Application Default Credentials para Cloud Function")
                bq_client = bigquery.Client()
            
            logger.info("BigQuery client configurado com sucesso!")
        except Exception as e:
            logger.error("Erro ao configurar BigQuery client: %s", str(e))
            bq_client = None
    return bq_client

def upload_to_bigquery(df, table_id: str):
    from google.cloud import bigquery
    
    logger.info("🔍 [DEBUG] Iniciando upload_to_bigquery...")
    logger.info(f"🔍 [DEBUG] DataFrame: {df is not None}")
    logger.info(f"🔍 [DEBUG] DataFrame vazio: {df.empty if df is not None else 'N/A'}")
    logger.info(f"🔍 [DEBUG] Tabela: {table_id}")
    
    bq_client = get_bq_client()
    if df is None or bq_client is None:
        logger.error("❌ DataFrame nulo ou BigQuery não configurado.")
        logger.error(f"❌ df is None: {df is None}")
        logger.error(f"❌ bq_client is None: {bq_client is None}")
        return
    
    # Definir schema explícito para garantir tipos corretos sempre
    schema = [
        bigquery.SchemaField("date_start", "DATETIME"),
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("account_name", "STRING"),
        bigquery.SchemaField("creative_id", "STRING"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("spend", "FLOAT"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("cpm", "FLOAT"),
        bigquery.SchemaField("ad_name", "STRING"),
        bigquery.SchemaField("date_stop", "DATETIME"),
        bigquery.SchemaField("imported_at", "DATETIME")
    ]
    
    # Aceitar DataFrames vazios para zerar a tabela
    if df.empty:
        logger.info("DataFrame vazio - zerando tabela %s", table_id)
        # Criar um DataFrame com schema explícito para garantir tipos corretos
        import pandas as pd
        schema_df = pd.DataFrame({
            "date_start": [pd.Timestamp.now()],
            "ad_id": [""],
            "campaign_id": [""],
            "campaign_name": [""],
            "account_id": [""],
            "account_name": [""],
            "creative_id": [""],
            "impressions": [0],
            "clicks": [0],
            "spend": [0.0],
            "ctr": [0.0],
            "cpm": [0.0],
            "ad_name": [""],
            "date_stop": [pd.Timestamp.now()],
            "imported_at": [pd.Timestamp.now()]
        })
        # Remover a linha de dados, mantendo apenas o schema
        df = schema_df.iloc[0:0]
    
    # Sempre usar schema explícito para garantir tipos corretos
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # MUDANÇA: Usar APPEND em vez de TRUNCATE
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

# ------------------------------------------------------------------------------
# ENTRYPOINT – Cloud Functions 2nd Generation
# ------------------------------------------------------------------------------
import functions_framework
from flask import Request

@functions_framework.http
def facebook_ads_today(request: Request):
    """
    Cloud Function HTTP entrypoint para processar dados de anúncios do Facebook de ontem.
    
    Métodos suportados:
    - GET: Executa o processamento completo
    - POST: Executa com parâmetros opcionais via JSON body
    
    Retorna:
    - JSON com status da execução e estatísticas
    """
    try:
        logger.info("🚀 Iniciando Cloud Function - Facebook Ads Today")
        
        # Verificar método HTTP
        if request.method not in ['GET', 'POST']:
            return {"error": "Método não suportado. Use GET ou POST."}, 405
        
        # Processar parâmetros opcionais se POST
        params = {}
        if request.method == 'POST':
            try:
                params = request.get_json() or {}
            except Exception as e:
                logger.warning("Erro ao processar JSON do body: %s", str(e))
        
        # Log dos parâmetros recebidos
        if params:
            logger.info("Parâmetros recebidos: %s", params)
        
        # Processar TODOS os grupos simultaneamente para máxima eficiência
        results = []
        all_groups = list(GROUPS.items())
        
        logger.info("🚀 Processando TODOS os %s grupos simultaneamente para máxima eficiência", len(GROUPS))
        logger.info("📊 Grupos: %s", ", ".join([name for name, _ in all_groups]))
        
        # Processar todos os grupos em paralelo
        with ThreadPoolExecutor(max_workers=len(all_groups)) as executor:
            futures = {executor.submit(process_group, group_name, group_config): group_name 
                      for group_name, group_config in all_groups}
            
            for future in as_completed(futures):
                group_name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    status_emoji = "✅" if result["status"] == "success" else "⚠️" if result["status"] == "no_data" else "❌"
                    logger.info("%s Grupo %s processado: %s registros em %.2f segundos", 
                               status_emoji, result["group"], result["records"], result["time"])
                except Exception as e:
                    logger.error("❌ Erro no grupo %s: %s", group_name, str(e))
                    results.append({"group": group_name, "records": 0, "time": 0, "status": "error"})

        # Consolidar e fazer upload por tabela
        logger.info("Iniciando consolidação e upload por tabela...")
        upload_results = consolidate_and_upload_by_table(results)
        
        # Calcular estatísticas finais
        total_records = sum(r["records"] for r in results)
        successful_groups = [r for r in results if r["status"] == "success"]
        no_data_groups = [r for r in results if r["status"] == "no_data"]
        error_groups = [r for r in results if r["status"] in ["error", "access_denied"]]
        
        logger.info("✅ Execução concluída com sucesso!")
        
        # Retornar resposta JSON estruturada
        response = {
            "status": "success",
            "message": "Processamento concluído com sucesso",
            "summary": {
                "total_groups": len(GROUPS),
                "successful_groups": len(successful_groups),
                "no_data_groups": len(no_data_groups),
                "error_groups": len(error_groups),
                "total_records": total_records
            },
            "groups": results,
            "upload_results": upload_results
        }
        
        return response, 200
        
    except Exception as e:
        logger.error("❌ Erro crítico na Cloud Function: %s", str(e))
        return {
            "status": "error",
            "message": f"Erro durante execução: {str(e)}",
            "error_type": type(e).__name__
        }, 500

# Mantendo compatibilidade com Cloud Functions 1st generation (se necessário)
def execute_notebook(event, context):
    """Entrypoint para Cloud Functions 1st generation (mantido para compatibilidade)"""
    import base64
    msg = base64.b64decode(event["data"]).decode("utf-8")
    logger.info("Mensagem recebida: %s", msg)

    # Processar TODOS os grupos simultaneamente para máxima eficiência
    results = []
    all_groups = list(GROUPS.items())
    
    logger.info("🚀 Processando TODOS os %s grupos simultaneamente para máxima eficiência", len(GROUPS))
    logger.info("📊 Grupos: %s", ", ".join([name for name, _ in all_groups]))
    
    # Processar todos os grupos em paralelo
    with ThreadPoolExecutor(max_workers=len(all_groups)) as executor:
        futures = {executor.submit(process_group, group_name, group_config): group_name 
                  for group_name, group_config in all_groups}
        
        for future in as_completed(futures):
            group_name = futures[future]
            try:
                result = future.result()
                results.append(result)
                status_emoji = "✅" if result["status"] == "success" else "⚠️" if result["status"] == "no_data" else "❌"
                logger.info("%s Grupo %s processado: %s registros em %.2f segundos", 
                           status_emoji, result["group"], result["records"], result["time"])
            except Exception as e:
                logger.error("❌ Erro no grupo %s: %s", group_name, str(e))
                results.append({"group": group_name, "records": 0, "time": 0, "status": "error"})

    # Consolidar e fazer upload por tabela
    logger.info("Iniciando consolidação e upload por tabela...")
    upload_results = consolidate_and_upload_by_table(results)
    
    logger.info("Todos os grupos processados e consolidados por tabela.")
    return "Execução concluída."

# ------------------------------------------------------------------------------
# EXECUÇÃO LOCAL
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()
    logger.info("Iniciando execução local do script (DADOS DE ANÚNCIOS DE ONTEM)...")
    logger.info("Configuração: MAX_WORKERS=%s, REQUEST_DELAY=%s, ACCOUNT_DELAY=%s", 
                MAX_WORKERS, REQUEST_DELAY, ACCOUNT_DELAY)
    
    # Processar TODOS os grupos simultaneamente para máxima eficiência
    results = []
    all_groups = list(GROUPS.items())
    
    logger.info("🚀 Processando TODOS os %s grupos simultaneamente para máxima eficiência", len(GROUPS))
    logger.info("📊 Grupos: %s", ", ".join([name for name, _ in all_groups]))
    
    # Processar todos os grupos em paralelo
    with ThreadPoolExecutor(max_workers=len(all_groups)) as executor:
        futures = {executor.submit(process_group, group_name, group_config): group_name 
                  for group_name, group_config in all_groups}
        
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
                results.append({"group": group_name, "records": 0, "time": 0, "status": "error"})

    # Consolidar e fazer upload por tabela
    logger.info("Iniciando consolidação e upload por tabela...")
    upload_results = consolidate_and_upload_by_table(results)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Resumo final detalhado
    logger.info("=" * 80)
    logger.info("📊 RESUMO FINAL DA EXECUÇÃO (DADOS DE ANÚNCIOS DE ONTEM)")
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
    
    logger.info("=" * 80)