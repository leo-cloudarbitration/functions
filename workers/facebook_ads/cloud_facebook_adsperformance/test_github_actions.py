#!/usr/bin/env python3
"""
Script de teste simples para GitHub Actions
Testa apenas a conexão com BigQuery sem fazer requisições ao Facebook
"""

import os
import logging
from datetime import datetime
import pandas as pd

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bigquery_connection():
    """Testa a conexão com BigQuery"""
    try:
        from google.cloud import bigquery
        
        # Verificar se estamos no GitHub Actions
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            logger.info("🔧 Usando credenciais do GitHub Actions")
            client = bigquery.Client()
        else:
            logger.info("🔧 Usando Application Default Credentials")
            client = bigquery.Client()
        
        # Testar conexão fazendo uma query simples
        query = "SELECT 1 as test_value"
        result = client.query(query).result()
        
        for row in result:
            logger.info(f"✅ Conexão BigQuery OK! Teste retornou: {row.test_value}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Erro na conexão BigQuery: {e}")
        return False

def test_environment():
    """Testa variáveis de ambiente"""
    logger.info("🔍 Verificando ambiente...")
    
    # Verificar se estamos no GitHub Actions
    if os.getenv("GITHUB_ACTIONS"):
        logger.info("✅ Executando no GitHub Actions")
        logger.info(f"   - Runner: {os.getenv('RUNNER_OS')}")
        logger.info(f"   - Python: {os.getenv('RUNNER_TOOL_CACHE')}")
    else:
        logger.info("✅ Executando localmente")
    
    # Verificar credenciais
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.info(f"✅ GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
    else:
        logger.info("⚠️ GOOGLE_APPLICATION_CREDENTIALS não definido")

def main():
    """Função principal de teste"""
    logger.info("🚀 Iniciando teste de conexão GitHub Actions")
    logger.info(f"⏰ Timestamp: {datetime.now()}")
    
    # Testar ambiente
    test_environment()
    
    # Testar BigQuery
    if test_bigquery_connection():
        logger.info("🎉 Todos os testes passaram! GitHub Actions está pronto!")
        return True
    else:
        logger.error("💥 Teste falhou!")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
