#!/usr/bin/env python3
"""
Script de debug para identificar problemas no BigQuery
"""

import os
import logging
from datetime import datetime
import pandas as pd

# Configurar logging detalhado
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_bigquery():
    """Debug completo da conexão BigQuery"""
    logger.info("🔍 Iniciando debug do BigQuery...")
    
    # 1. Verificar ambiente
    logger.info("=" * 50)
    logger.info("1. VERIFICANDO AMBIENTE")
    logger.info("=" * 50)
    
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.info(f"✅ GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
        
        # Verificar se o arquivo existe
        cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if os.path.exists(cred_file):
            logger.info(f"✅ Arquivo de credenciais existe: {cred_file}")
            logger.info(f"   Tamanho: {os.path.getsize(cred_file)} bytes")
        else:
            logger.error(f"❌ Arquivo de credenciais NÃO existe: {cred_file}")
            return False
    else:
        logger.warning("⚠️ GOOGLE_APPLICATION_CREDENTIALS não definido")
    
    # 2. Testar importação
    logger.info("=" * 50)
    logger.info("2. TESTANDO IMPORTAÇÕES")
    logger.info("=" * 50)
    
    try:
        from google.cloud import bigquery
        logger.info("✅ google.cloud.bigquery importado com sucesso")
    except Exception as e:
        logger.error(f"❌ Erro ao importar bigquery: {e}")
        return False
    
    # 3. Testar conexão
    logger.info("=" * 50)
    logger.info("3. TESTANDO CONEXÃO")
    logger.info("=" * 50)
    
    try:
        client = bigquery.Client()
        logger.info("✅ Cliente BigQuery criado com sucesso")
        
        # Testar query simples
        query = "SELECT 1 as test_value"
        result = client.query(query).result()
        
        for row in result:
            logger.info(f"✅ Query de teste funcionou: {row.test_value}")
            
    except Exception as e:
        logger.error(f"❌ Erro na conexão BigQuery: {e}")
        return False
    
    # 4. Testar tabela específica
    logger.info("=" * 50)
    logger.info("4. TESTANDO TABELA ESPECÍFICA")
    logger.info("=" * 50)
    
    table_id = "data-v1-423414.test.cloud_facebook_adsperformance_historical"
    
    try:
        # Verificar se a tabela existe
        table = client.get_table(table_id)
        logger.info(f"✅ Tabela existe: {table_id}")
        logger.info(f"   Schema: {len(table.schema)} campos")
        logger.info(f"   Linhas: {table.num_rows}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao acessar tabela {table_id}: {e}")
        return False
    
    # 5. Testar upload de dados
    logger.info("=" * 50)
    logger.info("5. TESTANDO UPLOAD DE DADOS")
    logger.info("=" * 50)
    
    try:
        # Criar DataFrame de teste
        test_data = pd.DataFrame({
            "date_start": [datetime.now()],
            "ad_id": ["test_ad_123"],
            "campaign_id": ["test_campaign_456"],
            "campaign_name": ["Test Campaign"],
            "account_id": ["test_account_789"],
            "account_name": ["Test Account"],
            "creative_id": ["test_creative_101"],
            "impressions": [1000],
            "clicks": [50],
            "spend": [25.50],
            "ctr": [0.05],
            "cpm": [25.50],
            "ad_name": ["Test Ad"],
            "date_stop": [datetime.now()],
            "imported_at": [datetime.now()]
        })
        
        logger.info(f"✅ DataFrame de teste criado: {len(test_data)} linhas")
        
        # Definir schema
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
        
        # Configurar job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=schema
        )
        
        logger.info("✅ Schema e job config criados")
        
        # Fazer upload
        job = client.load_table_from_dataframe(test_data, table_id, job_config=job_config)
        job.result()  # Aguardar conclusão
        
        logger.info(f"✅ Upload de teste realizado com sucesso!")
        logger.info(f"   Linhas inseridas: {job.output_rows}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no upload de teste: {e}")
        return False

def main():
    """Função principal"""
    logger.info("🚀 Iniciando debug completo do BigQuery")
    logger.info(f"⏰ Timestamp: {datetime.now()}")
    
    success = debug_bigquery()
    
    if success:
        logger.info("🎉 Debug concluído com sucesso!")
    else:
        logger.error("💥 Debug falhou!")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
