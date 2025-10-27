# -*- coding: utf-8 -*-
"""
Script de DEBUG para testar conexão Supabase e estrutura dos dados
"""

import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure suas credenciais aqui para teste local
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = "adsperfomance_creative_mapping"

def test_supabase_connection():
    """Testa conexão com Supabase e mostra estrutura dos dados."""
    try:
        from supabase import create_client
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            logger.error("❌ SUPABASE_URL e SUPABASE_KEY não configurados!")
            logger.info("Configure via environment variables:")
            logger.info("  export SUPABASE_URL='sua_url'")
            logger.info("  export SUPABASE_KEY='sua_chave'")
            return
        
        logger.info(f"🔍 Conectando ao Supabase...")
        logger.info(f"   URL: {SUPABASE_URL}")
        logger.info(f"   Tabela: {SUPABASE_TABLE}")
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Conexão estabelecida!")
        
        # Buscar dados
        logger.info(f"🔍 Buscando dados da tabela '{SUPABASE_TABLE}'...")
        response = supabase.table(SUPABASE_TABLE).select("*").limit(5).execute()
        
        if not response.data:
            logger.warning("⚠️ Nenhum dado encontrado na tabela!")
            logger.info("Verifique se:")
            logger.info("  1. A tabela existe no Supabase")
            logger.info("  2. A tabela tem dados")
            logger.info("  3. As permissões estão corretas")
            return
        
        logger.info(f"✅ {len(response.data)} registros encontrados (mostrando até 5)")
        
        # Mostrar estrutura do primeiro registro
        if response.data:
            first_record = response.data[0]
            logger.info("\n📋 Estrutura do primeiro registro:")
            logger.info("=" * 50)
            for key, value in first_record.items():
                value_type = type(value).__name__
                value_preview = str(value)[:50] if value else "None"
                logger.info(f"  {key:20s} ({value_type:10s}): {value_preview}")
            logger.info("=" * 50)
            
            # Mostrar todos os registros
            logger.info("\n📊 Todos os registros (até 5):")
            for i, record in enumerate(response.data, 1):
                logger.info(f"\nRegistro {i}:")
                for key, value in record.items():
                    logger.info(f"  {key}: {value}")
        
        logger.info("\n✅ Teste concluído com sucesso!")
        logger.info("\n💡 Próximos passos:")
        logger.info("  1. Verifique se o schema no main.py corresponde às colunas acima")
        logger.info("  2. Ajuste o schema se necessário")
        logger.info("  3. Execute o workflow no GitHub Actions")
        
    except ImportError:
        logger.error("❌ Biblioteca supabase não instalada!")
        logger.info("Instale com: pip install supabase")
    except Exception as e:
        logger.error(f"❌ Erro durante o teste: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    logger.info("🚀 Iniciando teste de conexão Supabase...")
    logger.info("")
    test_supabase_connection()

