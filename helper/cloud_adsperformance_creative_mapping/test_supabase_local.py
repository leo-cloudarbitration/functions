#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Teste LOCAL de conexão Supabase
Execute: python test_supabase_local.py
"""

import os
import sys

# ============================================================================
# 🔐 CONFIGURE SUAS CREDENCIAIS AQUI (apenas para teste local)
# ============================================================================
SUPABASE_URL = "https://gcqhdzafdqtjxqvrqpqu.supabase.co"
SUPABASE_KEY = "sb_secret_Q_aUwe0j1iDXF_k2nB0Nuw_optU7sk3"  # Cole sua chave aqui
SUPABASE_TABLE = "adsperfomance_creative_mapping"
# ============================================================================

def test_connection():
    """Testa conexão básica com Supabase."""
    print("🚀 Teste de Conexão Supabase")
    print("=" * 60)
    
    # Validar configurações
    print(f"\n📋 Configurações:")
    print(f"  URL: {SUPABASE_URL}")
    print(f"  Key: {SUPABASE_KEY[:20]}... (len={len(SUPABASE_KEY)})")
    print(f"  Table: {SUPABASE_TABLE}")
    
    # Verificar se a biblioteca está instalada
    try:
        from supabase import create_client, Client
        print("\n✅ Biblioteca 'supabase' instalada")
    except ImportError:
        print("\n❌ Biblioteca 'supabase' NÃO instalada!")
        print("   Instale com: pip install supabase")
        sys.exit(1)
    
    # Tentar conectar
    try:
        print(f"\n🔍 Tentando conectar ao Supabase...")
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Conexão estabelecida!")
        
        # Tentar buscar dados
        print(f"\n🔍 Buscando dados da tabela '{SUPABASE_TABLE}'...")
        response = supabase.table(SUPABASE_TABLE).select("*").limit(3).execute()
        
        if not response.data:
            print("⚠️ Tabela vazia ou não encontrada")
            print("\n💡 Verifique:")
            print("  1. Se a tabela existe no Supabase")
            print("  2. Se o nome está correto")
            print("  3. Se a chave tem permissões")
        else:
            print(f"✅ {len(response.data)} registros encontrados!")
            print("\n📊 Primeiro registro:")
            print("-" * 60)
            for key, value in response.data[0].items():
                print(f"  {key:25s} = {value}")
            print("-" * 60)
            
            print("\n📋 Colunas disponíveis:")
            print("  " + ", ".join(response.data[0].keys()))
            
        print("\n🎉 Teste concluído com sucesso!")
        
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        print(f"   Tipo: {type(e).__name__}")
        
        if "Invalid API key" in str(e):
            print("\n💡 Possíveis causas:")
            print("  1. A chave está incorreta")
            print("  2. A chave foi revogada no Supabase")
            print("  3. Você está usando 'anon' key ao invés de 'service_role'")
            print("\n🔧 Solução:")
            print("  1. Vá para: Settings → API no Supabase")
            print("  2. Copie a chave 'service_role' (Secret keys)")
            print("  3. Cole neste arquivo e teste novamente")
        
        sys.exit(1)

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Configure SUPABASE_URL e SUPABASE_KEY no arquivo!")
        sys.exit(1)
    
    test_connection()

