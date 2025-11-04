"""
Teste simples de credenciais Google Ads
"""
import os
import json
from google.ads.googleads.client import GoogleAdsClient

print("=" * 80)
print("🧪 TESTE DE CREDENCIAIS GOOGLE ADS")
print("=" * 80)

# Carregar configuração
ads_config_json = os.getenv("SECRET_GOOGLE_ADS_CONFIG")
if not ads_config_json:
    print("❌ SECRET_GOOGLE_ADS_CONFIG não encontrado!")
    exit(1)

google_ads_config = json.loads(ads_config_json)

print(f"✅ Configuração carregada")
print(f"   Developer Token: {google_ads_config['developer_token'][:10]}...")
print(f"   Client ID: {google_ads_config['client_id'][:20]}...")
print(f"   Login Customer ID: {google_ads_config['login_customer_id']}")
print()

# Criar cliente
try:
    client = GoogleAdsClient.load_from_dict(google_ads_config)
    print("✅ Cliente criado com sucesso")
except Exception as e:
    print(f"❌ Erro ao criar cliente: {e}")
    exit(1)

# Testar com query MUITO simples
print("\n" + "=" * 80)
print("🔍 TESTANDO QUERY SIMPLES")
print("=" * 80)

customer_id = "9679496200"
print(f"Customer ID: {customer_id}")

# Query mínima possível
query = """
    SELECT customer.id
    FROM customer
    LIMIT 1
"""

print(f"Query: {query.strip()}")
print()

try:
    ga_service = client.get_service("GoogleAdsService")
    print("✅ GoogleAdsService obtido")
    
    print("🔄 Executando query...")
    response = ga_service.search(customer_id=customer_id, query=query)
    
    count = 0
    for row in response:
        count += 1
        print(f"✅ Resposta recebida: customer.id = {row.customer.id}")
        break
    
    if count > 0:
        print(f"\n🎉 SUCESSO! Credenciais funcionando corretamente!")
    else:
        print(f"\n⚠️ Query executou mas não retornou dados")
        
except Exception as e:
    print(f"\n❌ ERRO: {e}")
    print(f"   Tipo: {type(e).__name__}")
    
    # Detalhes do erro
    if hasattr(e, 'failure'):
        print(f"   Failure: {e.failure}")
    if hasattr(e, 'request_id'):
        print(f"   Request ID: {e.request_id}")
    
    exit(1)

print("\n" + "=" * 80)
print("✅ TESTE CONCLUÍDO COM SUCESSO")
print("=" * 80)

