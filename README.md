# 🚀 Cloud Functions Repository

Repositório organizado para funções de coleta e processamento de dados de marketing digital.

## 📁 Estrutura do Projeto

```
├── facebook_ads/                       # Funções relacionadas ao Facebook Ads
│   └── cloud_facebook_adsperformance/
│       ├── main.py                      # Script principal de coleta
│       ├── requirements.txt            # Dependências Python
│       └── README.md                    # Documentação específica
├── gam/                                # Funções relacionadas ao GAM (Google Ad Manager)
│   └── cloud_gam_adsperformance/
│       ├── main.py                      # Script principal de coleta GAM
│       ├── requirements.txt            # Dependências Python
│       └── README.md                    # Documentação específica
├── helper/                              # Funções auxiliares e sincronização
│   └── cloud_adsperformance_creative_mapping/
│       ├── main.py                      # Sync Supabase → BigQuery
│       ├── requirements.txt            # Dependências Python
│       └── README.md                    # Documentação específica
├── google_ads/                          # Funções relacionadas ao Google Ads
├── analytics/                           # Funções de analytics e relatórios
├── utils/                               # Utilitários compartilhados
├── .github/workflows/                   # 🚀 GitHub Actions Workflows
│   ├── cloud_facebook_adsperformance.yml           # Workflow Facebook Ads
│   ├── cloud_gam_adsperformance.yml                # Workflow GAM Ads
│   ├── cloud_adsperformance_creative_mapping.yml   # Workflow Creative Mapping
│   ├── debug_bigquery.yml                          # Workflow de debug
│   └── test_connection.yml                         # Workflow de teste
└── README.md                            # Documentação
```

## 🔄 Workflows Ativos

### Facebook Ads Performance
- **Agendamento:** Diário às 06:00 BRT
- **Função:** Coleta dados de performance de anúncios do Facebook
- **Destino:** BigQuery (`data-v1-423414.test.cloud_facebook_adsperformance_historical`)
- **Localização:** `facebook_ads/cloud_facebook_adsperformance/`

### GAM Ads Performance
- **Agendamento:** Diário às 06:30 BRT
- **Função:** Coleta dados de performance do Google Ad Manager com utm_content
- **Destino:** BigQuery (`data-v1-423414.test.cloud_gam_adsperformance_historical`)
- **Localização:** `gam/cloud_gam_adsperformance/`

### Creative Mapping Sync
- **Agendamento:** Diário às 07:00 BRT
- **Função:** Sincroniza mapeamento de criativos do Supabase para BigQuery
- **Destino:** BigQuery (`data-v1-423414.test.cloud_adsperformance_creative_mapping`)
- **Localização:** `helper/cloud_adsperformance_creative_mapping/`

## 🛠️ Como Adicionar Novas Funções

1. **Criar pasta** em `[categoria]/[nome_da_funcao]/`
2. **Adicionar código** da função
3. **Criar workflow** em `.github/workflows/` (ex: `nova_funcao.yml`)
4. **Configurar secrets** necessários
5. **Testar** com `workflow_dispatch`

### Exemplo de Nova Função:
```
google_ads/
└── cloud_google_ads_performance/
    ├── main.py
    ├── requirements.txt
    └── README.md

# Workflow em .github/workflows/:
.github/workflows/cloud_google_ads_performance.yml
```

## 🔐 Secrets Configurados

### Obrigatórios:
- `SECRET_GOOGLE_SERVICE_ACCOUNT`: Credenciais do Google Cloud Service Account

### Para Creative Mapping (Supabase):
- `SUPABASE_URL`: URL do projeto Supabase (ex: `https://xxxxx.supabase.co`)
- `SUPABASE_KEY`: Chave de API do Supabase (service_role ou anon key)
- `SUPABASE_TABLE`: Nome da tabela no Supabase (padrão: `creative_mapping`)

## 📊 Monitoramento

- **Logs:** GitHub Actions → Workflows
- **Dados:** BigQuery Console
- **Status:** ✅ Funcionando perfeitamente

## 🚀 Deploy Local

```bash
# Editar código no Cursor
# Fazer commit e push
git add .
git commit -m "Sua alteração"
git push origin main

# Executar no GitHub Actions
# Vá em: Actions → cloud_facebook_adsperformance → Run workflow
```

---
*Última atualização: 27/10/2025*