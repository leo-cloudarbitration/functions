# 🚀 Cloud Functions Repository

Repositório organizado para funções de coleta e processamento de dados de marketing digital.

## 📁 Estrutura do Projeto

```
functions/
├── facebook_ads/                    # Funções relacionadas ao Facebook Ads
│   └── cloud_facebook_adsperformance/
│       ├── main.py                  # Script principal de coleta
│       ├── requirements.txt         # Dependências Python
│       └── README.md                # Documentação específica
├── google_ads/                      # Funções relacionadas ao Google Ads
├── analytics/                       # Funções de analytics e relatórios
└── utils/                           # Utilitários compartilhados

.github/workflows/                   # GitHub Actions
└── cloud_facebook_adsperformance.yml
```

## 🔄 Workflows Ativos

### Facebook Ads Performance
- **Agendamento:** Diário às 06:00 BRT
- **Função:** Coleta dados de performance de anúncios do Facebook
- **Destino:** BigQuery (`data-v1-423414.test.cloud_facebook_adsperformance_historical`)
- **Localização:** `functions/facebook_ads/cloud_facebook_adsperformance/`

## 🛠️ Como Adicionar Novas Funções

1. **Criar pasta** em `functions/[categoria]/[nome_da_funcao]/`
2. **Adicionar código** da função
3. **Criar workflow** em `.github/workflows/`
4. **Configurar secrets** necessários
5. **Testar** com `workflow_dispatch`

### Exemplo de Nova Função:
```
functions/
└── google_ads/
    └── cloud_google_ads_performance/
        ├── main.py
        ├── requirements.txt
        └── README.md
```

## 🔐 Secrets Configurados

- `SECRET_GOOGLE_SERVICE_ACCOUNT`: Credenciais do Google Cloud

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