# Facebook Ads Performance - GitHub Actions

Este projeto coleta dados de performance de anúncios do Facebook e os envia para o BigQuery usando GitHub Actions.

## Configuração

### 1. Secrets do GitHub

Configure o seguinte secret no seu repositório GitHub:

- `SECRET_GOOGLE_SERVICE_ACCOUNT`: JSON completo das credenciais do Google Cloud Service Account

### 2. Estrutura de Arquivos

```
cloud_function_deploy/
├── main.py              # Script principal
├── requirements.txt     # Dependências Python
└── README.md           # Este arquivo

.github/workflows/
└── cloud_facebook_adsperformance.yml  # Workflow do GitHub Actions
```

### 3. Execução

O workflow é executado automaticamente:
- **Agendado**: Todos os dias às 06:00 BRT (09:00 UTC)
- **Manual**: Via `workflow_dispatch` no GitHub

### 4. Dados Coletados

O script coleta métricas de anúncios do Facebook de **ontem**:
- Impressões, cliques, gastos
- CTR, CPM
- IDs de anúncio, campanha, conta
- Creative IDs (incluindo criativos dinâmicos)

### 5. Destino dos Dados

Os dados são enviados para a tabela BigQuery:
`data-v1-423414.test.cloud_facebook_adsperformance_historical`

### 6. Grupos de Contas

O script processa múltiplos grupos de contas Facebook em paralelo:
- CASF A, A2, B, B2, C, C2, C3
- Cloud Arbitration 1, 2, 3
- CAAG A, B

### 7. Logs

Todos os logs são exibidos no GitHub Actions, incluindo:
- Status de cada grupo processado
- Número de registros coletados
- Tempo de execução
- Erros e avisos

## Troubleshooting

### Erro de Credenciais
- Verifique se o secret `SECRET_GOOGLE_SERVICE_ACCOUNT` está configurado corretamente
- Confirme se o Service Account tem permissões no BigQuery

### Rate Limiting
- O script já inclui delays automáticos para evitar rate limiting
- Se necessário, ajuste os delays nas variáveis de ambiente

### Dados Excessivos
- Algumas contas podem retornar erro 500 por dados excessivos
- O script tenta novamente automaticamente com delays maiores
