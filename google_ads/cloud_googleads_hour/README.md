# Google Ads Hourly Data - GitHub Actions

Este projeto coleta dados horários de campanhas do Google Ads e os envia para o BigQuery usando GitHub Actions.

## Configuração

### 1. Secrets do GitHub

Configure os seguintes secrets no seu repositório GitHub:

- `SECRET_GOOGLE_SERVICE_ACCOUNT`: JSON completo das credenciais do Google Cloud Service Account
- `SECRET_GOOGLE_ADS_CONFIG`: JSON com as configurações do Google Ads (formato abaixo)

#### Formato do SECRET_GOOGLE_ADS_CONFIG:
```json
{
  "developer_token": "SEU_DEVELOPER_TOKEN",
  "client_id": "SEU_CLIENT_ID.apps.googleusercontent.com",
  "client_secret": "SEU_CLIENT_SECRET",
  "refresh_token": "SEU_REFRESH_TOKEN",
  "login_customer_id": "SEU_LOGIN_CUSTOMER_ID",
  "token_uri": "https://oauth2.googleapis.com/token",
  "use_proto_plus": true
}
```

### 2. Estrutura de Arquivos

```
google_ads/cloud_googleads_hour/
├── main.py              # Script principal
├── requirements.txt     # Dependências Python
└── README.md           # Este arquivo

.github/workflows/
└── cloud_googleads_hour.yml  # Workflow do GitHub Actions
```

### 3. Execução

O workflow é executado automaticamente:
- **Agendado**: Todos os dias às 08:00 BRT (11:00 UTC)
- **Manual**: Via `workflow_dispatch` no GitHub

### 4. Dados Coletados

O script coleta métricas horárias de campanhas do Google Ads para o dia atual:
- Data e hora (0-23)
- Account ID e Account Name
- Campaign ID e Campaign Name
- Moeda e Budget
- Spend, Clicks, CPC
- Impressions, CTR
- Conversions, Cost per Conversion

### 5. Destino dos Dados

Os dados são enviados para a tabela BigQuery:
`data-v1-423414.test.cloud_googleads_hour`

**Modo de escrita**: `WRITE_TRUNCATE` (sobrescreve os dados existentes)

### 6. Contas Processadas

O script processa as seguintes contas Google Ads:
- 9679496200
- 2153708041
- 1378108795
- 5088162800
- 7205935192

### 7. Logs

Todos os logs são exibidos no GitHub Actions, incluindo:
- Status de cada conta processada
- Número de registros coletados
- Tempo de execução
- Erros e avisos

## Execução Local

Para executar localmente, configure as variáveis de ambiente:

```bash
export SECRET_GOOGLE_SERVICE_ACCOUNT='{"type":"service_account",...}'
export SECRET_GOOGLE_ADS_CONFIG='{"developer_token":"...",...}'
```

Ou use arquivos de credenciais:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_ADS_DEVELOPER_TOKEN="seu_token"
export GOOGLE_ADS_CLIENT_ID="seu_client_id"
export GOOGLE_ADS_CLIENT_SECRET="seu_client_secret"
export GOOGLE_ADS_REFRESH_TOKEN="seu_refresh_token"
export GOOGLE_ADS_LOGIN_CUSTOMER_ID="seu_login_customer_id"
```

Depois execute:

```bash
cd google_ads/cloud_googleads_hour
pip install -r requirements.txt
python main.py
```

## Troubleshooting

### Erro de Credenciais
- Verifique se os secrets estão configurados corretamente no GitHub
- Confirme se o Service Account tem permissões no BigQuery
- Verifique se as credenciais do Google Ads estão válidas

### Erro de API
- Confirme que o Developer Token está aprovado
- Verifique se o Login Customer ID está correto
- Confirme que as contas têm acesso às campanhas

### Dados Vazios
- Verifique se há campanhas ativas nas contas
- Confirme se há dados para a data atual
- Verifique os logs para mensagens de erro específicas

## Schema da Tabela BigQuery

| Campo | Tipo | Descrição |
|-------|------|-----------|
| account_name | STRING | Nome da conta |
| account_id | STRING | ID da conta |
| campaign_id | STRING | ID da campanha |
| campaign_name | STRING | Nome da campanha |
| date | DATE | Data dos dados |
| hour | INTEGER | Hora do dia (0-23) |
| moeda | STRING | Código da moeda |
| budget | FLOAT | Budget da campanha |
| spend | FLOAT | Gasto |
| clicks | INTEGER | Número de cliques |
| cpc | FLOAT | Custo por clique |
| impressions | INTEGER | Número de impressões |
| ctr | FLOAT | Taxa de cliques |
| conversions | FLOAT | Número de conversões |
| cost_per_conversion | FLOAT | Custo por conversão |

