# 📊 Google Ads → BigQuery - Dados de Anteontem

Coleta dados de performance do Google Ads de **ANTEONTEM** (2 dias atrás) e envia para o BigQuery.

## 🎯 Objetivo

Coletar métricas diárias de campanhas do Google Ads do dia **ANTEONTEM** (before yesterday = 2 dias atrás) e armazenar no BigQuery para análise histórica.

## 📅 Período de Coleta

- **Data coletada:** ANTEONTEM (hoje - 2 dias)
- **Exemplo:** Se hoje é 07/01/2026, coleta dados de 05/01/2026

## 📊 Dados Coletados

### Campos extraídos:
- `account_name` - Nome da conta
- `account_id` - ID da conta
- `campaign_id` - ID da campanha
- `campaign_name` - Nome da campanha
- `date` - Data dos dados (ANTEONTEM)
- `moeda` - Código da moeda (BRL, USD, etc)
- `budget` - Budget da campanha
- `spend` - Gasto total
- `clicks` - Número de cliques
- `cpc` - Custo por clique médio
- `impressions` - Número de impressões
- `ctr` - Taxa de cliques
- `conversions` - Número de conversões
- `cost_per_conversion` - Custo por conversão
- `imported_at` - Timestamp da importação

## 🗄️ Destino

**BigQuery:**
- **Tabela:** `data-v1-423414.test.ca_googleads_historical`
- **Modo de escrita:** `WRITE_APPEND` (adiciona novos dados)

## ⚙️ Configuração

### Secrets Necessários (GitHub Actions)

#### 1. `SECRET_GOOGLE_SERVICE_ACCOUNT`
Credenciais do Google Cloud Service Account (formato JSON):
```json
{
  "type": "service_account",
  "project_id": "seu-project-id",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "...",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "..."
}
```

#### 2. `SECRET_GOOGLE_ADS_CONFIG`
Configuração do Google Ads API (formato JSON):
```json
{
  "developer_token": "seu_developer_token",
  "client_id": "seu_client_id.apps.googleusercontent.com",
  "client_secret": "seu_client_secret",
  "refresh_token": "seu_refresh_token",
  "login_customer_id": "1234567890",
  "use_proto_plus": true
}
```

### Customer IDs Configurados

O script coleta dados das seguintes contas (configuradas no código):
```python
CUSTOMER_IDS = [
    "9679496200", #001
    "1378108795", #002
    "2153708041", #003
    "5088162800", #004
    "7205935192", #005
    "4985450045", #006
    "4161586974", #007
    "5074252268", #008
    "8581304094", #009
    "2722606250", #010
]
```

## 🚀 Execução

### GitHub Actions (Automático)
- **Agendamento:** Diariamente às 00:54 BRT (03:54 UTC)
- **Workflow:** `.github/workflows/cloud_googleads_beforeyesterday.yml`

### Execução Manual
```bash
# Configurar variáveis de ambiente
export SECRET_GOOGLE_SERVICE_ACCOUNT='{"type":"service_account",...}'
export SECRET_GOOGLE_ADS_CONFIG='{"developer_token":"...",...}'

# Executar
python main.py
```

## 📦 Dependências

Instale as dependências com:
```bash
pip install -r requirements.txt
```

Principais bibliotecas:
- `google-ads==24.1.0` - API do Google Ads
- `google-cloud-bigquery==3.26.0` - Cliente BigQuery
- `pandas==2.2.3` - Manipulação de dados
- `pytz==2024.2` - Timezones

## 🔄 Fluxo de Execução

1. ✅ **Verificação de Secrets** - Valida se todas as credenciais estão configuradas
2. ✅ **Criação de Clientes** - Inicializa clientes Google Ads e BigQuery
3. ✅ **Coleta de Dados** - Para cada Customer ID:
   - Faz requisição à API do Google Ads
   - Usa retry logic (3 tentativas) para resiliência
   - Aguarda 1 segundo entre contas para evitar rate limiting
4. ✅ **Salvamento no BigQuery** - Insere todos os dados coletados

## 🛡️ Resiliência

O script implementa:
- **Retry Logic:** 3 tentativas com backoff exponencial (2s, 4s, 8s)
- **Delay entre requisições:** 1 segundo entre contas diferentes
- **Tratamento de erros:** Continua processando mesmo se uma conta falhar
- **Configurações GRPC:** Otimizadas para GitHub Actions

## 📝 Logs

O script gera logs detalhados:
```
🚀 Iniciando coleta de dados do Google Ads (ANTEONTEM)...
📅 Data: 2026-01-05
✅ Secrets verificados!
✅ Cliente Google Ads criado!
🔍 [1/10] Processando: 9679496200
   ✅ 45 registros extraídos
...
📈 RESUMO DA COLETA
✅ Sucesso: 10/10
📊 Total de registros: 450
✅ Dados inseridos com sucesso no BigQuery!
```

## ⚠️ Notas Importantes

1. **Data de coleta:** O script sempre coleta dados de ANTEONTEM (2 dias atrás)
2. **Timezone:** Usa timezone de São Paulo (America/Sao_Paulo)
3. **Modo de escrita:** APPEND (não sobrescreve dados existentes)
4. **GRPC:** Usa GRPC por padrão para comunicação com Google Ads API

## 🔍 Troubleshooting

### Erro: "GRPC target method can't be resolved"
- Verifique se as variáveis de ambiente GRPC estão configuradas
- O workflow já configura automaticamente: `GRPC_ENABLE_FORK_SUPPORT=1`, `GRPC_POLL_STRATEGY=poll`

### Erro: "SECRET_GOOGLE_ADS_CONFIG não encontrado"
- Verifique se o secret está configurado no GitHub Actions
- Certifique-se que o JSON está válido (use `true`/`false` minúsculo)

### Erro de autenticação BigQuery
- Verifique se `SECRET_GOOGLE_SERVICE_ACCOUNT` está correto
- Certifique-se que a Service Account tem permissões de escrita no BigQuery

## 📊 Estrutura da Tabela BigQuery

```sql
CREATE TABLE `data-v1-423414.test.ca_googleads_historical` (
  account_name STRING,
  account_id STRING,
  campaign_id STRING,
  campaign_name STRING,
  date DATE,
  moeda STRING,
  budget FLOAT64,
  spend FLOAT64,
  clicks INT64,
  cpc FLOAT64,
  impressions INT64,
  ctr FLOAT64,
  conversions FLOAT64,
  cost_per_conversion FLOAT64,
  imported_at TIMESTAMP
);
```

---

**Última atualização:** 07/01/2026

