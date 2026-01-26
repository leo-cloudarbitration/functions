# Facebook Ads Hourly Data - GitHub Actions

Este projeto coleta dados de performance de campanhas do Facebook **agregados por hora** e os envia para o BigQuery usando GitHub Actions.

## 🎯 Objetivo

Coletar métricas horárias de campanhas do Facebook de **ONTEM** e armazenar no BigQuery para análise histórica.

## 📅 Período de Coleta

- **Data coletada:** ONTEM (date_preset: "yesterday")
- **Exemplo:** Se hoje é 07/01/2026, coleta dados de 06/01/2026
- **Breakdown:** Por hora (hourly_stats_aggregated_by_advertiser_time_zone)

## 📊 Dados Coletados

### Campos extraídos:
- `site_name` - Primeiros 2 caracteres do nome da campanha
- `account_name` - Nome da conta
- `account_id` - ID da conta
- `date` - Data dos dados
- `time_interval` - Intervalo horário (0-23)
- `impressions` - Número de impressões
- `spend` - Gasto total
- `clicks` - Número de cliques em links
- `imported_at` - Timestamp da importação

## 🗄️ Destino

**BigQuery:**
- **Tabela:** `data-v1-423414.test.cloud_facebook_hour_historical`
- **Modo de escrita:** `WRITE_APPEND` (adiciona novos dados, acumula histórico)

## ⚙️ Configuração

### Secrets Necessários (GitHub Actions)

#### `SECRET_GOOGLE_SERVICE_ACCOUNT`
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

### Grupos de Contas Configurados

O script processa múltiplos grupos de contas Facebook em paralelo:
- **CASF A, A2**: 15 contas
- **CASF B, B2**: 34 contas
- **CASF C, C2, C3**: 41 contas
- **Cloud Arbitration 1, 2, 3**: 9 contas
- **CAAG A, B**: 6 contas
- **Nassovia A**: 3 contas

**Total**: 108 contas em 13 grupos

## 🚀 Execução

### GitHub Actions (Automático)
- **Agendamento:** Diariamente às 07:00 BRT (10:00 UTC)
- **Workflow:** `.github/workflows/cloud_facebook_hour_yesterday.yml`

### Execução Manual
```bash
# Configurar variáveis de ambiente
export SECRET_GOOGLE_SERVICE_ACCOUNT='{"type":"service_account",...}'

# Executar
python main.py
```

## 📦 Dependências

Instale as dependências com:
```bash
pip install -r requirements.txt
```

Principais bibliotecas:
- `google-cloud-bigquery==3.15.0` - Cliente BigQuery
- `google-cloud-storage==2.13.0` - Cliente Google Cloud Storage
- `aiohttp==3.9.1` - Cliente HTTP assíncrono
- `pandas==2.1.4` - Manipulação de dados
- `pytz==2023.3` - Timezones
- `functions-framework==3.5.0` - Suporte para Cloud Functions

## 🔄 Fluxo de Execução

1. ✅ **Verificação de Credenciais** - Valida se as credenciais estão configuradas
2. ✅ **Criação de Clientes** - Inicializa cliente BigQuery
3. ✅ **Coleta de Dados** - Para cada grupo e conta:
   - Faz requisição à API do Facebook Graph
   - Usa retry logic (3 tentativas) para resiliência
   - Processa em lotes de 5 contas por vez
   - Aguarda delays configuráveis entre requisições
4. ✅ **Processamento** - Agrega dados por hora
5. ✅ **Salvamento no BigQuery** - Insere todos os dados coletados

## 🛡️ Resiliência

O script implementa:
- **Retry Logic:** 3 tentativas com backoff exponencial (2s, 4s, 8s)
- **Processamento Assíncrono:** Múltiplos grupos processados em paralelo
- **Rate Limiting:** Delays automáticos entre requisições
- **Batch Processing:** Processa contas em lotes de 5 para evitar sobrecarga
- **Tratamento de erros:** Continua processando mesmo se uma conta falhar

## ⚡ Performance

- **Processamento Assíncrono:** Usa `aiohttp` para requisições paralelas
- **Conexões Otimizadas:** 100 conexões totais, 30 por host
- **Timeout:** 60 segundos por requisição
- **Configurações de Paralelismo:**
  - `MAX_WORKERS`: 15
  - `REQUEST_DELAY`: 0.35s
  - `ACCOUNT_DELAY`: 0.7s
  - `RATE_LIMIT_DELAY`: 30.0s

## 📝 Logs

O script gera logs detalhados:
```
🔄 Processando 13 grupos em paralelo para máxima velocidade
📊 [casf_a] Processando 7 contas...
✅ [casf_a] Processamento concluído: 150 registros
📊 Total de dados coletados: 2000 registros
📈 Dados processados por hora: 500 registros
☁️ Fazendo upload para BigQuery: data-v1-423414.test.cloud_facebook_hour_historical
✅ Dados enviados com sucesso para o BigQuery!
🎉 Execução concluída com sucesso!
```

## ⚠️ Notas Importantes

1. **Data de coleta:** O script coleta dados de ONTEM (date_preset: "yesterday")
2. **Timezone:** Usa timezone de São Paulo (America/Sao_Paulo) para `imported_at`
3. **Modo de escrita:** APPEND (adiciona novos dados, acumula histórico completo)
4. **Tabela única:** Todos os grupos usam a mesma tabela `cloud_facebook_hour_historical`

## 🔍 Troubleshooting

### Erro: "SECRET_GOOGLE_SERVICE_ACCOUNT não encontrado"
- Verifique se o secret está configurado no GitHub Actions
- Certifique-se que o JSON está válido

### Erro de autenticação BigQuery
- Verifique se `SECRET_GOOGLE_SERVICE_ACCOUNT` está correto
- Certifique-se que a Service Account tem permissões de escrita no BigQuery

### Rate Limiting do Facebook
- O script já inclui delays automáticos
- Se necessário, ajuste `REQUEST_DELAY` e `ACCOUNT_DELAY` no código

### Timeout
- Timeout configurado para 60 segundos por requisição
- Se necessário, ajuste em `aiohttp.ClientTimeout(total=60)`

## 📊 Estrutura da Tabela BigQuery

```sql
CREATE TABLE `data-v1-423414.test.cloud_facebook_hour_historical` (
  site_name STRING,
  account_name STRING,
  account_id STRING,
  date STRING,
  time_interval STRING,
  impressions INT64,
  spend FLOAT64,
  clicks INT64,
  imported_at STRING
);
```

## 🔧 Configuração para Produção

### ✅ Já Configurado

1. **Credenciais GCP**: ✅ Configurado para usar secrets do GitHub via `SECRET_GOOGLE_SERVICE_ACCOUNT`
2. **Workflow GitHub Actions**: ✅ Criado e configurado para rodar às 07h BRT
3. **Modo de Escrita**: ✅ WRITE_APPEND ativado para acumular histórico
4. **Processamento Assíncrono**: ✅ Otimizado para máxima performance
5. **Tabela Única**: ✅ Todos os grupos usam `cloud_facebook_hour_historical`

### ⚠️ Opcional

1. **Tokens de Acesso Facebook**: Os tokens do Facebook estão hardcoded no arquivo. Considere movê-los para variáveis de ambiente ou secrets do GitHub para maior segurança

## 📈 Status do Projeto

- [x] Criar workflow do GitHub Actions ✅
- [x] Configurar agendamento automático (07h BRT diariamente) ✅
- [x] Configurar credenciais do Google Cloud ✅
- [x] Implementar processamento assíncrono ✅
- [x] Configurar tabela única para todos os grupos ✅
- [x] Criar README completo ✅
- [ ] Mover tokens do Facebook para secrets (opcional)
- [ ] Documentar estrutura da tabela BigQuery (opcional)

---

**Última atualização:** Janeiro 2026

