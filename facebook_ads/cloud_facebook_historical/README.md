# Facebook Ads Historical - GitHub Actions

Este projeto coleta dados de performance de campanhas do Facebook de **ANTEONTEM** e os adiciona ao histórico no BigQuery usando GitHub Actions.

## 🎯 Objetivo

Coletar métricas diárias de campanhas do Facebook de **ANTEONTEM** e armazenar no BigQuery acumulando histórico (WRITE_APPEND). Este script é executado diariamente para construir um histórico completo dos dados.

## 📅 Período de Coleta

- **Data coletada:** ANTEONTEM (2 dias atrás)
- **Exemplo:** Se hoje é 07/01/2026, coleta dados de 05/01/2026
- **Atualização:** Todo dia às 00:56 BRT (03:56 UTC)
- **Modo:** APPEND (adiciona novos dados, acumula histórico completo)

## 📊 Dados Coletados

### Métricas de Campanha:
- `account_id` - ID da conta de anúncios
- `account_name` - Nome da conta
- `campaign_id` - ID da campanha
- `campaign_name` - Nome da campanha
- `date_start` - Data de início
- `date_stop` - Data de término
- `spend` - Gasto total
- `objective` - Objetivo da campanha
- `cpc` - Custo por clique
- `ctr` - Taxa de cliques
- `frequency` - Frequência
- `impressions` - Número de impressões
- `reach` - Alcance
- `conversions` - Conversões (quando disponível)
- `daily_budget` - Orçamento diário
- `lifetime_budget` - Orçamento vitalício
- `amount_spent` - Valor gasto
- `campaign_end_time` - Data/hora de término da campanha
- `campaign_status` - Status da campanha
- `imported_at` - Timestamp da importação

## 🗄️ Destino

**BigQuery:**
- **Tabela:** `data-v1-423414.test.cloud_facebook_historical_ca`
- **Modo de escrita:** `WRITE_APPEND` (adiciona novos dados, acumula histórico completo)
- **Comportamento:** Cada execução adiciona os dados de anteontem à tabela, mantendo todo o histórico

## ⚙️ Configuração

### Secrets Necessários (GitHub Actions)

#### `SECRET_GOOGLE_SERVICE_ACCOUNT`
Credenciais do Google Cloud Service Account (formato JSON completo).

#### `SECRET_FACEBOOK_GROUPS_CONFIG`
Configuração dos grupos de contas do Facebook (formato JSON). Veja `README_GROUPS_CONFIG.md` para mais detalhes.

### Estrutura de Arquivos

```
facebook_ads/cloud_facebook_historical/
├── main.py              # Script principal
├── requirements.txt     # Dependências Python
└── README.md           # Este arquivo
```

## 🚀 Execução

O workflow é executado automaticamente:
- **Agendado**: Todo dia às 00:56 BRT (03:56 UTC)
- **Manual**: Via `workflow_dispatch` no GitHub Actions

### Execução Local

Para executar localmente:

1. Configure o arquivo `groups_config.json` na pasta `facebook_ads/`
2. Configure o arquivo `credentials.json` na pasta `facebook_ads/`
3. Execute:
```bash
cd facebook_ads/cloud_facebook_historical
pip install -r requirements.txt
python main.py
```

## 📋 Grupos de Contas

O script processa múltiplos grupos de contas Facebook em paralelo:
- **CASF A, A2**: Contas do grupo CASF A
- **CASF B, B2**: Contas do grupo CASF B
- **CASF C, C2, C3**: Contas do grupo CASF C
- **Cloud Arbitration 1, 2, 3**: Contas de Cloud Arbitration
- **CAAG A, B**: Contas do grupo CAAG
- E outros grupos configurados em `groups_config.json`

## ⚙️ Configurações de Performance

O script usa as seguintes configurações (via variáveis de ambiente):
- `MAX_WORKERS`: 20 (threads paralelas)
- `REQUEST_DELAY`: 1.0s (delay entre requisições)
- `ACCOUNT_DELAY`: 1.5s (delay entre contas)
- `MAX_CHECKS`: 18 (tentativas máximas)
- `SLEEP_SECONDS`: 3s (tempo entre tentativas)

## 📝 Logs

O script gera logs detalhados:
```
🔄 [INSIGHTS] Processando insights da conta act_123...
✅ [INSIGHTS] Conta act_123: 150 registros de insights processados
🔄 [BUDGETS] Processando campanhas da conta act_123...
✅ [BUDGETS] Conta act_123: 50 campanhas processadas
📊 Linhas finais: 2000
☁️ Enviando 2000 registros para data-v1-423414.test.cloud_facebook_historical_ca...
✅ Adicionados 2000 registros para data-v1-423414.test.cloud_facebook_historical_ca
```

## ⚠️ Notas Importantes

1. **Data de coleta:** O script coleta dados de ANTEONTEM (2 dias atrás)
2. **Timezone:** Usa timezone de São Paulo (America/Sao_Paulo) para `imported_at`
3. **Modo de escrita:** APPEND (adiciona novos dados, acumula histórico completo)
4. **Frequência:** Executa uma vez por dia para construir o histórico gradualmente
5. **Contas problemáticas:** Algumas contas usam campos básicos (sem conversions) para evitar erros
6. **Histórico acumulado:** A tabela cresce com o tempo, mantendo todos os dados históricos

## 🔍 Troubleshooting

### Erro: "SECRET_FACEBOOK_GROUPS_CONFIG não encontrado"
- Verifique se o secret está configurado no GitHub Actions
- Certifique-se que o JSON está válido
- Veja `README_GROUPS_CONFIG.md` para mais detalhes

### Erro de autenticação BigQuery
- Verifique se `SECRET_GOOGLE_SERVICE_ACCOUNT` está correto
- Certifique-se que a Service Account tem permissões de escrita no BigQuery

### Rate Limiting do Facebook
- O script já inclui delays automáticos e back-off exponencial
- Se necessário, ajuste `REQUEST_DELAY` e `ACCOUNT_DELAY` via variáveis de ambiente

### Dados Excessivos
- Algumas contas podem retornar erro por dados excessivos
- O script usa limites menores (25-50 registros por página) para contas problemáticas
- Contas problemáticas são automaticamente detectadas e usam campos básicos

### Duplicação de Dados
- O script usa APPEND, então se executar manualmente múltiplas vezes, pode haver duplicação
- Execute apenas uma vez por dia conforme agendado
- Para limpar duplicatas, use queries SQL no BigQuery

## 📊 Estrutura da Tabela BigQuery

```sql
CREATE TABLE `data-v1-423414.test.cloud_facebook_historical_ca` (
  account_name STRING,
  account_id STRING,
  campaign_id STRING,
  campaign_name STRING,
  date_start DATETIME,
  date_stop DATETIME,
  conversions FLOAT64,
  spend FLOAT64,
  objective STRING,
  cpc FLOAT64,
  ctr FLOAT64,
  frequency FLOAT64,
  impressions INT64,
  reach INT64,
  imported_at DATETIME,
  daily_budget FLOAT64,
  lifetime_budget FLOAT64,
  amount_spent FLOAT64,
  campaign_end_time TIMESTAMP,
  campaign_status STRING
);
```

## 🔧 Configuração para Produção

1. Configure os secrets no GitHub:
   - `SECRET_GOOGLE_SERVICE_ACCOUNT`
   - `SECRET_FACEBOOK_GROUPS_CONFIG`

2. O workflow está configurado em:
   - `.github/workflows/cloud_facebook_historical.yml`

3. Verifique os logs no GitHub Actions após cada execução

4. **Importante:** Este script acumula dados históricos. A tabela crescerá com o tempo.

## 📈 Uso do Histórico

A tabela histórica pode ser usada para:
- Análises de tendências ao longo do tempo
- Comparações entre períodos
- Relatórios históricos
- Análises de performance de longo prazo

Para consultar dados específicos, use filtros por `date_start` ou `date_stop` no BigQuery.



