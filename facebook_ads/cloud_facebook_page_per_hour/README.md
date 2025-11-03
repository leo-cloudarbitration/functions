# Facebook Page Performance Per Hour - GitHub Actions

Este projeto coleta dados de performance de páginas do Facebook **agregados por hora** e os envia para o BigQuery usando GitHub Actions.

## Características

- ✨ **Async/Await**: Implementação totalmente assíncrona para máxima performance
- 🚀 **Processamento Paralelo**: Múltiplos grupos e contas processados simultaneamente
- ⏱️ **Dados Horários**: Coleta de métricas agregadas por intervalo de hora
- 📊 **BigQuery**: Upload direto dos dados processados

## Configuração

### 1. Secrets do GitHub

Configure o seguinte secret no seu repositório GitHub:

- `SECRET_GOOGLE_SERVICE_ACCOUNT`: JSON completo das credenciais do Google Cloud Service Account

**Nota**: As credenciais do BigQuery são carregadas automaticamente da variável de ambiente `SECRET_GOOGLE_SERVICE_ACCOUNT`. Se não estiver disponível, o sistema usará as credenciais padrão do ambiente.

### 2. Estrutura de Arquivos

```
facebook_ads/cloud_facebook_page_per_hour/
├── main.py              # Script principal
├── requirements.txt     # Dependências Python
└── README.md           # Este arquivo
```

### 3. Execução

O workflow pode ser executado:
- **Agendado**: Configure no GitHub Actions (ex: diariamente às 06:00 BRT)
- **Manual**: Via `workflow_dispatch` no GitHub
- **Local**: Execute `python main.py` para testes locais

### 4. Dados Coletados

O script coleta métricas de campanhas do Facebook de **ontem** com breakdown por hora:
- Impressões (agregadas por hora)
- Gastos (agregados por hora)
- Cliques em links (agregados por hora)
- Category (extraída do nome da campanha)
- Account name e ID
- Timestamp de importação

### 5. Destino dos Dados

Os dados são enviados para a tabela BigQuery:
`data-v1-423414.test.cloud_facebook_page_per_hour`

### 6. Grupos de Contas

O script processa múltiplos grupos de contas Facebook em paralelo:
- **CASF A, A2**: 15 contas
- **CASF B, B2**: 34 contas
- **CASF C, C2, C3**: 41 contas
- **Cloud Arbitration 1, 2, 3**: 9 contas
- **CAAG A, B**: 6 contas
- **Nassovia A**: 3 contas

**Total**: 108 contas em 14 grupos

### 7. Performance

- Processamento assíncrono com `aiohttp`
- Conexões simultâneas otimizadas (100 total, 30 por host)
- Processamento em lotes de 5 contas por vez
- Rate limiting automático com delays configuráveis

### 8. Logs

Todos os logs são exibidos durante a execução, incluindo:
- Status de cada grupo processado
- Número de registros coletados por grupo
- Total de dados agregados
- Tempo de execução
- Erros e avisos

## Processamento de Dados

### Extração de Category
A category é extraída do nome da campanha:
- Extrai os primeiros 3 segmentos separados por "_"
- Exemplo: `CASF_BR_GOLD_...` → Category: `CASF_BR_GOLD`

### Agregação
Os dados são agregados por:
- Category
- Account name
- Account ID
- Date
- Time interval (hora)

## Troubleshooting

### Erro de Credenciais
- Verifique se o secret `SECRET_GOOGLE_SERVICE_ACCOUNT` está configurado corretamente
- Confirme se o Service Account tem permissões no BigQuery

### Rate Limiting
- O script já inclui delays automáticos para evitar rate limiting
- Configurações atuais:
  - `REQUEST_DELAY`: 0.35s
  - `ACCOUNT_DELAY`: 0.7s
  - `RATE_LIMIT_DELAY`: 30.0s
  - Batch size: 5 contas por vez

### Timeout
- Timeout configurado para 60 segundos por requisição
- Se necessário, ajuste em `aiohttp.ClientTimeout(total=60)`

### Dados Excessivos
- Algumas contas podem retornar erro 500 por dados excessivos
- O script tenta novamente automaticamente com exponential backoff (3 tentativas)

## Dependências Principais

- `aiohttp`: Cliente HTTP assíncrono
- `pandas`: Processamento de dados
- `google-cloud-bigquery`: Upload para BigQuery
- `functions-framework`: Suporte para Cloud Functions
- `pytz`: Timezone handling

## Adaptações Necessárias

Antes de fazer deploy em produção, considere:

1. **Tokens de Acesso Facebook**: ⚠️ Os tokens do Facebook ainda estão hardcoded no arquivo. Considere movê-los para variáveis de ambiente ou secrets do GitHub para maior segurança
2. **Credenciais GCP**: ✅ Já configurado para usar secrets do GitHub via `SECRET_GOOGLE_SERVICE_ACCOUNT`
3. **Workflow File**: Criar `.github/workflows/cloud_facebook_page_per_hour.yml`
4. **Table ID**: Validar a tabela de destino no BigQuery
5. **Agendamento**: Configurar horário ideal de execução

## Próximos Passos

- [ ] Criar workflow do GitHub Actions
- [ ] Mover credenciais para secrets
- [ ] Configurar agendamento automático
- [ ] Testar execução completa
- [ ] Documentar estrutura da tabela BigQuery

