# GAM Hour Yesterday - GitHub Actions

Este projeto coleta dados de performance do Google Ad Manager (GAM) **agregados por hora** do dia anterior e os envia para o BigQuery usando GitHub Actions.

## 📊 Características

- ✨ **Async/Await**: Implementação totalmente assíncrona para máxima performance
- 🚀 **Processamento Paralelo**: Múltiplos sites processados simultaneamente
- ⏱️ **Dados Horários**: Coleta de métricas agregadas por intervalo de hora
- 📊 **BigQuery**: Upload direto dos dados processados
- 🔄 **Execução Automática**: Roda diariamente às 10:00 BRT

## 🎯 Dados Coletados

O script coleta métricas do GAM do **dia anterior** com breakdown por hora:

- **date**: Data do relatório
- **hour**: Hora do dia (0-23)
- **domain**: Domínio do site
- **impressions**: Impressões
- **clicks**: Cliques
- **ctr**: Taxa de cliques (CTR)
- **revenue**: Receita (convertida de micros para dólares)
- **ecpm**: eCPM calculado
- **viewable_rate**: Taxa de impressões visualizáveis
- **site_name**: Nome do site

## 🚀 Sites Monitorados

**Total: 14 sites**

- **onplif.com** (Network ID: 23152058020)
- **fintacle.com** (Network ID: 23152058020)
- **amigadamamae.com.br** (Network ID: 23302708904)
- **ifinane.com** (Network ID: 23313676084)
- **finr.com.br** (Network ID: 23314451390)
- **finanzco.com** (Network ID: 22958804404)
- **espacoextra.com.br** (Network ID: 22958804404)
- **vidadeproduto.com.br** (Network ID: 22958804404)
- **tecnologianocampo.com.br** (Network ID: 22024304448)
- **superinvestmentguide.com** (Network ID: 22024304448)
- **brasileirinho.blog.br** (Network ID: 23150219615)
- **bimviral.com** (Network ID: 23295671757)
- **investimentoagora.com.br** (Network ID: 23123915180)
- **vamosestudar.com.br** (Network ID: 23124049988)

## 📈 Destino dos Dados

**BigQuery Table:** `data-v1-423414.test.cloud_gam_hour_historical`

**Modo de escrita**: `WRITE_APPEND` - os dados são acumulados na tabela, permitindo histórico completo

## ⚙️ Configuração

### 1. Secrets do GitHub

Configure o seguinte secret no seu repositório GitHub:

- `SECRET_GOOGLE_SERVICE_ACCOUNT`: JSON completo das credenciais do Google Cloud Service Account

**Nota**: As credenciais do BigQuery são carregadas automaticamente da variável de ambiente `SECRET_GOOGLE_SERVICE_ACCOUNT`.

### 2. Estrutura de Arquivos

```
gam/cloud_gam_hour_yesterday.py/
├── main.py              # Script principal
├── requirements.txt     # Dependências Python
└── README.md           # Este arquivo
```

### 3. Execução

O workflow é executado automaticamente:
- **Agendado**: Todos os dias às 10:00 BRT (13:00 UTC)
- **Manual**: Via `workflow_dispatch` no GitHub Actions
- **Local**: Execute `python main.py` para testes locais

### 4. Workflow GitHub Actions

O arquivo `.github/workflows/cloud_gam_hour_yesterday.yml` está configurado para:
- Rodar automaticamente todos os dias às 10h da manhã (horário de Brasília)
- Buscar dados do **dia anterior** automaticamente
- Pode ser executado manualmente via GitHub Actions
- Autentica automaticamente com Google Cloud usando secrets

## 🔧 Processamento de Dados

### Conversões

- **Revenue**: Convertida de micros (1.000.000) para dólares
- **eCPM**: Calculado como `(revenue * 1000 / impressions)` quando há impressões
- **Timezone**: Utiliza `America/Sao_Paulo` para garantir data correta

### Agregação

Os dados são agregados por:
- Date
- Hour (0-23)
- Domain
- Site name

## 📝 Logs

Todos os logs são exibidos durante a execução, incluindo:
- Status de cada site processado
- Número de registros coletados por site
- Total de dados agregados
- Erros e avisos
- Status de inserção no BigQuery

## 🔍 Troubleshooting

### Erro de Credenciais
- Verifique se o secret `SECRET_GOOGLE_SERVICE_ACCOUNT` está configurado corretamente
- Confirme se o Service Account tem permissões no BigQuery

### Erro na API
- Verifique se a API Key está válida no código
- Confirme se os Network IDs e nomes dos sites estão corretos
- Verifique os logs para ver a resposta completa da API

### Timeout
- O script usa `aiohttp` com timeout padrão
- Se necessário, ajuste as configurações de timeout no código

### Dados Não Coletados
- Verifique se a data do dia anterior está correta
- Confirme se os sites estão ativos e retornando dados
- Verifique os logs para erros específicos de cada site

## 📦 Dependências Principais

- `aiohttp`: Cliente HTTP assíncrono
- `google-cloud-bigquery`: Upload para BigQuery
- `pytz`: Timezone handling

## ✅ Status do Projeto

- [x] Criar workflow do GitHub Actions ✅
- [x] Configurar agendamento automático (10h BRT diariamente) ✅
- [x] Configurar busca de dados do dia anterior ✅
- [x] Implementar processamento assíncrono ✅
- [x] Configurar WRITE_APPEND para acumular histórico ✅
- [ ] Mover API Key para secrets (opcional)
- [ ] Documentar estrutura da tabela BigQuery (opcional)

## 🔐 Segurança

**Nota**: A API Key está atualmente hardcoded no arquivo `main.py`. Para maior segurança em produção, considere movê-la para variáveis de ambiente ou secrets do GitHub.

---
*Última atualização: Janeiro 2025*

