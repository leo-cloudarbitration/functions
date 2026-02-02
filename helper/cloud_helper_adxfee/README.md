# Cloud Helper AdX Fee - GitHub Actions

Este projeto sincroniza dados de AdX Fee do Google Sheets para o BigQuery usando GitHub Actions.

## 🎯 Objetivo

Coletar dados de taxas AdX (Ad Exchange Fee) de uma planilha do Google Sheets e sincronizar com o BigQuery, sobrescrevendo os dados existentes (WRITE_TRUNCATE).

## 📅 Execução

- **Agendado**: Todo dia às 5:10 BRT (08:10 UTC)
- **Manual**: Via `workflow_dispatch` no GitHub Actions

## 📊 Dados Coletados

### Campos:
- `date` - Data (DATE)
- `adxfee` - Taxa AdX Fee (FLOAT64)
- `network_code` - Código da rede (STRING)
- `imported_at` - Timestamp da importação (TIMESTAMP)

## 🗄️ Destino

**BigQuery:**
- **Tabela:** `data-v1-423414.test.sheets_adxfee` (configurável via variável de ambiente)
- **Modo de escrita:** `WRITE_TRUNCATE` (sobrescreve completamente os dados a cada execução)

## ⚙️ Configuração

### Secrets Necessários (GitHub Actions)

#### `SECRET_GOOGLE_SERVICE_ACCOUNT`
Credenciais do Google Cloud Service Account (formato JSON completo). Deve ter permissões para:
- Ler Google Sheets
- Escrever no BigQuery

#### `SHEET_ID` (Opcional)
ID da planilha do Google Sheets. Padrão: `1Fsq0xbVtjZ71SajCyR9WDLr1S_tWHm_yhtRBqeJOpGM`

#### `WORKSHEET` (Opcional)
Nome da aba da planilha. Padrão: `adxfee`

#### `BIGQUERY_TABLE` (Opcional)
ID completo da tabela no BigQuery. Padrão: `data-v1-423414.test.sheets_adxfee`

### Estrutura de Arquivos

```
helper/cloud_helper_adxfee/
├── main.py              # Script principal
├── requirements.txt     # Dependências Python
└── README.md           # Este arquivo
```

## 🚀 Execução

O workflow é executado automaticamente:
- **Agendado**: Todo dia às 5:10 BRT (08:10 UTC)
- **Manual**: Via `workflow_dispatch` no GitHub Actions

### Execução Local

Para executar localmente:

1. Configure as variáveis de ambiente:
```bash
export SECRET_GOOGLE_SERVICE_ACCOUNT='{"type":"service_account",...}'
export SHEET_ID="seu_sheet_id"
export WORKSHEET="adxfee"
export BIGQUERY_TABLE="data-v1-423414.test.sheets_adxfee"
```

2. Ou configure um arquivo `service_account.json` local

3. Execute:
```bash
cd helper/cloud_helper_adxfee
pip install -r requirements.txt
python main.py
```

## 📋 Estrutura da Planilha

A planilha do Google Sheets deve ter as seguintes colunas:
- `date` - Data
- `xrate` ou `adxfee` - Taxa AdX Fee
- `network_code` - Código da rede (opcional)

**Nota:** Se a coluna for `xrate`, ela será automaticamente renomeada para `adxfee` para corresponder ao schema do BigQuery.

## ⚠️ Notas Importantes

1. **Modo de escrita:** TRUNCATE (sobrescreve completamente os dados a cada execução)
2. **Compatibilidade:** O código aceita tanto `xrate` quanto `adxfee` como nome de coluna
3. **Validação:** Linhas sem `date` ou `adxfee` são removidas
4. **Timezone:** `imported_at` usa UTC

## 🔍 Troubleshooting

### Erro: "SECRET_GOOGLE_SERVICE_ACCOUNT não encontrado"
- Verifique se o secret está configurado no GitHub Actions
- Certifique-se que o JSON está válido

### Erro de autenticação Google Sheets
- Verifique se `SECRET_GOOGLE_SERVICE_ACCOUNT` está correto
- Certifique-se que a Service Account tem permissões de leitura na planilha

### Erro de autenticação BigQuery
- Verifique se `SECRET_GOOGLE_SERVICE_ACCOUNT` está correto
- Certifique-se que a Service Account tem permissões de escrita no BigQuery

### Planilha não encontrada
- Verifique se o `SHEET_ID` está correto
- Verifique se a Service Account tem acesso à planilha
- Verifique se o nome da aba (`WORKSHEET`) está correto

## 📊 Estrutura da Tabela BigQuery

```sql
CREATE TABLE `data-v1-423414.test.sheets_adxfee` (
  date DATE,
  adxfee FLOAT64,
  network_code STRING,
  imported_at TIMESTAMP
);
```

## 🔧 Configuração para Produção

1. Configure os secrets no GitHub:
   - `SECRET_GOOGLE_SERVICE_ACCOUNT` (obrigatório)
   - `SHEET_ID` (opcional, tem padrão)
   - `WORKSHEET` (opcional, padrão: "adxfee")
   - `BIGQUERY_TABLE` (opcional, tem padrão)

2. O workflow está configurado em:
   - `.github/workflows/cloud_helper_adxfee.yml`

3. Verifique os logs no GitHub Actions após cada execução

