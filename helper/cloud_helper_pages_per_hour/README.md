# Pages Per Hour Sync (Google Sheets → BigQuery)

Função para sincronização de dados de páginas por hora do Google Sheets para BigQuery.

## 📊 Funcionalidades

- **Sincronização completa** de dados do Google Sheets
- **Upload automático** para BigQuery
- **Substituição de dados** (WRITE_TRUNCATE) para manter sincronizado
- **Execução agendada** diária às 07:00 BRT

## 🎯 Dados Sincronizados

A função busca todos os registros do Google Sheets e sincroniza para BigQuery:

- **url**: URL da página (STRING)
- **category**: Categoria da página (STRING)
- **category_mae**: Categoria mãe (STRING)
- **imported_at**: Timestamp de importação (DATETIME)

## 📈 Origem e Destino dos Dados

**Google Sheets ID:** `1hEKsS5VtOw58OKnO6clcSjtZ25ckm5urJSC5EcIV_Oo`

**BigQuery Table:** `data-v1-423414.test.cloud_snapshot_page_per_hour`

**Modo:** WRITE_TRUNCATE (substitui todos os dados a cada execução)

## ⚙️ Configuração

### Secrets Necessários no GitHub:

1. **SECRET_GOOGLE_SERVICE_ACCOUNT** - Credenciais do BigQuery e Google Sheets (já configurado)

**Nota:** A mesma service account deve ter acesso ao Google Sheets.

### Permissões Necessárias:

A service account precisa ter acesso ao Google Sheets:
1. Abra o Google Sheets
2. Clique em "Compartilhar"
3. Adicione o email da service account como Editor
4. O email está no arquivo JSON das credenciais (campo `client_email`)

## 🚀 Execução

### Automática:
- **Agendamento:** Diário às 07:00 BRT
- **Modo:** Sincronização completa (substitui dados)

### Manual:
```bash
# Via GitHub Actions
# Actions → cloud_helper_pages_per_hour → Run workflow

# Local (desenvolvimento)
cd helper/cloud_helper_pages_per_hour
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
python main.py
```

## 📝 Logs

Monitore a execução em:
- **GitHub Actions:** Actions → cloud_helper_pages_per_hour
- **BigQuery:** Console → cloud_snapshot_page_per_hour

## 🔧 Personalização

### Ajustar Schema do BigQuery:

Se sua planilha tiver colunas diferentes, edite o schema em `main.py`:

```python
schema = [
    bigquery.SchemaField("sua_coluna", "STRING"),
    # ... adicione suas colunas aqui
]
```

### Ajustar Range da Planilha:

Por padrão, lê a primeira aba completa. Para ajustar:

```python
SHEETS_RANGE = "Sheet1!A1:Z1000"  # Exemplo de range específico
```

---
*Criado em: 05/11/2025*

