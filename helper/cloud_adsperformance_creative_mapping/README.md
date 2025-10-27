# Creative Mapping Sync (Supabase → BigQuery)

Função para sincronização de dados de mapeamento de criativos do Supabase para BigQuery.

## 📊 Funcionalidades

- **Sincronização completa** de dados do Supabase
- **Upload automático** para BigQuery
- **Substituição de dados** (WRITE_TRUNCATE) para manter sincronizado
- **Execução agendada** diária às 07:00 BRT

## 🎯 Dados Sincronizados

A função busca todos os registros da tabela Supabase e sincroniza para BigQuery:

- **id**: ID único do registro
- **creative_id**: ID do criativo
- **creative_name**: Nome do criativo
- **campaign_id**: ID da campanha
- **campaign_name**: Nome da campanha
- **ad_account_id**: ID da conta de anúncios
- **platform**: Plataforma (Facebook, Google, etc)
- **created_at**: Data de criação
- **updated_at**: Data de atualização
- **imported_at**: Timestamp de importação

## 📈 Destino dos Dados

**BigQuery Table:** `data-v1-423414.test.cloud_adsperformance_creative_mapping`

**Modo:** WRITE_TRUNCATE (substitui todos os dados a cada execução)

## ⚙️ Configuração

### Secrets Necessários no GitHub:

1. **SECRET_GOOGLE_SERVICE_ACCOUNT** - Credenciais do BigQuery (já configurado)
2. **SUPABASE_URL** - URL do projeto Supabase
3. **SUPABASE_KEY** - Chave de API do Supabase

**Nota:** O nome da tabela (`adsperfomance_creative_mapping`) está fixo no código.

### Como Adicionar Secrets:

1. Vá para o repositório no GitHub
2. **Settings** → **Secrets and variables** → **Actions**
3. Clique em **New repository secret**
4. Adicione cada secret acima

## 🚀 Execução

### Automática:
- **Agendamento:** Diário às 07:00 BRT
- **Modo:** Sincronização completa (substitui dados)

### Manual:
```bash
# Via GitHub Actions
# Actions → cloud_adsperformance_creative_mapping → Run workflow

# Local (desenvolvimento)
cd helper/cloud_adsperformance_creative_mapping
export SUPABASE_URL="sua_url"
export SUPABASE_KEY="sua_chave"
python main.py
```

## 📝 Logs

Monitore a execução em:
- **GitHub Actions:** Actions → cloud_adsperformance_creative_mapping
- **BigQuery:** Console → cloud_adsperformance_creative_mapping

## 🔧 Personalização

### Ajustar Schema do BigQuery:

Se sua tabela Supabase tiver colunas diferentes, edite o schema em `main.py`:

```python
schema = [
    bigquery.SchemaField("sua_coluna", "STRING"),
    # ... adicione suas colunas aqui
]
```

### Ajustar Nome da Tabela Supabase:

Edite diretamente em `main.py`:

```python
SUPABASE_TABLE = "nome_da_sua_tabela"  # Linha 44
```

---
*Última atualização: 27/10/2025*

