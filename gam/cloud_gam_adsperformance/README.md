# GAM Ads Performance Data Collection

Função para coleta de dados de performance do Google Ad Manager (GAM) com foco em utm_content.

## 📊 Funcionalidades

- **Coleta assíncrona** de dados de múltiplos sites GAM
- **Filtragem por utm_content** para análise de campanhas
- **Agregação inteligente** de métricas por site e network
- **Upload automático** para BigQuery
- **Execução agendada** diária às 06:30 BRT

## 🎯 Dados Coletados

- **date**: Data do relatório
- **network_id**: ID da rede GAM
- **site_name**: Nome do site
- **key**: Chave do parâmetro (utm_content)
- **value**: Valor do utm_content
- **impressions**: Impressões
- **clicks**: Cliques
- **ctr**: Taxa de cliques
- **revenue**: Receita
- **ecpm**: eCPM
- **match_rate**: Taxa de correspondência
- **imported_at**: Timestamp de importação

## 🚀 Sites Monitorados

- finanzco.com
- espacoextra.com.br
- vidadeproduto.com.br
- tecnologianocampo.com.br
- superinvestmentguide.com
- brasileirinho.blog.br
- bimviral.com
- onplif.com
- amigadamamae.com.br
- investimentoagora.com.br
- vamosestudar.com.br
- ifinane.com

## 📈 Destino dos Dados

**BigQuery Table:** `data-v1-423414.test.cloud_gam_adsperformance_historical`

## ⚙️ Configuração

- **Agendamento:** Diário às 06:30 BRT
- **API:** ActiveView External API
- **Autenticação:** Google Cloud Service Account
- **Modo:** WRITE_APPEND (adiciona dados)

## 🔧 Execução Manual

```bash
# Via GitHub Actions
# Actions → cloud_gam_adsperformance → Run workflow

# Local (desenvolvimento)
cd gam/cloud_gam_adsperformance
python main.py
```

## 📝 Logs

Monitore a execução em:
- **GitHub Actions:** Actions → cloud_gam_adsperformance
- **BigQuery:** Console → cloud_gam_adsperformance_historical

---
*Última atualização: 27/10/2025*
