# Configuração de Grupos do Facebook

Este arquivo explica como configurar os grupos e tokens do Facebook para as funções.

## 🔒 Segurança

**IMPORTANTE**: Os tokens do Facebook são informações sensíveis e não devem ser commitados no repositório.

## 📋 Opções de Configuração

### Opção 1: GitHub Secrets (Recomendado para Produção)

1. Acesse: `Settings → Secrets and variables → Actions → New repository secret`
2. Nome do secret: `SECRET_FACEBOOK_GROUPS_CONFIG`
3. Valor: Cole o conteúdo completo do arquivo `groups_config.json`
4. Salve o secret

**Vantagens:**
- ✅ Seguro: tokens não ficam no código
- ✅ Funciona automaticamente no GitHub Actions
- ✅ Fácil de atualizar

### Opção 2: Arquivo Local (Para Desenvolvimento)

1. Copie `groups_config.example.json` para `groups_config.json`
2. Preencha os tokens reais no arquivo
3. O arquivo `groups_config.json` está no `.gitignore` e não será commitado

**Vantagens:**
- ✅ Fácil para desenvolvimento local
- ✅ Não precisa configurar secrets

## 📝 Estrutura do Arquivo

O arquivo JSON deve ter a seguinte estrutura:

```json
{
  "grupo_nome": {
    "token": "SEU_TOKEN_DO_FACEBOOK",
    "accounts": [
      "act_123456789",
      "act_987654321"
    ],
    "account_names": [
      "Nome da Conta 1",
      "Nome da Conta 2"
    ]
  }
}
```

## 🔄 Como Funciona

O código tenta carregar a configuração nesta ordem:

1. **GitHub Secret** (`SECRET_FACEBOOK_GROUPS_CONFIG`) - se existir, usa este
2. **Arquivo local** (`facebook_ads/groups_config.json`) - fallback para desenvolvimento

## ⚠️ Importante

- O arquivo `groups_config.json` está no `.gitignore` e **NÃO deve ser commitado**
- Use sempre o GitHub Secret para produção
- O arquivo `groups_config.example.json` é apenas um template e pode ser commitado

## 🚀 Atualizando Tokens

### No GitHub Secret:
1. Vá em `Settings → Secrets and variables → Actions`
2. Edite `SECRET_FACEBOOK_GROUPS_CONFIG`
3. Cole o novo conteúdo do JSON
4. Salve

### No Arquivo Local:
1. Edite `facebook_ads/groups_config.json`
2. Atualize os tokens
3. Salve (não será commitado automaticamente)







