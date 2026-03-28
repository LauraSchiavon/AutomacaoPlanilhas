---
description: Preencher Coluna TC da planilha criativa usando dados da API do Facebook
---

# Preencher Planilha - Teste de Criativos

## Objetivo
Preencher a **Coluna A** (`TC`) da aba `032026` no arquivo `FB - LOTTO V7.xlsx` com o identificador
do teste de criativo e o nome da conta (ex: `TC238 CA6.DIANA`), baseando-se nos nomes de anúncios
que o usuário inseriu manualmente na **Coluna B**.

## Inputs

| Parâmetro | Origem | Exemplo |
|---|---|---|
| account_id | Seleção do usuário no app | `act_123456789` |
| date_start | Seleção do usuário | `2026-02-01` |
| date_end | Seleção do usuário | `2026-03-03` |
| fb_token | Campo do painel lateral ou default hardcoded | `EAAW...` |

## Script de Execução

`execution/fill_creative_tests.py`

### Fluxo

1. Chama `FacebookAdsAPI.get_ad_insights()` com `level='ad'` para obter `campaign_name` + `ad_name` de todos os anúncios da conta no período.
2. Constrói um índice `{ad_name_lower → campaign_name}`.
3. Para cada linha da aba `032026` com valor na Coluna B (nome do anúncio):
   - Busca no índice qual `campaign_name` contém o nome do anúncio (filtro "contém", case-insensitive).
   - Extrai `TCxxx` usando regex `TC(\d+)`.
   - Extrai o nome da conta usando o padrão `]CONTA TC`.
   - Escreve `TCxxx CONTA` na Coluna A.
4. Mescla as células consecutivas da Coluna A que têm o mesmo valor (mesmo TC).
5. Salva o arquivo Excel.

## Formato do Nome de Campanha

```
[LOTTOV7]CA6.DIANA TC238 ABO 1-50-1 - LT801.30
            ↑            ↑                ↑
         Conta          TC             Anúncio
```

**Coluna A resultante:** `TC238 CA6.DIANA`

## Execução via CLI

```bash
python execution/fill_creative_tests.py \
  --account-id act_123456 \
  --date-start 2026-02-01 \
  --date-end 2026-03-03
```

## Execução via Web App

1. Selecionar a BM e a Conta de Anúncios no painel.
2. Definir o período de datas.
3. Preencher a **Coluna B** do Excel com os nomes dos anúncios.
4. Clicar em **"✍️ Preencher Coluna TC"** na seção "Preencher Planilha" da página.

## Notas e Edge Cases

- Se um anúncio não for encontrado na API, a Coluna A fica em branco e o nome do anúncio aparece no aviso de "Não encontrados".
- Se o Excel estiver aberto no Excel ao rodar o script, ele vai falhar ao salvar. Feche o arquivo antes.
- O período de datas deve cobrir quando os anúncios estiveram ativos.
