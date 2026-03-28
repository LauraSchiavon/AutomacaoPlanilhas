# Sistema de Importação Automática - Facebook Ads & RedTrack

## 📋 Visão Geral

Este sistema automatiza a importação de dados de anúncios do **Facebook Ads Manager** e dados de conversão do **RedTrack** para sua planilha Excel, organizando os dados brutos em uma aba separada com fórmulas de referência na aba principal.

## 🎯 O que foi entregue

### 1. **Scripts Python**
- `facebook_redtrack_importer_v2.py` - Script principal com seleção interativa de conta
- `configure_excel.py` - Configuração automática da planilha

### 2. **Documentação**
- `SETUP_GUIDE.md` - Guia completo de configuração
- `AUTOMATION_SETUP.md` - Instruções para automação diária
- `README.md` - Este arquivo

### 3. **Planilha Configurada**
- Nova aba "Dados Brutos" com headers formatados
- Estrutura pronta para receber dados do Facebook e RedTrack

## 🚀 Como Usar

### Passo 1: Instalar Dependências

```bash
pip install requests pandas openpyxl
```

### Passo 2: Executar o Script de Importação

```bash
python3 facebook_redtrack_importer_v2.py
```

O script irá:
1. ✅ Listar seus Business Managers
2. ✅ Permitir seleção do BM desejado
3. ✅ Listar contas de anúncios disponíveis
4. ✅ Permitir seleção da conta
5. ✅ Buscar dados dos últimos 30 dias
6. ✅ Importar dados para a aba "Dados Brutos"
7. ✅ Salvar a planilha

### Passo 3: Adicionar Fórmulas na Aba Principal

Na aba "032026", adicione as seguintes fórmulas nas colunas correspondentes:

#### Para dados do Facebook Ads:

**CPM (Coluna G):**
```excel
=VLOOKUP(B4, 'Dados Brutos'!A:L, 10, FALSE)
```

**CTR (Coluna H):**
```excel
=VLOOKUP(B4, 'Dados Brutos'!A:L, 11, FALSE)
```

**CPC (Coluna I):**
```excel
=VLOOKUP(B4, 'Dados Brutos'!A:L, 12, FALSE)
```

**GASTO (Coluna J):**
```excel
=VLOOKUP(B4, 'Dados Brutos'!A:L, 9, FALSE)
```

**HOOK VIEW 3" (Coluna E):**
```excel
=VLOOKUP(B4, 'Dados Brutos'!A:L, 7, FALSE)
```

**Body View 75% (Coluna F):**
```excel
=VLOOKUP(B4, 'Dados Brutos'!A:L, 8, FALSE)
```

#### Para dados do RedTrack:

**VENDAS (Coluna K):**
```excel
=SUMIF('Dados Brutos'!N:N, B4, 'Dados Brutos'!O:O)
```

**CPA (Coluna L):**
```excel
=IFERROR(VLOOKUP(B4, 'Dados Brutos'!M:P, 4, FALSE), 0)
```

## 📊 Estrutura da Aba "Dados Brutos"

### Colunas A-L: Dados do Facebook Ads

| Col | Campo | Descrição |
|-----|-------|-----------|
| A | campaign_id | ID da Campanha |
| B | campaign_name | Nome da Campanha |
| C | adset_id | ID do Conjunto de Anúncios |
| D | adset_name | Nome do Conjunto |
| E | ad_id | ID do Anúncio |
| F | ad_name | Nome do Anúncio |
| G | impressions | Impressões (Hook View 3") |
| H | clicks | Cliques (Body View 75%) |
| I | spend | Gasto Total |
| J | cpm | Custo por Mil (CPM) |
| K | ctr | Taxa de Cliques (CTR) |
| L | cpc | Custo por Clique (CPC) |

### Colunas N-Q: Dados do RedTrack

| Col | Campo | Descrição |
|-----|-------|-----------|
| N | conversion_id | ID da Conversão |
| O | campaign_id | ID da Campanha |
| P | revenue | Receita (Vendas) |
| Q | cpa | Custo por Aquisição (CPA) |

## 🔄 Automação Diária

Para que os dados sejam atualizados automaticamente todos os dias:

### Windows - Task Scheduler

1. Crie um arquivo `run_import.bat`:
```batch
@echo off
cd /d "C:\caminho\para\pasta"
python3 facebook_redtrack_importer_v2.py >> import_log.txt 2>&1
```

2. Abra Task Scheduler (Win + R → `taskschd.msc`)
3. Crie uma nova tarefa básica
4. Configure para rodar diariamente às 2:00 AM
5. Aponte para o arquivo `run_import.bat`

### macOS/Linux - Cron

1. Crie um arquivo `run_import.sh`:
```bash
#!/bin/bash
cd /caminho/para/pasta
python3 facebook_redtrack_importer_v2.py >> import_log.txt 2>&1
```

2. Torne executável: `chmod +x run_import.sh`
3. Edite crontab: `crontab -e`
4. Adicione a linha:
```
0 2 * * * /caminho/para/pasta/run_import.sh
```

## ⚠️ Pontos Importantes

### Credenciais Armazenadas
As credenciais estão armazenadas no script. Para maior segurança, considere:
- Usar variáveis de ambiente
- Usar um arquivo de configuração separado
- Rotacionar tokens regularmente

### Limites de API
- **Facebook:** 200 requisições/hora
- **RedTrack:** Verifique seu plano

### Antes de Executar
- ✅ Feche o arquivo Excel
- ✅ Verifique conexão com internet
- ✅ Confirme que as credenciais estão corretas

## 🔧 Troubleshooting

### "No business managers found"
→ Verifique se o token do Facebook é válido e tem permissões necessárias

### "No ad accounts found"
→ Certifique-se de que o Business Manager selecionado possui contas de anúncios

### "Error fetching conversions"
→ Verifique se a API Key do RedTrack está correta

### "Excel file is locked"
→ Feche o arquivo Excel antes de executar o script

## 📞 Suporte

Para dúvidas ou problemas:
1. Consulte `SETUP_GUIDE.md` para configuração detalhada
2. Consulte `AUTOMATION_SETUP.md` para automação
3. Verifique a documentação das APIs:
   - Facebook: https://developers.facebook.com/docs/marketing-api
   - RedTrack: https://api.redtrack.io/docs/index.html

## 📝 Próximos Passos

1. ✅ Instale as dependências Python
2. ✅ Execute o script de importação
3. ✅ Verifique se os dados aparecem em "Dados Brutos"
4. ✅ Adicione as fórmulas na aba principal
5. ✅ Configure a automação diária (opcional)

## 🎉 Pronto!

Seu sistema de importação automática está configurado e pronto para uso!

---

**Criado em:** 02 de Março de 2026
**Versão:** 2.0
