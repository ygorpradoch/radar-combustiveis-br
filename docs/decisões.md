## Decisão de grão — Fonte principal

Escolhemos o arquivo de Estados (granularidade UF/semana/produto)
como fonte principal da fato. Municípios foram descartados na Fase 1
por estarem fragmentados em múltiplos arquivos e não agregarem valor
às perguntas de negócio definidas. Podem entrar numa v2.

## Inspeção do arquivo ANP — semanal-estados-desde-2013.xlsx

******Formato:****** Excel (.xlsx), cabeçalho real na linha 18
(linhas 1-17 são metadados descritivos da ANP)

******Volume:****** 112.079 linhas × 18 colunas

******O que está limpo:******
**-** Datas já vêm como datetime64, sem conversão necessária
**-** Preços de revenda (PREÇO MÉDIO REVENDA) vêm como float64
**-** REGIÃO, ESTADO e PRODUTO sem nulos

******Problemas encontrados e decisões de tratamento:******

**1.** Nulos disfarçados: ~54.833 ocorrências do valor "-" nas colunas
   de distribuição (MARGEM, PREÇO MÉDIO, DESVIO PADRÃO, etc).
   Motivo: ANP substituiu nulos por traço na planilha.
   Tratamento: converter "-" para NULL no staging.

**2.** Colunas de distribuição como object: consequência direta
   dos traços. Com "-" na coluna, pandas não consegue inferir
   float64. Tratamento: após converter "-" para NULL, castear
   para NUMERIC no staging.

**3.** Inconsistência de grafia na unidade de medida:
**-** GLP: "R$/13Kg" (10.661 linhas) e "R$/13kg" (7.902 linhas)
**-** GNV: "R$/m3" (6.615 linhas) e "R$/m³" (4.946 linhas)
   Tratamento: normalizar para uppercase no staging.

**4.** Produtos com unidades incomparáveis entre si:
**-** Combustíveis líquidos: R$/l
**-** GLP: R$/13Kg (botijão)
**-** GNV: R$/m³
   Decisão: nunca comparar preços entre produtos diferentes
   sem filtrar por unidade de medida. Médias nacionais
   serão sempre calculadas dentro do mesmo produto.
