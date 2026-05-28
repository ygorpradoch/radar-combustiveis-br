## Decisão de grão — fonte principal

Usei o arquivo de estados (granularidade UF/semana/produto) como fonte da tabela fato. O arquivo de municípios foi descartado na Fase 1: os dados estão fragmentados em vários arquivos e não respondem às perguntas de negócio que defini. Pode entrar numa v2.

## Inspeção do arquivo ANP — semanal-estados-desde-2013.xlsx

Formato: Excel (.xlsx), cabeçalho real na linha 18. As linhas 1-17 são metadados da ANP.

Volume: 112.079 linhas × 18 colunas.

O que já vem limpo:
- Datas como datetime64, sem conversão
- Preços de revenda como float64
- REGIÃO, ESTADO e PRODUTO sem nulos

Problemas encontrados:

**1. Nulos disfarçados de traço.** Cerca de 54.833 ocorrências do valor `"-"` nas colunas de distribuição (margem, preço médio de distribuição, desvio padrão). A ANP preencheu as células vazias com traço em vez de deixá-las em branco. O pandas lê a coluna inteira como `object` por causa disso.
Tratamento: converter `"-"` para `None` antes de salvar o parquet.

**2. Colunas de distribuição como object.** Consequência direta dos traços. Com `"-"` na coluna, o pandas não infere float64.
Tratamento: depois de converter os traços, usar `SAFE_CAST` para `NUMERIC` no staging.

**3. Inconsistência de grafia na unidade de medida.**
- GLP: `"R$/13Kg"` (10.661 linhas) e `"R$/13kg"` (7.902 linhas)
- GNV: `"R$/m3"` (6.615 linhas) e `"R$/m³"` (4.946 linhas)
Tratamento: normalizar com `UPPER()` no staging.

**4. Produtos com unidades incomparáveis.**
- Combustíveis líquidos: R$/l
- GLP: R$/13Kg (botijão)
- GNV: R$/m³

Decisão: não comparar preços entre produtos de unidades diferentes. Médias nacionais sempre dentro do mesmo produto.

## Decisão — onde normalizar a unidade de medida

Não normalizei na ingestão para preservar o dado bruto no raw. A normalização de `"R$/13Kg"` e `"R$/13kg"` para um padrão único fica no SQL de staging (Fase 3). Assim dá para rastrear o que veio da fonte original.

## Fase 2 — orquestração e validação

Ferramenta: Apache Airflow 3.1.7 em Docker local.

**TaskFlow API.** Usei o decorator `@task` em vez da API clássica. As dependências entre tasks ficam implícitas pelo retorno das funções, sem precisar declarar `set_downstream`.

**Parquet como formato intermediário.** Dataframes não passam entre tasks via XCom por causa do limite de tamanho. A task `extrair` salva em `/tmp/anp_estados.parquet` e passa só o caminho. As tasks seguintes leem o arquivo pelo caminho. Parquet preserva tipos, então datas e floats chegam corretos.

**Tratar traços na task extrair (decisão inicial).** O `replace("-", None)` precisou ir para antes do `to_parquet` porque o pyarrow não aceita strings em colunas numéricas na conversão. Isso foi depois refatorado para uma task separada (ver seção "Task tratar").

**Validação como portão.** O schema Pandera valida ESTADO, PRODUTO, UNIDADE DE MEDIDA, DATA INICIAL e PREÇO MÉDIO REVENDA. Se qualquer regra falhar, a task `validar` lança exceção e `carregar` nunca executa. Testei com falha proposital e o comportamento foi confirmado.

## Decisão de grão — fato_preco_estados

Grão: produto + estado + semana (DATA INICIAL). Uma linha representa as estatísticas de preço de um produto em um estado em uma semana.

As quatro perguntas de negócio são respondidas com esse grão:
- ranking por estado/produto: `GROUP BY estado, produto`
- evolução temporal: `ORDER BY data_inicial`
- margem: `PREÇO MÉDIO REVENDA - PREÇO MÉDIO DISTRIBUIÇÃO`
- anomalia: comparar com média regional/nacional

Grão de município descartado por causa da fragmentação dos arquivos. Pode entrar numa v2.

## Fase 3 — staging e modelagem dimensional

Camadas criadas:

- `staging.stg_precos_estados`: dado limpo com tipos corrigidos, unidade normalizada com `UPPER()`, datas convertidas para DATE, colunas de distribuição convertidas com `SAFE_CAST` para NUMERIC.
- `mart.dim_tempo`: calendário diário contíguo de 2012-12-30 a 2026-12-31, gerado com `GENERATE_DATE_ARRAY + UNNEST`. Precisa ser diário para o Power BI funcionar (ver abaixo).
- `mart.fato_preco_estados`: tabela fato com `PARTITION BY data_inicial` e `CLUSTER BY estado, produto`.

**PARTITION BY + CLUSTER BY na fato.** Partição por data reduz o custo de cada query no Power BI porque o BigQuery lê só as partições necessárias. Cluster por estado e produto acelera os filtros mais comuns do relatório.

**dim_tempo diária, não semanal.** O Power BI exige tabela de datas contígua para funções de time intelligence. Uma tabela só com as datas semanais da ANP teria lacunas que quebram `DATEADD` e `SAMEPERIODLASTYEAR`.

## Fase 4 — carga incremental e idempotência

**MERGE na staging, CREATE OR REPLACE na fato.**

A staging usa `MERGE` com chave `data_inicial + estado + produto` (o grão da fato). Rodar o pipeline duas vezes com o mesmo dado não gera duplicatas. O `MERGE` atualiza linhas existentes (para absorver correções retroativas da ANP) e insere só o que é novo.

A fato usa `CREATE OR REPLACE` porque é uma materialização direta da staging, que já é idempotente. Recriar a fato a partir de uma staging limpa sempre produz o mesmo resultado. O trade-off é custo: a cada execução toda a staging é relida. Para 112k linhas isso é aceitável. Em volumes maiores o ideal seria `MERGE` na fato também.

**MERGE atômico no controle de carga.**

O controle de carga (`raw.controle_carga`) registra a última data carregada. Troquei `DELETE + INSERT` por um único `MERGE ON TRUE`. O motivo: `DELETE + INSERT` não é atômico. Se falha entre os dois comandos, a tabela fica vazia e o pipeline recarrega a série completa na próxima execução. O `MERGE` faz tudo em uma operação.

**Fallback para primeira execução.**

Se `controle_carga` não existe ou está vazia, a DAG usa `pd.Timestamp("2012-12-29")` como data de referência e carrega a série histórica completa. O pipeline roda do zero sem configuração manual.

**AirflowSkipException para lote vazio.**

Se não há dados novos depois do filtro incremental, a DAG lança `AirflowSkipException` em vez de falhar. "Não havia nada para fazer" é diferente de "deu erro". O Airflow propaga o skip para todas as tasks dependentes automaticamente.

**create_bqstorage_client=False na leitura do controle.**

O `.to_dataframe()` do BigQuery usa por padrão a BigQuery Storage API, que exige a permissão `bigquery.readsessions.create` na service account. Essa permissão não estava concedida, então o fallback de 2012 era ativado a cada execução e recarregava as 112k linhas inteiras. A correção foi passar `create_bqstorage_client=False`. Para uma query de 1 linha, a API REST padrão é suficiente.

**ROW_NUMBER() para deduplicar o source do MERGE.**

O raw usa `WRITE_APPEND` por design: é um log de todas as cargas. Em reprocessamentos, o raw acumula duplicatas para a mesma chave `data_inicial + estado + produto`. O BigQuery exige que cada linha do destino do `MERGE` corresponda a no máximo uma linha do source. Sem deduplicação, o `MERGE` falha.

A solução foi envolver o `SELECT` do `USING` em uma subconsulta com `ROW_NUMBER()` particionado pela chave de negócio, mantendo só a primeira ocorrência. Validado em teste: raw com 224.158 linhas (duplicado intencional), staging ficou em 112.079.

## Reposicionamento de escopo — produtos filtrados

O dataset original tem 7 produtos:
- GASOLINA COMUM, GASOLINA ADITIVADA, ETANOL HIDRATADO, OLEO DIESEL, OLEO DIESEL S10 (postos de combustível)
- GLP (R$/13kg, botijão de gás de cozinha) — excluído
- GNV (R$/m³, gás natural veicular) — excluído

O projeto monitora preços em postos de abastecimento. GLP e GNV têm unidades incomparáveis com os demais. Na prática, o GLP em torno de R$100 por botijão comprimia todas as séries de R$5-7 no gráfico de evolução, tornando o visual ilegível.

Os 5 produtos mantidos compartilham a mesma unidade (R$/l), então as comparações entre produtos e entre estados fazem sentido.

## Decisão — task `tratar` (separação de responsabilidades)

Criei `scripts/tratamento.py` com uma task dedicada ao tratamento, separando o que estava misturado no `extrair.py`:

- `extrair`: ler o Excel, filtrar por data, salvar parquet
- `tratar`: substituir traços por None, filtrar produtos válidos

Cadeia do DAG depois da refatoração:
```
extrair → tratar → validar → carregar → atualizar_staging → atualizar_mart
```

O filtro de produtos usa `df[df["PRODUTO"].isin(PRODUTOS_POSTO)]`. A lista `PRODUTOS_POSTO` fica no topo do `tratamento.py`.

Com GLP e GNV removidos antes da validação, a lista de unidades aceitas pelo Pandera reduziu de 5 variantes para 1: `['R$/l']`. Qualquer unidade diferente agora é um erro real, não um produto fora do escopo.

## Fase 5 — Power BI: modelagem e DAX

**Importar vs DirectQuery.** Escolhi o modo Importar. O volume de 112k linhas cabe na memória, o dado é semanal (não precisa de tempo real) e os visuais respondem sem latência. DirectQuery faria sentido para dados em tempo real ou volumes que não cabem em memória.

**Tabelas carregadas.** `mart.fato_preco_estados` e `mart.dim_tempo`. A staging não foi carregada porque a fato já tem todos os seus dados. A `controle_carga` foi carregada como referência mas não participa do modelo.

**Relacionamento do modelo estrela.**

`dim_tempo[data]` → `fato_preco_estados[data_inicial]`
Cardinalidade: um para muitos (1:*). Direção do filtro: única (dim_tempo filtra a fato). `dim_tempo` marcada como Tabela de Datas para que as funções de time intelligence funcionem.

**Medidas DAX criadas.**

DAX (Data Analysis Expressions) é a linguagem de fórmulas do Power BI. Medidas são cálculos que se adaptam ao contexto do visual. São preferíveis a colunas calculadas porque não ocupam memória, são calculadas sob demanda.

As 7 medidas respondem às 4 perguntas de negócio:

1. `Preco Medio Revenda`
   `= AVERAGE(fato_preco_estados[preco_medio_revenda])`
   Base de todos os outros cálculos. Média do preço no contexto atual do visual.

2. `Preco Semana Anterior`
   `= CALCULATE([Preco Medio Revenda], DATEADD(dim_tempo[data], -7, DAY))`
   Desloca o contexto de data 7 dias para trás. Exige `dim_tempo` marcada como tabela de datas.

3. `Variacao Semanal %`
   ```
   = VAR atual = [Preco Medio Revenda]
     VAR anterior = [Preco Semana Anterior]
     RETURN DIVIDE(atual - anterior, anterior)
   ```
   `VAR` guarda valores intermediários. `DIVIDE` retorna `BLANK()` se o denominador for zero, em vez de erro.

4. `Ranking UF`
   `= RANKX(ALL(fato_preco_estados[estado]), [Preco Medio Revenda], , DESC, Dense)`
   `ALL()` remove o filtro de estado para que o ranking considere os 27 estados mesmo quando o visual está filtrado. Dense: empates recebem o mesmo número sem pular posições.

5. `Media Nacional`
   `= CALCULATE([Preco Medio Revenda], ALL(fato_preco_estados[estado]))`
   Preço médio ignorando o filtro de estado. Usado como linha de referência nos visuais.

6. `Desvio Padrao Preco`
   `= STDEVX.P(fato_preco_estados, fato_preco_estados[preco_medio_revenda])`
   Dispersão dos preços no contexto atual. `STDEVX.P` itera linha por linha (padrão das funções X do DAX).

7. `Pct Acima Media Nacional`
   ```
   = VAR media = [Media Nacional]
     RETURN DIVIDE(
       CALCULATE(COUNTROWS(fato_preco_estados),
         FILTER(fato_preco_estados, fato_preco_estados[preco_medio_revenda] > media)),
       COUNTROWS(fato_preco_estados))
   ```
   `VAR` captura o valor da medida antes do `FILTER`. Sem isso, o `CALCULATE` retorna erro porque medidas não podem ser usadas diretamente como condição de filtro.

## Fase 5 — design dos dashboards

Princípio: cada visual responde uma pergunta que nenhum outro visual na mesma página responde. Se dois visuais mostram a mesma coisa, um sai.

**Página Executiva — estrutura final**

Objetivo: entender o cenário em 5 segundos.

- KPI Preço Médio Revenda
- KPI Variação Semanal % (formatação condicional verde/vermelho)
- KPI Dispersão entre estados
- Barras Top 5 estados mais caros
- Linha de evolução temporal
- Cartão última atualização
- Slicer produto (seleção única) + slicer data

Removido: gráfico "Variação semanal por produto". Com slicer de produto em seleção única, o gráfico mostrava a variação de um só produto, mesma informação do KPI de variação semanal.

**Página Analítica — estrutura final**

Objetivo: investigar padrões entre estados, regiões e produtos.

- Ranking UF (barras horizontais + linha de referência Média Nacional)
- Evolução por região (5 linhas: Norte, Nordeste, Centro-Oeste, Sudeste, Sul). O gap regional está crescendo? Essa dimensão de tempo + geografia não existe na executiva.
- Variação semanal por produto. Qual combustível oscila mais? Movido da executiva, onde não fazia sentido com seleção única.
- KPI Dispersão + KPI % Acima da Média
- Slicers: produto (multi-seleção), data, estado

Removido: mapa coroplético. Mostrava Preço Médio por estado com encoding de cor, mesma informação do ranking de barras com encoding de comprimento. Barras são mais precisas para comparação quantitativa.

27 estados em um gráfico de linhas produz 27 séries ilegíveis. 5 regiões mostram o padrão macro sem ruído: Norte sistematicamente mais caro, gap se ampliando depois de 2022.
