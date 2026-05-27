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

## Decisão — Onde tratar inconsistência de unidade de medida

Optamos por não normalizar na ingestão para preservar o dado bruto
na camada raw. A normalização de "R$/13Kg" e "R$/13kg" para um
padrão único será feita no SQL de staging (Fase 3), mantendo
rastreabilidade e facilitando manutenção futura.

## Fase 2 — Orquestração e validação

**Ferramenta:** Apache Airflow 3.1.7 em Docker local

**Decisão: TaskFlow API**
Usamos o decorator @task em vez da API clássica do Airflow.
Código mais limpo e dependências entre tasks definidas
implicitamente pelo retorno de funções.

**Decisão: parquet como formato intermediário**
Dataframes não trafegam entre tasks via XCom (limite de tamanho).
A task extrair salva em /tmp/anp_estados.parquet e passa só
o caminho. As tasks seguintes leem o arquivo pelo caminho.
Parquet preserva tipos (datas e floats chegam corretos).

**Decisão: tratar traços na task extrair**
O replace("-", None) foi movido para a task extrair antes do
to_parquet, porque o pyarrow não aceita strings em colunas
numéricas na conversão. O dado já entra limpo no parquet.

**Validação como portão**
Schema Pandera valida ESTADO, PRODUTO, UNIDADE DE MEDIDA,
DATA INICIAL e PREÇO MÉDIO REVENDA. Se qualquer regra falhar,
a task validar lança exceção e carregar nunca executa.
Testado com falha proposital: comportamento confirmado.

## Decisão de grão — fato_preco_estados

Grão: produto + estado + semana (DATA INICIAL)
Uma linha = estatísticas de preço de um produto em um estado
em uma semana específica.

As quatro perguntas de negócio são respondidas com esse grão:

- ranking por estado/produto → GROUP BY estado, produto
- evolução temporal → ORDER BY data_inicial
- margem → PREÇO MÉDIO REVENDA - PREÇO MÉDIO DISTRIBUIÇÃO
- anomalia → comparar com média regional/nacional

Grão de município foi descartado (fragmentação dos arquivos).
Pode entrar numa v2.


## Fase 3 — Staging e modelagem dimensional

**Camadas criadas:**

- staging.stg_precos_estados: dado limpo, tipos corrigidos,
  unidade normalizada com UPPER(), datas convertidas para DATE,
  colunas de distribuição convertidas com SAFE_CAST para NUMERIC.
- mart.dim_tempo: calendário diário contíguo de 2012-12-30 a
  2026-12-31, gerado com GENERATE_DATE_ARRAY + UNNEST.
  Necessário para time intelligence no Power BI.
- mart.fato_preco_estados: tabela fato com PARTITION BY
  data_inicial e CLUSTER BY estado, produto.

**Decisão: PARTITION BY + CLUSTER BY na fato**
Partição por data reduz custo de query no Power BI e sustenta
idempotência na Fase 4 (reprocessar uma semana sobrescreve
só a partição dela). Cluster por estado e produto acelera
os filtros mais comuns do relatório.

**Decisão: dim_tempo diária, não semanal**
Power BI exige tabela de datas contígua para time intelligence.
Tabela semanal criaria lacunas que quebram funções como
DATEADD e SAMEPERIODLASTYEAR.

## Fase 4 — Carga incremental e idempotência

**Decisão: MERGE na staging, CREATE OR REPLACE na fato**

A staging usa MERGE com chave data_inicial + estado + produto
(o grão da fato). Isso garante idempotência real: rodar o pipeline
duas vezes com o mesmo dado não gera duplicatas. O MERGE atualiza
linhas existentes (para absorver correções retroativas da ANP) e
insere apenas o que é novo.

A fato usa CREATE OR REPLACE porque é uma materialização direta
da staging, que já é idempotente. Recriar a fato a partir de uma
staging limpa sempre produz o mesmo resultado sem duplicatas.
O trade-off é custo: a cada execução toda a staging é relida para
recriar a fato, independente do volume do lote. Para o volume atual
(~112k linhas) essa troca é aceitável. Num volume maior o ideal
seria MERGE também na fato com a mesma chave de negócio.

**Decisão: MERGE atômico no controle de carga**

O controle de carga (raw.controle_carga) registra a última data
carregada para filtrar apenas dados novos na próxima execução.
Substituímos o DELETE+INSERT por um único MERGE com ON TRUE.
Motivo: DELETE+INSERT não é atômico — uma falha entre os dois
comandos deixa a tabela vazia, forçando reprocessamento completo
na próxima execução. O MERGE resolve isso em uma operação só.

**Decisão: fallback para primeira execução**

Se controle_carga não existir ou estiver vazia, a DAG usa
pd.Timestamp("2012-12-29") como data de referência, carregando
a série histórica completa. Isso permite que o pipeline rode do
zero sem configuração manual prévia.

**Decisão: AirflowSkipException para lote vazio**

Se não há dados novos após o filtro incremental, a DAG lança
AirflowSkipException em vez de falhar. Skip é a semântica correta:
"não havia nada para fazer" é diferente de "deu erro". O Airflow
propaga o skip automaticamente para todas as tasks dependentes.

**Decisão: create_bqstorage_client=False na leitura do controle**

O método .to_dataframe() do BigQuery usa por padrão a BigQuery
Storage API para leitura rápida, o que exige a permissão
bigquery.readsessions.create na service account. Como essa permissão
não estava concedida (princípio do menor privilégio), o fallback de
2012-12-29 era acionado a cada execução, carregando a série completa
repetidamente. A correção foi passar create_bqstorage_client=False,
forçando o uso da API REST padrão. Para uma query de 1 linha como
essa, não há diferença de performance.

**Decisão: ROW_NUMBER() para deduplicar o source do MERGE**

O raw usa WRITE_APPEND por design — é um log imutável de todas as
cargas. Em reprocessamentos ou testes, o raw pode acumular duplicatas
para a mesma chave data_inicial + estado + produto. O BigQuery exige
que cada linha do destino do MERGE corresponda a no máximo uma linha
do source, então sem deduplicação o MERGE falharia. A solução foi
envolver o SELECT do USING em uma subconsulta com ROW_NUMBER()
particionado pela chave de negócio, mantendo apenas a primeira
ocorrência de cada combinação. Isso foi validado em teste: raw com
224.158 linhas (duplicado), staging permaneceu em 112.079 (correto).
