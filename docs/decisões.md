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

## Reposicionamento de escopo — Produtos filtrados

**Decisão: manter apenas combustíveis vendidos em postos de abastecimento**

O dataset original da ANP contém 7 produtos:
- GASOLINA COMUM, GASOLINA ADITIVADA, ETANOL HIDRATADO, OLEO DIESEL, OLEO DIESEL S10 → vendidos em postos
- GLP (R$/13kg — botijão de gás de cozinha) → excluído
- GNV (R$/m³ — gás natural veicular) → excluído

Motivo: o projeto "Radar Combustíveis BR" monitora preços em postos de
abastecimento. GLP e GNV possuem unidades incomparáveis com os demais
(R$/13kg e R$/m³ vs R$/l), distorciam KPIs e tornavam visuais ilegíveis
(GLP ~R$100 comprimia todas as outras séries no gráfico de evolução).

Os 5 produtos mantidos compartilham a mesma unidade (R$/l), tornando
comparações entre produtos e entre estados matematicamente válidas.

## Decisão — Task `tratar` (separação de responsabilidades)

Criamos `scripts/tratar.py` com uma task dedicada ao tratamento de dados,
separando responsabilidades que estavam misturadas no `extrair.py`:

- `extrair`: ler Excel, filtrar por data, salvar parquet — só extração
- `tratar`: substituir traços por None, filtrar produtos válidos — só tratamento

Cadeia do DAG após refatoração:
  extrair → tratar → validar → carregar → atualizar_staging → atualizar_mart

O filtro de produtos usa df[df["PRODUTO"].isin(PRODUTOS_POSTO)],
equivalente ao WHERE PRODUTO IN (...) do SQL. A lista PRODUTOS_POSTO
é centralizada no topo do tratar.py, fácil de manter em numa v2.

**Impacto na validação:**
Com GLP e GNV removidos antes da validação, a lista de unidades válidas
reduziu de 5 variantes para 1: ['R$/l']. A validação ficou mais forte —
qualquer unidade diferente de R$/l é capturada como erro real.

## Fase 5 — Power BI: modelagem e DAX

**Decisão: Importar vs DirectQuery**

Escolhemos o modo Importar (cópia local dos dados) em vez de
DirectQuery (queries em tempo real no BigQuery). Motivos: volume
pequeno (~112k linhas) cabe na memória do Power BI; dado é semanal
(não tempo real); visuais respondem instantaneamente sem custo de
query a cada interação. DirectQuery faria sentido para dados em
tempo real ou volumes que não cabem em memória.

**Decisão: tabelas carregadas no modelo**

Carregadas: mart.fato_preco_estados e mart.dim_tempo.
A staging não foi carregada porque a fato já contém todos os seus
dados — carregar as duas seria redundância. O Power BI consome a
camada mart, não staging. A controle_carga foi carregada como
referência opcional mas não participa do modelo dimensional.

**Relacionamento do modelo estrela**

dim_tempo[data] → fato_preco_estados[data_inicial]
Cardinalidade: um para muitos (1:*)
Direção do filtro: único (dim_tempo filtra a fato)
dim_tempo marcada como Tabela de Datas — obrigatório para funções
de time intelligence DAX (DATEADD, SAMEPERIODLASTYEAR, etc.)

**Medidas DAX criadas — o que são e por que essas**

DAX (Data Analysis Expressions) é a linguagem de fórmulas do Power
BI. Medidas são cálculos reutilizáveis que se adaptam ao contexto
do visual (filtros, slicers, drill-down). São preferíveis a colunas
calculadas porque não ocupam memória — são calculadas sob demanda.

As 7 medidas criadas respondem às 4 perguntas de negócio do projeto:

1. Preco Medio Revenda
   = AVERAGE(fato_preco_estados[preco_medio_revenda])
   Base de todos os outros cálculos. Média do preço de revenda
   no contexto do visual (estado, produto, período selecionado).

2. Preco Semana Anterior
   = CALCULATE([Preco Medio Revenda], DATEADD(dim_tempo[data], -7, DAY))
   Desloca o contexto de data 7 dias para trás. CALCULATE reexecuta
   uma medida com filtros diferentes — aqui muda o período avaliado.
   Exige dim_tempo marcada como tabela de datas para funcionar.

3. Variacao Semanal %
   = VAR atual = [Preco Medio Revenda]
     VAR anterior = [Preco Semana Anterior]
     RETURN DIVIDE(atual - anterior, anterior)
   Variação percentual semana a semana. VAR guarda valores
   intermediários para reutilização. DIVIDE é divisão segura —
   retorna BLANK() se denominador for zero, nunca erro.

4. Ranking UF
   = RANKX(ALL(fato_preco_estados[estado]), [Preco Medio Revenda], , DESC, Dense)
   Classifica estados por preço médio. ALL() remove o filtro de
   estado para que o ranking considere sempre todos os 27 estados,
   mesmo quando o visual está filtrado. Dense: empates recebem o
   mesmo número sem pular posições.

5. Media Nacional
   = CALCULATE([Preco Medio Revenda], ALL(fato_preco_estados[estado]))
   Preço médio ignorando o filtro de estado — sempre calcula sobre
   todos os estados. Base para comparações regionais.

6. Desvio Padrao Preco
   = STDEVX.P(fato_preco_estados, fato_preco_estados[preco_medio_revenda])
   Dispersão dos preços entre os registros do contexto atual.
   STDEVX.P itera linha por linha da tabela (padrão funções X do DAX)
   e calcula desvio padrão populacional. Quantifica a tese central
   do projeto: existe dispersão real de preços entre estados.

7. Pct Acima Media Nacional
   = VAR media = [Media Nacional]
     RETURN DIVIDE(
       CALCULATE(COUNTROWS(fato_preco_estados),
         FILTER(fato_preco_estados, fato_preco_estados[preco_medio_revenda] > media)),
       COUNTROWS(fato_preco_estados))
   Percentual de registros com preço acima da média nacional.
   VAR captura o valor da medida antes do FILTER — necessário porque
   medidas não podem ser usadas diretamente como filtro no CALCULATE
   (erro: "PLACEHOLDER em expressão True/False"). FILTER itera a
   tabela e aceita variáveis na condição.

## Fase 5 — Design dos dashboards

**Princípio adotado:** cada visual responde uma pergunta que nenhum outro
visual na página responde. Visuais redundantes foram eliminados.

**Página Executiva — estrutura final**

Objetivo: snapshot em 5 segundos ("quanto custa, subiu ou desceu?").

- KPI Preço Médio Revenda — quanto custa?
- KPI Variação Semanal % — subiu ou desceu? (formatação verde/vermelho)
- KPI Dispersão entre estados — quão desigual é o preço entre estados?
- Barras Top 5 estados mais caros — preview geográfico sem duplicar a analítica
- Linha Evolução temporal — qual a tendência? (única dimensão de tempo)
- Cartão Última Atualização — referência temporal dos dados
- Slicer produto (seleção única) + Slicer data

Removido: gráfico "Variação semanal por produto" — redundante com o KPI
de variação quando apenas um produto está selecionado.

**Página Analítica — estrutura final**

Objetivo: investigar padrões entre estados, regiões e produtos.

- Ranking UF (barras + linha referência Média Nacional) — quem paga mais vs média?
- Evolução por REGIÃO (5 linhas: Norte, Nordeste, Centro-Oeste, Sudeste, Sul)
  → o gap regional está crescendo? Dimensão tempo + geografia, não existia na executiva.
  → Norte sistematicamente mais caro por logística amazônica (confirmado nos dados).
- Variação semanal por produto — qual combustível é mais volátil?
  → Movido da executiva onde não fazia sentido com seleção única de produto.
- KPI Dispersão + KPI % Acima da Média — quantificam a tese central do projeto
- Slicers: produto (multi-seleção), data, estado

Removido: mapa coroplético — mostrava Preço Médio × estado com encoding de cor,
mesma informação do ranking de barras com encoding de comprimento. Barras são
mais precisas para comparação quantitativa. O padrão geográfico (Norte mais caro)
é capturado pelo gráfico de Evolução por Região com mais contexto temporal.
