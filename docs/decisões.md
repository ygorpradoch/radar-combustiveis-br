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
