# Radar de Combustíveis BR

> **Pipeline de dados ponta a ponta:** da ingestão de dados públicos da ANP à decisão de negócio em Power BI, orquestrado com Airflow sobre GCP.
>
> `Airflow` · `Python` · `Validação de dados` · `BigQuery` · `SQL` · `Power BI / DAX` · `GCP`

**Autor:** Ygor Prado · **GitHub:** [github.com/ygorpradoch](https://github.com/ygorpradoch)

---

## Sumário

* [1. Visão geral](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#1-vis%C3%A3o-geral)
  * [1.1. Arquitetura de referência](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#11-arquitetura-de-refer%C3%AAncia)
  * [1.2. Decisão de infraestrutura e custo](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#12-decis%C3%A3o-de-infraestrutura-e-custo)
* [2. Stack e pré-requisitos](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#2-stack-e-pr%C3%A9-requisitos)
* [3. Como usar este documento](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#3-como-usar-este-documento)
* [Fase 0 — Setup do ambiente](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#fase-0--setup-do-ambiente)
* [Fase 1 — Ingestão manual (o caminho feliz)](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#fase-1--ingest%C3%A3o-manual-o-caminho-feliz)
* [Fase 2 — Orquestração e validação](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#fase-2--orquestra%C3%A7%C3%A3o-e-valida%C3%A7%C3%A3o)
* [Fase 3 — Staging e modelagem dimensional](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#fase-3--staging-e-modelagem-dimensional)
* [Fase 4 — Carga incremental e idempotência](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#fase-4--carga-incremental-e-idempot%C3%AAncia)
* [Fase 5 — Power BI: modelagem e DAX avançado](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#fase-5--power-bi-modelagem-e-dax-avan%C3%A7ado)
* [Fase 6 — Documentação, GitHub e portfólio](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#fase-6--documenta%C3%A7%C3%A3o-github-e-portf%C3%B3lio)
* [7. Checklist mestre de captura](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#7-checklist-mestre-de-captura)
* [Apêndice A — dbt (opcional)](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#ap%C3%AAndice-a--dbt-opcional)
* [Apêndice B — Terraform (opcional)](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#ap%C3%AAndice-b--terraform-opcional)
* [Próximos passos](https://claude.ai/chat/0c31fad3-acf4-4d75-80e8-4eba01a7372f#pr%C3%B3ximos-passos)

---

## 1. Visão geral

**O que é.** Um pipeline de dados completo que ingere a Série Histórica de Preços de Combustíveis da ANP (dado público, real, atualizado semanalmente), garante a confiabilidade desse dado com validação automatizada, modela em esquema dimensional dentro do BigQuery e entrega um relatório analítico avançado em Power BI. Tudo costurado por uma orquestração em Apache Airflow.

**A tese de negócio.** Existe dispersão real e grande de preço de combustível entre estados, municípios e bandeiras. Quem opera frota, transportadora, ou administra custos recorrentes (incluindo gestão predial com gerador e contratos de abastecimento) precisa saber onde está mais barato, como o preço evolui, qual a margem entre distribuição e revenda, e onde há anomalia. O relatório responde a uma pergunta de compra e benchmarking, não é só um painel bonito.

**Por que este projeto carrega bem um portfólio.** Ele conta uma história de ponta a ponta (ingestão → confiabilidade → modelagem → decisão). Recrutador de dados não se impressiona com o número de ferramentas, e sim com a coerência do arco e com a evidência de que você tomou decisões de engenharia conscientes. Aqui você terá três delas para mostrar: idempotência, carga incremental e qualidade como portão de bloqueio.

> **💡 Por que isso importa**
> O dado da ANP tem schema que mudou ao longo dos anos (encoding, separador, nomes de coluna). Lidar com isso é evolução de schema de verdade, não exercício de tutorial. É exatamente o tipo de problema que separa um projeto real de um "hello world" de pipeline.

### 1.1. Arquitetura de referência

O caminho que o dado percorre, na ordem em que ele anda. Cada bloco corresponde a um requisito que você levantou:

```text
  ANP (CSV semanal, público)
        │   ① Python: extração incremental
        ▼
  GCS  ── camada raw (data lake, particionada por ano/semana)
        │   ② Python: validação (Pandera / Great Expectations)
        ▼
  BigQuery ── raw → staging → marts (modelo estrela)
        │   ③ SQL: limpeza, padronização e modelagem dimensional
        ▼
  Power BI ── modelo estrela + medidas DAX → relatório de decisão

  [ Apache Airflow em Docker local orquestra ①→②→③ ]
```

| Camada      | Onde vive                            | O que faz                                                                   |
| :---------- | :----------------------------------- | :-------------------------------------------------------------------------- |
| Ingestão   | Python (script chamado pelo Airflow) | Baixa o CSV da ANP e grava o bruto intocado no GCS.                         |
| Raw         | Cloud Storage + BigQuery             | Espelho fiel da fonte. Nada é tratado. Permite reprocessar sem reextrair.  |
| Validação | Python (Pandera/GE)                  | Portão de qualidade. Se falha, a DAG falha. Confiabilidade explícita.     |
| Staging     | BigQuery (SQL)                       | Tipagem, datas, padronização de texto, unificação de schema entre anos. |
| Marts       | BigQuery (SQL)                       | Modelo dimensional (fato + dimensões) pronto para consumo analítico.      |
| Consumo     | Power BI + DAX                       | Modelo estrela, medidas avançadas, relatório que responde a decisão.     |

> *Slot de imagem:* exporte este diagrama para `docs/arquitetura.png` e use-o no topo do README.

### 1.2. Decisão de infraestrutura e custo

Você optou por rodar o Airflow localmente em Docker e usar o GCP apenas como destino dos dados. Essa é a escolha certa para preservar seus créditos.

> **💡 Por que isso importa**
>
> * **Airflow gerenciado (Cloud Composer) é caro:** ele cobra por hora de ambiente mesmo ocioso e pode consumir boa parte do crédito de US$ 300 em poucas semanas. Rodando local, esse custo é zero.
> * **GCS e BigQuery são baratíssimos para este volume:** o dataset inteiro tem poucos GB; o BigQuery oferece 1 TB de consulta grátis por mês e o armazenamento custa centavos. Seus créditos sobram quase intactos.
> * **Decisão a registrar:** "Airflow local vs Composer" é o seu primeiro registro de decisão. Anote o trade-off (custo × fidelidade ao ambiente de produção). Isso vira parágrafo no post de portfólio.

---

## 2. Stack e pré-requisitos

Este documento assume que você está começando do zero em cada ferramenta. Cada fase traz o setup necessário. Visão geral do que será usado:

| Camada          | Ferramenta                    | Para que serve no projeto                                     |
| :-------------- | :---------------------------- | :------------------------------------------------------------ |
| Orquestração  | Apache Airflow (Docker)       | Agendar e encadear extração, validação e transformação. |
| Linguagem       | Python 3.11+ (Pandas)         | Extração, validação e cola entre os serviços.            |
| Validação     | Pandera ou Great Expectations | Garantir regras de qualidade antes de subir o dado.           |
| Lake            | Google Cloud Storage          | Guardar o dado bruto (camada raw).                            |
| Warehouse       | Google BigQuery               | Armazenar e transformar via SQL (staging e marts).            |
| Transformação | SQL (BigQuery Standard SQL)   | Limpeza, padronização e modelagem dimensional.              |
| BI              | Power BI Desktop + DAX        | Modelo estrela e relatório analítico de decisão.           |
| Versionamento   | Git + GitHub                  | Histórico, vitrine pública e base do post de portfólio.    |

**Antes de começar, instale:** Docker Desktop, Python 3.11+, Git, Power BI Desktop (Windows) e a CLI do Google Cloud (`gcloud`). Crie uma conta GCP e ative os créditos gratuitos.

---

## 3. Como usar este documento

**Filosofia central: documente enquanto constrói, não no fim.** A maior causa de portfólio fraco é tentar escrever o post depois que tudo já passou, quando você já esqueceu por que tomou cada decisão e não tem print de nada. Cada fase deste guia tem uma seção  **📸 O que documentar e capturar** . Trate-a como entregável obrigatório da fase, não como bônus.

Existe um motivo concreto. O post de portfólio será gerado depois a partir do seu repositório, e ele precisa de quatro insumos que só existem se você capturar na hora: **(1)** imagens reais (diagramas, prints de UI, dashboards); **(2)** outputs renderizados (gráficos e tabelas executados, não código vazio); **(3)** decisões técnicas com o porquê; **(4)** links de entregáveis. Sem isso, o post fica genérico.

**Crie já dois artefatos de captura:**

1. Uma pasta `images/` no repositório para os prints.
2. Um `docs/decisoes.md` onde, ao fim de cada fase, você escreve em 3 a 5 linhas qual decisão tomou e por quê. Esse arquivo é ouro: é dele que sai a parte mais difícil de escrever do post.

> **Convenção de nomes dos prints:** use `fase0X-descricao.png` (ex.: `fase02-dag-sucesso.png`). Facilita achar depois.

---

## Fase 0 — Setup do ambiente

**Objetivo.** Ter o GCP provisionado (bucket + dataset + conta de serviço) e o Airflow subindo em Docker, conversando com o GCP. Ao fim desta fase, nada de dado ainda; apenas o terreno pronto.

### Requisitos e passos

1. **Criar o projeto no GCP** e confirmar que os créditos gratuitos estão ativos no billing.
2. **Habilitar as APIs** Cloud Storage e BigQuery no projeto.
3. **Criar o bucket** na região `southamerica-east1` (São Paulo, menor latência para você).
4. **Criar o dataset** no BigQuery (ex.: `anp_combustiveis`). Você usará datasets separados para `raw`, `staging` e `mart`.
5. **Criar uma conta de serviço (service account)** com papéis  **mínimos** : acesso de objeto ao bucket, BigQuery Data Editor e BigQuery Job User. Gerar uma chave JSON.
6. **Subir o Airflow em Docker** a partir do docker-compose oficial, montando a chave JSON dentro do container.

### Dica (caminho fácil): comandos `gcloud`

Estes comandos são o esqueleto. Substitua os nomes. Tente entender cada um antes de colar; é assim que o setup deixa de ser mágica.

```bash
# 1) define o projeto ativo
gcloud config set project SEU_PROJETO

# 2) habilita as APIs necessárias
gcloud services enable storage.googleapis.com bigquery.googleapis.com

# 3) cria o bucket (nome global e único)
gcloud storage buckets create gs://radar-combustiveis-raw \
    --location=southamerica-east1

# 4) cria os datasets do BigQuery
bq --location=southamerica-east1 mk --dataset SEU_PROJETO:raw
bq --location=southamerica-east1 mk --dataset SEU_PROJETO:staging
bq --location=southamerica-east1 mk --dataset SEU_PROJETO:mart

# 5) cria a service account e gera a chave
gcloud iam service-accounts create airflow-pipeline
gcloud iam service-accounts keys create ./gcp/sa.json \
    --iam-account=airflow-pipeline@SEU_PROJETO.iam.gserviceaccount.com
```

### Snippet crítico: `docker-compose` (ajuste sobre o oficial)

Baixe o arquivo oficial do Airflow (`curl -LfO https://airflow.apache.org/docker-compose.yaml`) e aplique estes ajustes no bloco `x-airflow-common`. Eles instalam os providers do Google e injetam a credencial:

```yaml
x-airflow-common:
  &airflow-common
  environment:
    # aponta o SDK do Google para a chave montada no container
    GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/gcp/sa.json
    _PIP_ADDITIONAL_REQUIREMENTS: >-
      apache-airflow-providers-google pandera
      google-cloud-storage google-cloud-bigquery pandas
  volumes:
    - ./dags:/opt/airflow/dags
    - ./scripts:/opt/airflow/scripts
    - ./gcp:/opt/airflow/gcp        # chave aqui — NUNCA versionar
```

Suba com `docker compose up airflow-init` e depois `docker compose up -d`. A interface fica em `localhost:8080`.

> **💡 Por que isso importa**
>
> * Conta de serviço com papel mínimo (e não com permissão total) é uma das primeiras coisas que um avaliador de segurança/dados repara. Mostra que você entende o princípio do menor privilégio.
> * **A chave JSON jamais vai para o GitHub.** Coloque `gcp/` e `*.json` no `.gitignore` antes do primeiro commit. Vazar uma chave de serviço em repositório público é um erro clássico e grave.

> **📸 O que documentar e capturar**
>
> * Print do bucket criado no Console do GCP → `fase00-bucket-gcs.png`
> * Print dos três datasets no BigQuery → `fase00-datasets-bq.png`
> * Print da interface do Airflow rodando em `localhost:8080` → `fase00-airflow-up.png`
> * No `docs/decisoes.md`: registre "Airflow local vs Composer" e "papéis mínimos na service account".
> * **Para o README depois:** estes prints viram a seção "Como rodar". O diagrama de arquitetura (seção 1.1) deve virar `docs/arquitetura.png`.

---

## Fase 1 — Ingestão manual (o caminho feliz)

**Objetivo.** Provar que o caminho funciona de ponta a ponta com **um** arquivo, rodando o script na mão (ainda sem Airflow). Baixar um CSV da ANP, subir para o GCS e carregar na tabela `raw` do BigQuery.

**Por que manual primeiro.** Orquestrar algo que ainda não funciona é a receita para depurar dois problemas ao mesmo tempo. Faça o fluxo funcionar reto, depois automatize. Essa disciplina aparece no projeto.

### Requisitos e passos

1. **Identificar a URL** de um CSV da Série Histórica de Preços de Combustíveis no portal da ANP (gov.br/anp) ou no dados.gov.br.
2. **Inspecionar o arquivo** antes de programar qualquer coisa: qual o separador, o encoding e os nomes das colunas? Anote.
3. **Escrever o script de ingestão** que baixa, sobe ao GCS e carrega no BigQuery `raw`.
4. **Conferir no BigQuery** que as primeiras linhas chegaram e fazem sentido.

> **💡 Por que isso importa**
> **Inspecionar antes de codar é meio caminho.** Os arquivos da ANP costumam vir com separador ponto-e-vírgula, encoding latin-1 e o número decimal com vírgula. Descobrir isso na inspeção evita horas de erro silencioso (datas e preços virando texto ou nulo).

### Snippet crítico: ingestão GCS + BigQuery

Estrutura mínima. Mantenha o caminho no GCS particionado por ano para a camada `raw` fazer sentido como data lake:

```python
from google.cloud import storage, bigquery

BUCKET = 'radar-combustiveis-raw'

def subir_para_gcs(local, ano, arquivo):
    blob = storage.Client().bucket(BUCKET).blob(
        f'raw/anp/{ano}/{arquivo}')
    blob.upload_from_filename(local)
    return f'gs://{BUCKET}/raw/anp/{ano}/{arquivo}'

def carregar_raw(uri):
    bq = bigquery.Client()
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=';',     # ANP usa ponto-e-vírgula
        skip_leading_rows=1,
        autodetect=True,         # ok na raw; na staging tiparemos à mão
        write_disposition='WRITE_APPEND',
    )
    bq.load_table_from_uri(uri, 'SEU_PROJETO.raw.precos', job_config=cfg).result()
```

> **📸 O que documentar e capturar**
>
> * Print do objeto no GCS (mostrando o caminho particionado `raw/anp/ANO/`) → `fase01-objeto-gcs.png`
> * Print de `SELECT * ... LIMIT 10` na tabela `raw`, no editor do BigQuery → `fase01-raw-preview.png`
> * No `docs/decisoes.md`: registre separador, encoding e formato decimal descobertos. Esse achado é um bom parágrafo no post ("o dado real não vem limpo").
> * **Insight a guardar:** qualquer descoberta sobre a sujeira do dado vira um blockquote forte no post de portfólio.

---

## Fase 2 — Orquestração e validação

**Objetivo.** Transformar o script manual da Fase 1 em uma DAG do Airflow, com um portão de validação entre a extração e a carga. A validação que falha derruba a DAG de propósito.

### Requisitos e passos

1. **Modelar a DAG** com tarefas claras: `extrair` → `validar` → `carregar_raw` → `transformar`.
2. **Definir o agendamento** semanal (a ANP publica semanalmente) e desligar o `catchup` para não disparar execuções retroativas sem querer.
3. **Escrever as expectativas de qualidade** com Pandera (mais leve) ou Great Expectations (mais robusto e vendável).
4. **Provocar uma falha proposital** (ex.: alterar uma regra) para ver a DAG ficar vermelha. Esse print é um dos mais valiosos do projeto.

### Snippet crítico: esqueleto da DAG (Airflow TaskFlow)

```python
from airflow.decorators import dag, task
import pendulum

@dag(schedule='@weekly',
     start_date=pendulum.datetime(2024, 1, 1, tz='America/Fortaleza'),
     catchup=False, tags=['anp', 'combustiveis'])
def pipeline_anp():
    @task
    def extrair() -> str: ...        # baixa o CSV, retorna caminho local
    @task
    def validar(caminho: str) -> str: ...   # portão de qualidade
    @task
    def carregar_raw(caminho: str) -> None: ...
    @task
    def transformar() -> None: ...   # dispara o SQL de staging/mart no BQ

    transformar(carregar_raw(validar(extrair())))

pipeline_anp()
```

### Snippet crítico: validação com Pandera

O detalhe que importa: `lazy=True` junta todos os erros de uma vez em vez de parar no primeiro. E lançar exceção dentro da task é o que faz o Airflow marcar a tarefa como falha.

```python
import pandera as pa
from pandera import Column, Check

UFS = ['AC','AL','AP','AM','BA','CE', ...]   # as 27 unidades
PRODUTOS = ['GASOLINA','ETANOL','DIESEL S10','DIESEL S500','GLP']

schema = pa.DataFrameSchema({
    'uf':            Column(str,   Check.isin(UFS)),
    'produto':       Column(str,   Check.isin(PRODUTOS)),
    'preco_revenda': Column(float, Check.gt(0)),   # preço nunca <= 0
    'data_coleta':   Column('datetime64[ns]'),
}, coerce=True)

def validar(df):
    schema.validate(df, lazy=True)   # exceção aqui derruba a DAG
```

> **💡 Por que isso importa**
>
> * **Qualidade como portão é o que separa "mover dado" de "entregar dado confiável".** Qualquer um copia um CSV para um banco. Poucos garantem que o dado que chegou está dentro das regras antes de propagá-lo. É o requisito de validação que você pediu, e ele é um diferencial real.
> * Idempotência das tasks (rodar duas vezes sem efeito colateral) começa a importar aqui. Guarde isso para a Fase 4, onde você vai implementar de fato.

> **📸 O que documentar e capturar**
>
> * Print do *Graph view* da DAG **toda verde** (execução bem-sucedida) → `fase02-dag-sucesso.png`
> * Print da DAG **vermelha** na falha proposital de validação → `fase02-dag-falha.png` *(este vende muito)*
> * Print do log da task de validação mostrando o erro capturado → `fase02-log-validacao.png`
> * No `docs/decisoes.md`: por que Pandera ou GE, e quais expectativas você escolheu e por quê.
> * **Narrativa:** o par sucesso/falha conta a história de confiabilidade melhor que mil palavras. Reserve os dois prints lado a lado no post.

---

## Fase 3 — Staging e modelagem dimensional

**Objetivo.** Transformar o dado bruto em um modelo dimensional (esquema estrela) limpo, dentro do BigQuery, usando SQL. Esta é a fase em que a metade analítica do projeto ganha forma.

### 3.1. As três camadas no BigQuery

* **`raw`:** espelho fiel do CSV, tudo como texto. Não trata nada. Já existe desde a Fase 1.
* **`staging`:** tipagem correta, datas parseadas, padronização de texto (trim, uppercase, normalização de UF e município) e unificação do schema entre anos diferentes.
* **`marts`:** o esquema estrela pronto para o Power BI consumir.

### 3.2. O modelo estrela

Uma tabela fato central cercada por dimensões. A decisão mais importante de toda esta fase é a **definição do grão** da fato.

| Tabela                 | Tipo      | Grão / conteúdo                                                                                                 |
| :--------------------- | :-------- | :---------------------------------------------------------------------------------------------------------------- |
| `fato_preco_revenda` | Fato      | Uma coleta de preço por revenda, produto e data. Métricas: preço de revenda, preço de distribuição, margem. |
| `dim_tempo`          | Dimensão | Calendário diário com ano, mês, semana, trimestre. Marcada como tabela de datas no Power BI.                   |
| `dim_geografia`      | Dimensão | Região → UF → município (hierarquia).                                                                         |
| `dim_produto`        | Dimensão | Gasolina, etanol, diesel S-10, diesel S-500, GLP.                                                                 |
| `dim_revenda`        | Dimensão | Identificação do posto (CNPJ), endereço.                                                                       |
| `dim_bandeira`       | Dimensão | Distribuidora/bandeira do posto.                                                                                  |

### 3.3. A decisão de grão (raciocine antes de codar)

Não vou te entregar o grão pronto, porque definir grão é a habilidade que esta fase deveria treinar em você. Responda a si mesmo, nesta ordem:

1. Qual é o evento mais atômico que o dado registra? (uma pesquisa de preço em um posto, em uma data?)
2. Se eu somar a métrica sem cuidado, o número faz sentido? (somar preços não faz; média sim, e talvez média ponderada).
3. O grão que escolhi consegue responder **todas** as perguntas de negócio da seção 1? Se uma pergunta exige um grão mais fino, o modelo precisa mudar.

> **Dica:** os arquivos da ANP existem em duas naturezas (por posto e resumo municipal). O grão que você escolher determina qual arquivo é a fonte da fato. Decida conscientemente e registre o porquê.

### Snippet crítico: DDL da tabela fato (BigQuery)

Repare no `PARTITION BY` e no `CLUSTER BY`: particionar por data e agrupar por UF/produto deixa as consultas baratas e rápidas (e é o que sustenta a idempotência da Fase 4).

```sql
CREATE OR REPLACE TABLE `SEU_PROJETO.mart.fato_preco_revenda`
PARTITION BY data_coleta
CLUSTER BY uf, produto AS
SELECT
  PARSE_DATE('%d/%m/%Y', data_da_coleta)              AS data_coleta,
  UPPER(TRIM(estado))                                 AS uf,
  TRIM(municipio)                                     AS municipio,
  TRIM(produto)                                       AS produto,
  TRIM(bandeira)                                      AS bandeira,
  CAST(REPLACE(valor_de_venda,  ',', '.') AS NUMERIC) AS preco_revenda,
  CAST(REPLACE(valor_de_compra, ',', '.') AS NUMERIC) AS preco_distribuicao
FROM `SEU_PROJETO.staging.stg_precos`
WHERE valor_de_venda IS NOT NULL;
```

> **📸 O que documentar e capturar**
>
> * Diagrama do modelo estrela (faça no dbdiagram.io ou no próprio Power BI) → `docs/modelo-dimensional.png`
> * Print de uma query de staging com o resultado (mostrando o dado já limpo) → `fase03-staging-query.png`
> * Print da tabela fato com amostra de linhas → `fase03-fato-preview.png`
> * No `docs/decisoes.md`: a  **decisão de grão** , escrita com o raciocínio dos 3 passos. É o trecho técnico mais valioso do projeto.

---

## Fase 4 — Carga incremental e idempotência

**Objetivo.** Fazer o pipeline rodar repetidamente sem duplicar dado (idempotência) e processando apenas o que é novo (incremental). É aqui que o projeto deixa de ser tutorial e vira engenharia de verdade.

### 4.1. Os dois conceitos

* **Idempotência:** rodar a mesma carga duas vezes produz exatamente o mesmo resultado. Reprocessar a semana 10 não pode criar linhas duplicadas da semana 10.
* **Carga incremental:** a cada execução, processar somente o período novo, em vez de reprocessar a série inteira. Controla-se com um estado (qual a última semana carregada).

### 4.2. Duas estratégias de idempotência (escolha e justifique)

| Estratégia               | Como funciona                                                                                 | Quando usar                                                                                   |
| :------------------------ | :-------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------- |
| Sobrescrita de partição | Recarrega a partição inteira da data com `WRITE_TRUNCATE`no decorador `table$YYYYMMDD`. | Simples e robusta quando o dado de uma data sempre chega completo.**Recomendada aqui.** |
| `MERGE`(upsert)         | Atualiza linhas existentes e insere novas com base em uma chave de negócio.                  | Quando linhas podem ser corrigidas/atualizadas individualmente.                               |

### Snippet crítico: idempotência via `MERGE` (upsert)

```sql
MERGE `SEU_PROJETO.mart.fato_preco_revenda` AS T
USING staging_do_lote AS S
  ON  T.data_coleta  = S.data_coleta
  AND T.cnpj_revenda = S.cnpj_revenda
  AND T.produto      = S.produto
WHEN MATCHED THEN
  UPDATE SET preco_revenda = S.preco_revenda,
             preco_distribuicao = S.preco_distribuicao
WHEN NOT MATCHED THEN
  INSERT (data_coleta, cnpj_revenda, produto, preco_revenda, preco_distribuicao)
  VALUES (S.data_coleta, S.cnpj_revenda, S.produto,
          S.preco_revenda, S.preco_distribuicao);
```

> **Dica para o incremental:** guarde a última data carregada (numa tabela de controle no BQ ou numa Variable do Airflow) e, na extração, baixe apenas arquivos mais recentes que ela.

> **💡 Por que isso importa**
> Idempotência é a pergunta de entrevista clássica de engenharia de dados. Ter implementado de verdade (e conseguir explicar a estratégia escolhida) coloca você à frente de quem só leu sobre o assunto.

> **📸 O que documentar e capturar**
>
> * Print de duas execuções seguidas + um `COUNT(*)` provando que a contagem **não** mudou na segunda → `fase04-idempotencia-prova.png`
> * Print do mecanismo de controle do incremental (Variable do Airflow ou tabela de controle) → `fase04-controle-incremental.png`
> * No `docs/decisoes.md`: qual estratégia de idempotência você escolheu (sobrescrita de partição × `MERGE`) e por quê.
> * **Demonstração matadora:** o print do `COUNT(*)` idêntico antes/depois do reprocessamento é prova concreta. Vale um blockquote no post.

---

## Fase 5 — Power BI: modelagem e DAX avançado

**Objetivo.** Conectar o Power BI ao modelo estrela no BigQuery e construir um relatório que responde a decisão, com medidas DAX que vão muito além de `SUM`.

### 5.1. Modelagem no Power BI

* **Conecte** via conector nativo do BigQuery (Obter Dados → Google BigQuery).
* **Relacione** a fato às dimensões em esquema estrela (1 para muitos, da dimensão para a fato).
* **Marque `dim_tempo` como Tabela de Datas.** Isso é pré-requisito para a inteligência temporal ( *time intelligence* ) funcionar corretamente.

### 5.2. Medidas DAX que demonstram maturidade

Não vou entregar todas prontas. Seguem as críticas como referência; as variações (mês, trimestre) ficam como seu exercício.

```dax
-- base
Preco Medio Revenda = AVERAGE( fato_preco_revenda[preco_revenda] )

-- variação semana contra semana (inteligência temporal)
Preco Semana Anterior =
    CALCULATE( [Preco Medio Revenda], DATEADD( dim_tempo[data], -7, DAY ) )

Variacao Semanal % =
    VAR atual    = [Preco Medio Revenda]
    VAR anterior = [Preco Semana Anterior]
    RETURN DIVIDE( atual - anterior, anterior )

-- ranking de UFs mais caras
Ranking UF =
    RANKX( ALL( dim_geografia[uf] ), [Preco Medio Revenda], , DESC, Dense )

-- dispersão de preço por região
Desvio Padrao Preco = STDEV.P( fato_preco_revenda[preco_revenda] )

-- % de postos acima da média nacional
Media Nacional = CALCULATE( [Preco Medio Revenda], ALL( dim_geografia ) )
% Acima da Media Nacional =
    DIVIDE(
      CALCULATE( COUNTROWS( fato_preco_revenda ),
                 fato_preco_revenda[preco_revenda] > [Media Nacional] ),
      COUNTROWS( fato_preco_revenda ) )
```

> **Nuance técnica (rigor):** `DATEADD` exige uma coluna de data contígua. Como o dado é semanal, garanta que `dim_tempo` seja diária e que as datas de coleta batam, ou crie um índice de semana para deslocamentos mais seguros. Se quiser média ponderada, o peso (número de postos pesquisados) depende do grão que você definiu na Fase 3 — tudo se conecta.

### 5.3. Estrutura do relatório (duas páginas)

* **Página executiva:** KPIs (preço médio nacional, variação semanal), tendência temporal, destaque das maiores altas/quedas.
* **Página analítica:** mapa coroplético por UF, drill por bandeira e região, tabela de ranking, dispersão de preços.

> **📸 O que documentar e capturar**
>
> * Print da visão de Modelo (relacionamentos do esquema estrela) → `fase05-modelo-relacionamentos.png`
> * Screenshot da página executiva do relatório → `fase05-relatorio-executivo.png`
> * Screenshot da página analítica (com o mapa) → `fase05-relatorio-analitico.png`
> * Print do painel de uma medida DAX selecionada → `fase05-dax-medida.png`
> * Salve o `.pbix` em `powerbi/` no repositório. Se publicar no Power BI Service, guarde o link público.
> * **Entregável externo:** um link de relatório publicado é o tipo de coisa que a skill de portfólio usa como "Links externos". Tenha o link à mão.

---

## Fase 6 — Documentação, GitHub e portfólio

**Objetivo.** Empacotar tudo de forma que um recrutador entenda o projeto em dois minutos de README e queira abrir o post completo. Esta fase consome o que você capturou nas seções 📸.

### 6.1. Estrutura recomendada do repositório

```text
radar-combustiveis-br/
├── dags/                  # DAG do Airflow
├── scripts/               # extração, validação (Python)
├── sql/                   # staging e marts (SQL)
├── powerbi/               # .pbix + screenshots do relatório
├── docs/
│   ├── arquitetura.png         # diagrama da seção 1.1
│   ├── modelo-dimensional.png  # esquema estrela
│   └── decisoes.md             # registro de decisões (ADR-lite)
├── images/                # todos os prints fase0X-*.png
├── docker-compose.yaml
├── requirements.txt
├── .gitignore             # ignora gcp/, *.json, .env
├── .env.example           # variáveis sem valores reais
└── README.md
```

> **💡 Por que isso importa**
> **O `.env.example` e o `.gitignore` não são detalhe.** Mostram que você sabe separar configuração de segredo. A skill de portfólio lê o `.env.example` para entender as dependências externas, e o avaliador lê o `.gitignore` para confirmar que você não vazou credenciais.

### 6.2. O que o README precisa ter

1. Uma frase de abertura com a tese de negócio (o problema que o projeto resolve).
2. O diagrama de arquitetura (`docs/arquitetura.png`) logo no topo.
3. Stack usada, em badges ou lista.
4. Seção "Como rodar" com os prints da Fase 0.
5. Um screenshot do relatório final do Power BI (o gancho visual).
6. Link para o post completo no seu site de portfólio.

### 6.3. Ponte para o post de portfólio (`.mdx`)

Quando o repositório estiver completo e coeso, o post longo é gerado a partir dele. Para que a geração produza algo forte, confirme que o repositório tem:

* README coerente com o que o código realmente faz (sem etapas descritas que não existem).
* Imagens reais em `images/` e `docs/` (a skill as usa como base dos placeholders visuais).
* O `docs/decisoes.md` preenchido (vira a seção de decisões técnicas, a mais difícil de escrever depois).
* Links externos funcionais (relatório publicado, se houver).
* Scripts e SQL comentados, contando a mesma história do README.

> Quando chegar aqui, é só me dizer: aponte o repositório e eu gero o `.mdx` no padrão do seu site (frontmatter, narrativa estilo Medium, placeholders de imagem e seção de contato).

> **📸 O que documentar e capturar**
>
> * Confira o checklist mestre da seção 7 antes de considerar a fase encerrada.
> * **Regra de ouro:** se você seguiu as seções 📸 de todas as fases, esta fase é só montagem. Se pulou, vai ter que reabrir o projeto para tirar print, e aí dói.

---

## 7. Checklist mestre de captura

Consolidação de tudo que precisa ser capturado, e onde cada artefato será usado. Marque conforme avança.

| Fase | Artefato a capturar                                           | Uso    |
| :--: | :------------------------------------------------------------ | :----- |
|  0  | Print do bucket GCS, dos datasets BQ e do Airflow rodando     | README |
|  0  | Diagrama de arquitetura (`docs/arquitetura.png`)            | Ambos  |
|  0  | Decisão: Airflow local × Composer; papéis mínimos         | Post   |
|  1  | Objeto no GCS particionado; preview da tabela `raw`         | Post   |
|  1  | Decisão/achado: separador, encoding, decimal da ANP          | Post   |
|  2  | DAG verde (sucesso) e DAG vermelha (falha de validação)     | Ambos  |
|  2  | Log da validação capturando o erro                          | Post   |
|  2  | Decisão: Pandera × GE; expectativas escolhidas              | Post   |
|  3  | Diagrama do modelo estrela (`docs/modelo-dimensional.png`)  | Ambos  |
|  3  | Query de staging e fato com resultado                         | Post   |
|  3  | Decisão de grão (com o raciocínio dos 3 passos)            | Post   |
|  4  | Prova de idempotência (`COUNT`igual após reprocessar)     | Post   |
|  4  | Mecanismo do incremental                                      | Post   |
|  4  | Decisão: estratégia de idempotência escolhida              | Post   |
|  5  | Modelo de relacionamentos; páginas do relatório; medida DAX | Ambos  |
|  5  | `.pbix`salvo e link do relatório publicado (se houver)     | Ambos  |
|  6  | README final coerente;`.gitignore`e `.env.example`        | Ambos  |

---

## Apêndice A — dbt (opcional)

**O que muda.** Em vez de SQL solto na Fase 3, você organiza as transformações como modelos dbt, com testes de dados versionados e documentação automática. dbt é uma das ferramentas mais valorizadas no mercado de dados hoje.

**Trade-off.** Adiciona uma curva de aprendizado e mais uma peça ao ambiente. Vale se você quer pesar o lado de engenharia /  *analytics engineering* . Se o foco do momento for entregar o arco completo, deixe para uma v2 do projeto.

### Snippet de referência: modelo + teste dbt

```sql
-- models/marts/fato_preco_revenda.sql
{{ config(materialized='table', partition_by={'field':'data_coleta',
          'data_type':'date'}) }}
SELECT ... FROM {{ ref('stg_precos') }}
```

```yaml
# models/marts/schema.yml  (testes de dados versionados)
models:
  - name: fato_preco_revenda
    columns:
      - name: preco_revenda
        tests: [not_null]
      - name: uf
        tests:
          - accepted_values: { values: ['CE','SP','RJ', ...] }
```

> **💡 Por que isso importa**
> Testes de dados em dbt cobrem o mesmo objetivo da validação Pandera, mas no nível do warehouse e versionados em Git. Mencionar que você entende as duas camadas de qualidade (na ingestão e no warehouse) é um sinal forte de maturidade.

---

## Apêndice B — Terraform (opcional)

**O que muda.** Em vez de criar bucket e datasets na mão (ou via `gcloud`), você os declara como código (infraestrutura como código). Permite destruir e recriar tudo com um comando, e documenta a infra no próprio repositório.

**Trade-off.** Para um projeto de portfólio com poucos recursos, é mais sinalização de competência do que necessidade. Um `main.tf` curto já comunica que você conhece o conceito.

### Snippet de referência: infra mínima

```hcl
# main.tf
provider "google" {
  project = var.projeto
  region  = "southamerica-east1"
}

resource "google_storage_bucket" "raw" {
  name     = "radar-combustiveis-raw"
  location = "southamerica-east1"
}

resource "google_bigquery_dataset" "mart" {
  dataset_id = "mart"
  location   = "southamerica-east1"
}
```

> **💡 Por que isso importa**
> Se incluir Terraform, adicione um print de `terraform plan/apply` e um parágrafo no README. Vira mais um artefato de captura e mais um ponto de conversa em entrevista.

---

## Próximos passos

A ordem importa. Não pule a sequência das fases: cada uma assume que a anterior funciona.

1. **Comece pela Fase 0.** Quando o Airflow estiver de pé conversando com o GCP, me avise se quiser que eu te guie no setup com mais detalhe (no estilo dica antes da solução).
2. **Preencha a seção 📸 de cada fase** no momento em que a encerra. Documente enquanto constrói.
3. **Ao terminar a Fase 6,** aponte o repositório e peça a geração do post `.mdx` para o seu site.

**Dúvida em qualquer fase?** Volte aqui e me chame com o ponto específico. Posso detalhar o setup do GCP, depurar a DAG, revisar seu modelo dimensional ou afinar as medidas DAX.
