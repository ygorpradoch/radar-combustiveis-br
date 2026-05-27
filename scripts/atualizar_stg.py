from config import *
from google.cloud import bigquery

def atualizar_stg_function() -> None:
    bq  = bigquery.Client()

    bq.query(f"""CREATE TABLE IF NOT EXISTS `{TABELA_STG}` (
                    data_inicial             DATE,
                    data_final               DATE,
                    regiao                   STRING,
                    estado                   STRING,
                    produto                  STRING,
                    num_postos               INT64,
                    unidade_medida           STRING,
                    preco_medio_revenda      FLOAT64,
                    preco_medio_distribuicao NUMERIC,
                    margem_media_revenda     NUMERIC
                )""").result()

    bq.query(f"""MERGE `{TABELA_STG}` T
            USING (
                SELECT * EXCEPT(rn)
                FROM (
                    SELECT
                        DATE(`DATA INICIAL`)                                 AS data_inicial,
                        DATE(`DATA FINAL`)                                   AS data_final,
                        `REGIÃO`                                             AS regiao,
                        ESTADO                                               AS estado,
                        PRODUTO                                              AS produto,
                        `NÚMERO DE POSTOS PESQUISADOS`                       AS num_postos,
                        UPPER(`UNIDADE DE MEDIDA`)                           AS unidade_medida,
                        `PREÇO MÉDIO REVENDA`                                AS preco_medio_revenda,
                        SAFE_CAST(`PREÇO MÉDIO DISTRIBUIÇÃO` AS NUMERIC)     AS preco_medio_distribuicao,
                        SAFE_CAST(`MARGEM MÉDIA REVENDA`     AS NUMERIC)     AS margem_media_revenda,
                        ROW_NUMBER() OVER (
                            PARTITION BY DATE(`DATA INICIAL`), ESTADO, PRODUTO
                            ORDER BY `DATA INICIAL` DESC
                        ) AS rn
                    FROM `{TABELA_RAW}`
                )
                WHERE rn = 1
            ) S
            ON  T.data_inicial = S.data_inicial
            AND T.estado       = S.estado
            AND T.produto      = S.produto
            WHEN MATCHED THEN UPDATE SET
                data_final               = S.data_final,
                regiao                   = S.regiao,
                num_postos               = S.num_postos,
                unidade_medida           = S.unidade_medida,
                preco_medio_revenda      = S.preco_medio_revenda,
                preco_medio_distribuicao = S.preco_medio_distribuicao,
                margem_media_revenda     = S.margem_media_revenda
            WHEN NOT MATCHED THEN INSERT VALUES (
                S.data_inicial, S.data_final, S.regiao, S.estado,
                S.produto, S.num_postos, S.unidade_medida,
                S.preco_medio_revenda, S.preco_medio_distribuicao,
                S.margem_media_revenda
            )
        """).result()
    print("Staging atualizada (MERGE)")