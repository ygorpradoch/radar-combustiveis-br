CREATE OR REPLACE TABLE `radar-combustiveis-br.staging.stg_precos_estados` AS
  SELECT
    DATE(`DATA INICIAL`)              AS data_inicial,
    DATE(`DATA FINAL`)                AS data_final,
    `REGIÃO`                          AS regiao,
    ESTADO                            AS estado,
    PRODUTO                           AS produto,
    `NÚMERO DE POSTOS PESQUISADOS`    AS num_postos,
    UPPER(`UNIDADE DE MEDIDA`)        AS unidade_medida,
    `PREÇO MÉDIO REVENDA`             AS preco_medio_revenda,
    SAFE_CAST(`PREÇO MÉDIO DISTRIBUIÇÃO` AS NUMERIC) AS preco_medio_distribuicao,
    SAFE_CAST(`MARGEM MÉDIA REVENDA`     AS NUMERIC) AS margem_media_revenda 

  FROM `radar-combustiveis-br.raw.precos_estados`

  