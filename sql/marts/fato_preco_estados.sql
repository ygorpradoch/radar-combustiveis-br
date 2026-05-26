CREATE OR REPLACE TABLE `radar-combustiveis-br.mart.fato_preco_estados`
PARTITION BY data_inicial
CLUSTER BY estado, produto
AS
SELECT * FROM `radar-combustiveis-br.staging.stg_precos_estados`