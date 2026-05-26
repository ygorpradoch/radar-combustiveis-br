CREATE OR REPLACE TABLE `radar-combustiveis-br.mart.dim_tempo` AS
  SELECT
  data,
  EXTRACT(YEAR FROM data)    AS ano,
  EXTRACT(MONTH FROM data)   AS mes,
  EXTRACT(WEEK FROM data)    AS semana,
  EXTRACT(QUARTER FROM data) AS trimestre,
  FORMAT_DATE('%A', data)    AS dia_semana
FROM
  UNNEST(GENERATE_DATE_ARRAY('2012-12-30', '2026-12-31')) AS data
ORDER BY data