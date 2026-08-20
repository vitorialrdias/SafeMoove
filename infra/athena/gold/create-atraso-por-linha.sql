CREATE TABLE gold.atraso_por_linha
WITH (
  format = 'PARQUET',
  external_location = 's3://safe-moove-raw/gold/atraso_por_linha/'
) AS
SELECT
  l.codigo_linha as id_linha,
  p.codigo_parada as id_parada,
  p.horario_previsto,
  p.horario_consulta
FROM linhas l
LEFT JOIN previsoes p ON l.codigo_linha = p.codigo_linha
;

