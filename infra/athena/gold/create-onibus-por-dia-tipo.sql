DROP TABLE IF EXISTS gold.onibus_dia_tipo;

CREATE TABLE gold.onibus_dia_tipo
WITH (
  format = 'PARQUET',
  external_location = 's3://safe-moove-raw/gold/onibus-dia-tipo/'
) AS
SELECT
  l.tipo as tipo_linha,
  COUNT(DISTINCT p.prefixo_veiculo) as qtd_onibus,
  l.dia as dia_circulacao
FROM silver.linhas l
JOIN silver.posicoes p ON l.codigo_linha = p.codigo_linha
GROUP BY l.dia, l.tipo
;
