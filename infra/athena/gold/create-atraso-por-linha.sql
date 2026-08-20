DROP TABLE IF EXISTS gold.atraso_por_linha;

CREATE TABLE gold.atraso_por_linha
WITH (
  format = 'PARQUET',
  external_location = 's3://safe-moove-raw/gold/atraso-por-linha/'
) AS
WITH previsoes_ordenadas AS (
  SELECT
    codigo_linha,
    codigo_parada,
    prefixo_veiculo,
    dia,
    timestamp_previsao,
    horario_previsto,
    LAG(horario_previsto) OVER (
      PARTITION BY codigo_linha, codigo_parada, prefixo_veiculo
      ORDER BY timestamp_previsao
    ) AS horario_previsto_anterior,
    LAG(timestamp_previsao) OVER (
      PARTITION BY codigo_linha, codigo_parada, prefixo_veiculo
      ORDER BY timestamp_previsao
    ) AS timestamp_previsao_anterior
  FROM silver.previsoes
),
drift AS (
  SELECT
    codigo_linha,
    dia,
    date_diff(
      'minute',
      date_parse(horario_previsto_anterior, '%H:%i'),
      date_parse(horario_previsto, '%H:%i')
    ) AS drift_minutos
  FROM previsoes_ordenadas
  WHERE horario_previsto_anterior IS NOT NULL
    -- só compara leituras que realmente aconteceram próximas no tempo real
    -- (timestamp_previsao tem data completa; sem isso o LAG pode comparar
    -- sessões de coleta diferentes, horas ou dias de distância)
    AND date_diff(
      'minute',
      from_iso8601_timestamp(timestamp_previsao_anterior),
      from_iso8601_timestamp(timestamp_previsao)
    ) BETWEEN 0 AND 60
)
SELECT
  l.letreiro,
  l.origem,
  l.destino,
  d.codigo_linha AS id_linha,
  d.dia AS dia_circulacao,
  cast(AVG(d.drift_minutos) AS decimal) AS atraso_medio_minutos,
  COUNT(*) AS qtd_leituras
FROM drift d
JOIN silver.linhas l ON d.codigo_linha = l.codigo_linha
GROUP BY l.letreiro, l.origem, l.destino, d.codigo_linha, d.dia
;
