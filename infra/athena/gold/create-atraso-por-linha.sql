DROP TABLE IF EXISTS gold.atraso_por_linha;

CREATE TABLE gold.atraso_por_linha
WITH (
  format = 'PARQUET',
  external_location = 's3://safe-moove-raw/gold/atraso-por-linha/'
) AS
WITH base AS (
  SELECT
    codigo_linha,
    codigo_parada,
    prefixo_veiculo,
    dia,
    ciclo,
    -- ano/mes/dia da particao ja sao hora local de SP (consumer_s3.py),
    -- mesmo fuso de horario_previsto/horario_consulta -- evita misturar
    -- timestamp_previsao (UTC) com hora local, que gerou erro de ~21h
    (CAST(ano AS varchar) || '-' || lpad(CAST(mes AS varchar), 2, '0') || '-' || lpad(CAST(dia AS varchar), 2, '0')) AS data_local,
    horario_previsto,
    horario_consulta
  FROM silver.previsoes
),
reconstruido AS (
  SELECT
    codigo_linha, codigo_parada, prefixo_veiculo, dia, ciclo,
    date_parse(data_local || ' ' || horario_consulta, '%Y-%m-%d %H:%i') AS consulta_ts,
    date_parse(data_local || ' ' || horario_previsto, '%Y-%m-%d %H:%i') AS previsto_ts_bruto
  FROM base
),
corrigido AS (
  SELECT
    codigo_linha, codigo_parada, prefixo_veiculo, dia, ciclo,
    consulta_ts,
    -- previsao sempre aponta pro futuro -- se a reconstrucao caiu antes da
    -- consulta, cruzou meia-noite -> soma 1 dia
    (CASE
      WHEN previsto_ts_bruto < consulta_ts THEN previsto_ts_bruto + INTERVAL '1' DAY
      ELSE previsto_ts_bruto
    END) AS previsto_ts
  FROM reconstruido
),
marcado AS (
  SELECT *,
    CASE
      WHEN LAG(ciclo) OVER (PARTITION BY codigo_linha, codigo_parada, prefixo_veiculo ORDER BY ciclo) IS NULL
        OR ciclo - LAG(ciclo) OVER (PARTITION BY codigo_linha, codigo_parada, prefixo_veiculo ORDER BY ciclo) > 1
      THEN 1 ELSE 0
    END AS inicio_nova_viagem
  FROM corrigido
),
viagens AS (
  SELECT *,
    SUM(inicio_nova_viagem) OVER (
      PARTITION BY codigo_linha, codigo_parada, prefixo_veiculo ORDER BY ciclo
    ) AS numero_viagem
  FROM marcado
),
resumo_viagem AS (
  SELECT
    codigo_linha, codigo_parada, prefixo_veiculo, numero_viagem, dia,
    COUNT(*) AS qtd_leituras,
    min_by(previsto_ts, ciclo) AS previsao_inicial_ts,
    max_by(consulta_ts, ciclo) AS ultima_leitura_ts
  FROM viagens
  GROUP BY codigo_linha, codigo_parada, prefixo_veiculo, numero_viagem, dia
),
atraso_por_viagem AS (
  SELECT
    codigo_linha, dia,
    date_diff('minute', previsao_inicial_ts, ultima_leitura_ts) AS atraso_minimo_minutos
  FROM resumo_viagem
  WHERE qtd_leituras >= 2
)
SELECT
  l.letreiro,
  l.origem,
  l.destino,
  a.codigo_linha AS id_linha,
  a.dia AS dia_circulacao,
  cast(AVG(a.atraso_minimo_minutos) AS decimal) AS atraso_medio_minutos,
  COUNT(*) AS qtd_viagens
FROM atraso_por_viagem a
JOIN silver.linhas l ON a.codigo_linha = l.codigo_linha
GROUP BY l.letreiro, l.origem, l.destino, a.codigo_linha, a.dia
;
