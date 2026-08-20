CREATE EXTERNAL TABLE IF NOT EXISTS silver.previsoes (
  ciclo bigint,
  horario_consulta string,
  codigo_linha bigint,
  codigo_parada bigint,
  nome_parada string,
  latitude_parada double,
  longitude_parada double,
  prefixo_veiculo string,
  horario_previsto string,
  acessivel boolean,
  timestamp_previsao string,
  latitude_veiculo double,
  longitude_veiculo double
)
PARTITIONED BY (ano int, mes int, dia int)
STORED AS PARQUET
LOCATION 's3://safe-moove-raw/parquet/previsoes/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.ano.type' = 'integer',
  'projection.ano.range' = '2024,2030',
  'projection.mes.type' = 'integer',
  'projection.mes.range' = '1,12',
  'projection.mes.digits' = '2',
  'projection.dia.type' = 'integer',
  'projection.dia.range' = '1,31',
  'projection.dia.digits' = '2',
  'storage.location.template' = 's3://safe-moove-raw/parquet/previsoes/ano=${ano}/mes=${mes}/dia=${dia}/'
);
