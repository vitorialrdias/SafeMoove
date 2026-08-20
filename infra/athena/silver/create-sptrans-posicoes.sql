CREATE EXTERNAL TABLE IF NOT EXISTS silver.posicoes (
  horario_consulta string,
  timestamp_veiculo string,
  codigo_linha bigint,
  letreiro string,
  sentido bigint,
  origem string,
  destino string,
  prefixo_veiculo string,
  acessivel boolean,
  latitude double,
  longitude double
)
PARTITIONED BY (ano int, mes int, dia int)
STORED AS PARQUET
LOCATION 's3://safe-moove-raw/parquet/posicoes/'
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
  'storage.location.template' = 's3://safe-moove-raw/parquet/posicoes/ano=${ano}/mes=${mes}/dia=${dia}/'
);
