CREATE EXTERNAL TABLE IF NOT EXISTS silver.linhas (
  codigo_linha bigint,
  circular boolean,
  letreiro string,
  sentido bigint,
  tipo bigint,
  origem string,
  destino string
)
PARTITIONED BY (ano int, mes int, dia int)
STORED AS PARQUET
LOCATION 's3://safe-moove-raw/parquet/linhas/'
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
  'storage.location.template' = 's3://safe-moove-raw/parquet/linhas/ano=${ano}/mes=${mes}/dia=${dia}/'
);
