import pyarrow as pa

# Schema fixo por tópico, usado pelo consumer_s3 ao gravar Parquet.
# Evita que o pyarrow infira dtypes diferentes entre batches do mesmo
# tópico (o que gera arquivos com schemas incompatíveis no mesmo
# particionamento e quebra leitura no Athena).

SCHEMAS = {
    "sptrans-linhas": pa.schema([
        ("codigo_linha", pa.int64()),
        ("circular", pa.bool_()),
        ("letreiro", pa.string()),
        ("sentido", pa.int64()),
        ("tipo", pa.int64()),
        ("origem", pa.string()),
        ("destino", pa.string()),
    ]),
    "sptrans-paradas": pa.schema([
        ("codigo_parada", pa.int64()),
        ("nome", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
    ]),
    "sptrans-posicoes": pa.schema([
        ("horario_consulta", pa.string()),
        ("timestamp_veiculo", pa.string()),
        ("codigo_linha", pa.int64()),
        ("letreiro", pa.string()),
        ("sentido", pa.int64()),
        ("origem", pa.string()),
        ("destino", pa.string()),
        ("prefixo_veiculo", pa.string()),
        ("acessivel", pa.bool_()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
    ]),
    "sptrans-previsoes": pa.schema([
        ("horario_consulta", pa.string()),
        ("codigo_linha", pa.int64()),
        ("codigo_parada", pa.int64()),
        ("nome_parada", pa.string()),
        ("latitude_parada", pa.float64()),
        ("longitude_parada", pa.float64()),
        ("prefixo_veiculo", pa.string()),
        ("horario_previsto", pa.string()),
        ("acessivel", pa.bool_()),
        ("timestamp_previsao", pa.string()),
        ("latitude_veiculo", pa.float64()),
        ("longitude_veiculo", pa.float64()),
    ]),
    "sptrans-corredores": pa.schema([
        ("codigo_corredor", pa.int64()),
        ("nome_corredor", pa.string()),
    ]),
}
