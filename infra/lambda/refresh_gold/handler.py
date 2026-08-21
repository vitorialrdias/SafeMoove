import json
import os
import time

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
DATABASE = "gold"
DATA_BUCKET = os.environ.get("DATA_BUCKET", "safe-moove-raw")
RESULTS_BUCKET = os.environ["ATHENA_RESULTS_BUCKET"]

athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


TABELAS = {
    "onibus_dia_tipo": {
        "arquivo": "create-onibus-por-dia-tipo.sql",
        "prefixo_s3": "gold/onibus-dia-tipo/",
    },
    "atraso_por_linha": {
        "arquivo": "create-atraso-por-linha.sql",
        "prefixo_s3": "gold/atraso-por-linha/",
    },
}


def _run_query(sql, database=None):
    kwargs = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": f"s3://{RESULTS_BUCKET}/"},
    }
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}

    query_id = athena.start_query_execution(**kwargs)["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if status["State"] != "SUCCEEDED":
        raise Exception(f"Query falhou ({status['State']}): {status.get('StateChangeReason')}")

    return query_id


def _limpar_prefixo(prefixo):
    paginator = s3.get_paginator("list_objects_v2")
    chaves = []
    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            chaves.append({"Key": obj["Key"]})

    for i in range(0, len(chaves), 1000):
        lote = chaves[i:i + 1000]
        s3.delete_objects(Bucket=DATA_BUCKET, Delete={"Objects": lote})

    return len(chaves)


def handler(event, context):
    base_dir = os.path.dirname(__file__)
    resultado = {}

    for nome, cfg in TABELAS.items():
        caminho = os.path.join(base_dir, cfg["arquivo"])
        with open(caminho, encoding="utf-8") as f:
            conteudo = f.read()

        # separa o DROP (primeiro ';') do CREATE (resto, sem o ';' final) --
        # nao usa split(';') ingenuo pq comentarios no SQL podem ter ';'
        primeiro_fim = conteudo.index(";")
        drop_stmt = conteudo[:primeiro_fim]
        resto = conteudo[primeiro_fim + 1:].strip()
        create_stmt = resto[:-1] if resto.endswith(";") else resto

        try:
            apagados = _limpar_prefixo(cfg["prefixo_s3"])
            _run_query(drop_stmt, database=DATABASE)
            _run_query(create_stmt, database=DATABASE)
            resultado[nome] = {"status": "ok", "s3_objetos_limpos": apagados}
        except Exception as e:
            resultado[nome] = {"status": "erro", "mensagem": str(e)}

    return {"statusCode": 200, "body": json.dumps(resultado)}
