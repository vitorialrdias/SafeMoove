import os
import time

import boto3
import pandas as pd

from shared.logger import get_logger

logger = get_logger(__name__)

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
DATABASE = os.getenv("ATHENA_DATABASE", "gold")
RESULTS_BUCKET = os.getenv("ATHENA_RESULTS_BUCKET")

_athena_client = None


def _get_athena_client():
    global _athena_client
    if _athena_client is None:
        _athena_client = boto3.client("athena", region_name=REGION)
    return _athena_client


def _output_location():
    if not RESULTS_BUCKET:
        raise RuntimeError("ATHENA_RESULTS_BUCKET não configurado no ambiente (.env).")
    return f"s3://{RESULTS_BUCKET}/"


def _tentar_numerico(coluna):
    convertida = pd.to_numeric(coluna, errors="coerce")
    if convertida.notna().sum() == coluna.notna().sum():
        return convertida
    return coluna


def _resultado_para_dataframe(query_id):
    athena = _get_athena_client()
    paginator = athena.get_paginator("get_query_results")

    colunas = None
    linhas = []

    for pagina in paginator.paginate(QueryExecutionId=query_id):
        registros = pagina["ResultSet"]["Rows"]
        if colunas is None:
            colunas = [c.get("VarCharValue", "") for c in registros[0]["Data"]]
            registros = registros[1:]
        for r in registros:
            linhas.append([c.get("VarCharValue") for c in r["Data"]])

    df = pd.DataFrame(linhas, columns=colunas)
    return df.apply(_tentar_numerico)


def executar_query(sql, database=DATABASE):
    """Roda uma query no Athena e retorna o resultado como DataFrame."""
    athena = _get_athena_client()

    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": _output_location()},
    )["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Query falhou ({status['State']}): {status.get('StateChangeReason')}")

    return _resultado_para_dataframe(query_id)


def carregar_onibus_por_dia_tipo():
    """gold.onibus_dia_tipo -- veículos distintos por dia, por tipo de linha."""
    logger.info("Consultando gold.onibus_dia_tipo...")
    return executar_query("SELECT * FROM gold.onibus_dia_tipo")


def carregar_atraso_por_linha():
    """gold.atraso_por_linha -- atraso mínimo médio por linha e dia."""
    logger.info("Consultando gold.atraso_por_linha...")
    return executar_query("SELECT * FROM gold.atraso_por_linha")


if __name__ == "__main__":
    onibus = carregar_onibus_por_dia_tipo()
    atraso = carregar_atraso_por_linha()

    logger.info(f"onibus_dia_tipo: {onibus.shape[0]} linhas, colunas: {list(onibus.columns)}")
    logger.info(f"atraso_por_linha: {atraso.shape[0]} linhas, colunas: {list(atraso.columns)}")
