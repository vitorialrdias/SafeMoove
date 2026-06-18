import os
import time
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("TOKEN-SPTRANS")

TOPIC = "sptrans-posicoes"

api = SPTransAPI(TOKEN)
producer = get_producer()

codigo_linha = api.obter_codigo_linha("8000")

if not codigo_linha:
    raise Exception("Linha não encontrada")

while True:

    payload = api.obter_posicao(codigo_linha)

    if payload:

        producer.send(TOPIC, payload)

        producer.flush()

        print("Posições enviadas")

    time.sleep(10)