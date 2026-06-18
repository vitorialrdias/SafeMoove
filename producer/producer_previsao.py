import os
import time
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer
from producer.api_sptrans import SPTransAPI

load_dotenv()

TOKEN = os.getenv("TOKEN-SPTRANS")

TOPIC = "sptrans-previsoes"

api = SPTransAPI(TOKEN)
producer = get_producer()

codigo_linha = api.obter_codigo_linha("8000")

if not codigo_linha:
    raise Exception("Linha não encontrada")

URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Previsao/Linha"
)

while True:

    r = requests.get(
        URL,
        headers=api.headers,
        params={"codigoLinha": codigo_linha}
    )

    if r.status_code == 200:

        payload = r.json()

        producer.send(TOPIC, payload)

        producer.flush()

        print("Previsões enviadas")

    time.sleep(15)