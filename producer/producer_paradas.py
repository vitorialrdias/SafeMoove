import os
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("TOKEN-SPTRANS")

TOPIC = "sptrans-paradas"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

BASE_URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Parada/Buscar"
)

producer = get_producer()

# Exemplo simples: busca genérica (SPTrans não permite listagem total)
params = {"termosBusca": "terminal"}

r = requests.get(BASE_URL, headers=HEADERS, params=params)

if r.status_code == 200:

    dados = r.json()

    for parada in dados:

        payload = {
            "codigo_parada": parada.get("cp"),
            "nome": parada.get("np"),
            "latitude": parada.get("py"),
            "longitude": parada.get("px")
        }

        producer.send(TOPIC, payload)

producer.flush()

print("Paradas enviadas para Kafka")