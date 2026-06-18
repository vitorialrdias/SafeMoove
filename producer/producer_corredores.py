import os
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("TOKEN-SPTRANS")

TOPIC = "sptrans-corredores"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Corredor"
)

producer = get_producer()

r = requests.get(URL, headers=HEADERS)

if r.status_code == 200:

    producer.send(TOPIC, r.json())

    producer.flush()

print("Corredores enviados")