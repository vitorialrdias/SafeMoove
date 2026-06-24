import os
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("TOKEN_SPTRANS")
TOPIC = "sptrans-velocidade"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Velocidade"
)

producer = get_producer()

try:
    r = requests.get(URL, headers=HEADERS, timeout=30)

    if r.status_code == 200:
        producer.send(TOPIC, r.json())
        producer.flush()
        print("Velocidade enviada")
    else:
        print(f"Falha ao buscar velocidade: status {r.status_code}")

except requests.exceptions.RequestException as e:
    print(f"Erro de rede ao buscar velocidade: {e}")

finally:
    producer.close()