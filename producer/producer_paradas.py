import os
import time
import string
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("TOKEN_SPTRANS")
TOPIC = "sptrans-paradas"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

BASE_URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Parada/Buscar"
)

REQUEST_INTERVAL = float(os.getenv("PARADAS_REQUEST_INTERVAL", "0.3"))

# A API não tem endpoint de listagem total, então cobrimos o máximo
# possível buscando por dígitos e letras (igual ao producer_linhas.py).
TERMOS_BUSCA = list(string.digits) + list(string.ascii_lowercase)

producer = get_producer()
paradas_encontradas = {}

for termo in TERMOS_BUSCA:

    try:
        r = requests.get(
            BASE_URL,
            headers=HEADERS,
            params={"termosBusca": termo},
            timeout=30
        )

        if r.status_code != 200:
            print(f"'{termo}' -> status {r.status_code}, ignorado")
            time.sleep(REQUEST_INTERVAL)
            continue

        for parada in r.json():

            chave = parada.get("cp")
            if chave is None or chave in paradas_encontradas:
                continue

            payload = {
                "codigo_parada": parada.get("cp"),
                "nome": parada.get("np"),
                "latitude": parada.get("py"),
                "longitude": parada.get("px")
            }

            paradas_encontradas[chave] = payload
            producer.send(TOPIC, payload)

        print(f"'{termo}' processado ({len(paradas_encontradas)} paradas até agora)")

    except requests.exceptions.RequestException as e:
        print(f"Erro de rede em '{termo}': {e}")

    except Exception as e:
        print(f"Erro inesperado em '{termo}': {e}")

    time.sleep(REQUEST_INTERVAL)

producer.flush()
producer.close()

print(f"Total paradas: {len(paradas_encontradas)}")