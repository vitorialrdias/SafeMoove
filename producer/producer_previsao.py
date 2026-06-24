import os
import time
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer
from producer.api_sptrans import SPTransAPI

load_dotenv()

TOKEN = os.getenv("TOKEN_SPTRANS")
TOPIC = "sptrans-previsoes"

LINHA_BUSCA = os.getenv("LINHA_PREVISOES", "8000")
POLL_INTERVAL = int(os.getenv("PREVISOES_POLL_INTERVAL", "15"))

api = SPTransAPI(TOKEN)
producer = get_producer()

codigo_linha = api.obter_codigo_linha(LINHA_BUSCA)

if not codigo_linha:
    raise Exception(f"Linha '{LINHA_BUSCA}' não encontrada")

URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Previsao/Linha"
)

print(f"Monitorando previsões da linha {LINHA_BUSCA} (código {codigo_linha})")

while True:

    try:
        r = requests.get(
            URL,
            headers=api.headers,
            params={"codigoLinha": codigo_linha},
            timeout=30
        )

        if r.status_code == 200:
            producer.send(TOPIC, r.json())
            producer.flush()
            print("Previsões enviadas")
        else:
            print(f"Falha ao buscar previsões: status {r.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ao obter previsões: {e}")

    except Exception as e:
        print(f"Erro inesperado ao obter previsões: {e}")

    time.sleep(POLL_INTERVAL)