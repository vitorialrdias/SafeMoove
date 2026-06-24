import os
import time
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("TOKEN_SPTRANS")
TOPIC = "sptrans-posicoes"

LINHA_BUSCA = os.getenv("LINHA_POSICOES", "8000")
POLL_INTERVAL = int(os.getenv("POSICOES_POLL_INTERVAL", "10"))

api = SPTransAPI(TOKEN)
producer = get_producer()

codigo_linha = api.obter_codigo_linha(LINHA_BUSCA)

if not codigo_linha:
    raise Exception(f"Linha '{LINHA_BUSCA}' não encontrada")

print(f"Monitorando posições da linha {LINHA_BUSCA} (código {codigo_linha})")

while True:

    try:
        payload = api.obter_posicao(codigo_linha)

        if payload:
            producer.send(TOPIC, payload)
            producer.flush()
            print("Posições enviadas")

    except Exception as e:
        print(f"Erro ao obter/enviar posições: {e}")

    time.sleep(POLL_INTERVAL)
