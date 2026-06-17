import time, os
from dotenv import load_dotenv
from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer

load_dotenv()

TOPIC = "sptrans-posicoes"

TOKEN = os.getenv("TOKEN-SPTRANS")
print("TOKEN carregado:", TOKEN is not None)
api = SPTransAPI(TOKEN)

producer = get_producer()

codigo_linha = api.obter_codigo_linha("8000")

if not codigo_linha:
    raise Exception(
        "Não foi possível localizar a linha"
    )

while True:

    try:

        payload = api.obter_posicao(codigo_linha)

        if payload is None:
            print("Sem dados recebidos")
            time.sleep(10)
            continue

        producer.send(
            TOPIC,
            payload
        )

        producer.flush()

        print("Enviado para Kafka")

    except Exception as e:

        print(f"Erro: {e}")

    time.sleep(10)