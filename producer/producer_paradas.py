import os
import time
import string
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer
from shared.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

TOPIC = "sptrans-paradas"
TOKEN = os.getenv("SafeMooveTOKENolhovivo")
REQUEST_INTERVAL = float(os.getenv("PARADAS_REQUEST_INTERVAL", "0.3"))

TERMOS_BUSCA = list(string.digits) + list(string.ascii_lowercase)


def main():
    if not TOKEN:
        raise Exception("Token da SPTrans não encontrado no ambiente (.env).")

    api = SPTransAPI(TOKEN)
    producer = get_producer()

    paradas_encontradas = {}

    try:
        for termo in TERMOS_BUSCA:
            try:
                dados = api.buscar_paradas(termo)

                if dados:
                    for parada in dados:
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

                logger.info(f"'{termo}' processado ({len(paradas_encontradas)} paradas até agora)")

            except Exception as e:
                logger.error(f"Erro inesperado em '{termo}': {e}")

            time.sleep(REQUEST_INTERVAL)

    finally:
        producer.flush()
        producer.close()

    logger.info(f"Total paradas encontradas: {len(paradas_encontradas)}")


if __name__ == "__main__":
    main()
