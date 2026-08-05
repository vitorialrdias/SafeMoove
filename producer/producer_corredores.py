import os
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer
from shared.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

TOPIC = "sptrans-corredores"
TOKEN = os.getenv("SafeMooveTOKENolhovivo")


def main():
    if not TOKEN:
        raise Exception("Token da SPTrans não encontrado no ambiente (.env).")

    api = SPTransAPI(TOKEN)
    producer = get_producer()

    try:
        dados = api.listar_corredores()

        if dados:
            for corredor in dados:
                payload = {
                    "codigo_corredor": corredor.get("cc"),
                    "nome_corredor": corredor.get("nc"),
                }
                producer.send(TOPIC, payload)

            producer.flush()
            logger.info(f"Corredores enviados com sucesso! Total: {len(dados)}")
        else:
            logger.warning("Nenhum corredor retornado pela API.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
