import os
import time
from collections import deque
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer
from shared.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

TOPIC = "sptrans-linhas"
TOKEN = os.getenv("SafeMooveTOKENolhovivo")
REQUEST_INTERVAL = float(os.getenv("LINHAS_REQUEST_INTERVAL", "0.8"))

# sem valor, roda a descoberta completa (padrão de produção)
MAX_LINHAS = os.getenv("LINHAS_MAX_ENCONTRADAS")
MAX_LINHAS = int(MAX_LINHAS) if MAX_LINHAS else None


def main():
    if not TOKEN:
        raise Exception("Token da SPTrans não encontrado no ambiente (.env).")

    api = SPTransAPI(TOKEN)
    producer = get_producer()

    linhas_encontradas = {}
    seen_terms = set()
    queue = deque()

    for i in range(10000):
        queue.append(str(i))

    for p in list("NABCDEFGHIJR"):
        queue.append(p)

    try:
        while queue:
            termo = queue.popleft()

            if termo in seen_terms:
                continue

            seen_terms.add(termo)

            try:
                data = api.buscar_linhas(termo)

                if data:
                    for linha in data:
                        chave = linha.get("cl")
                        if not chave or chave in linhas_encontradas:
                            continue

                        payload = {
                            "codigo_linha": linha.get("cl"),
                            "circular": linha.get("lc"),
                            "letreiro": linha.get("lt"),
                            "sentido": linha.get("sl"),
                            "tipo": linha.get("tl"),
                            "origem": linha.get("tp"),
                            "destino": linha.get("ts")
                        }

                        linhas_encontradas[chave] = payload
                        producer.send(TOPIC, payload)

                        logger.info(f"encontrada: {linha.get('lt')}")

                        lt = linha.get("lt")

                        if lt and lt not in seen_terms:
                            queue.append(lt)

                        # variações reais de linha, ex: 546J-10
                        tl = linha.get("tl")
                        if lt and tl is not None:
                            variant = f"{lt}-{tl}"
                            if variant not in seen_terms:
                                queue.append(variant)

                logger.info(f"{termo} processado ({len(linhas_encontradas)} linhas até agora)")

                if MAX_LINHAS and len(linhas_encontradas) >= MAX_LINHAS:
                    logger.info(f"Limite de {MAX_LINHAS} linhas atingido, encerrando descoberta.")
                    break

            except Exception as e:
                logger.error(f"Erro inesperado em {termo}: {e}")

            time.sleep(REQUEST_INTERVAL)

    finally:
        producer.flush()
        producer.close()

    logger.info(f"Total linhas encontradas: {len(linhas_encontradas)}")


if __name__ == "__main__":
    main()
