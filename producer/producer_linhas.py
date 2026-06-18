import os
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOPIC = "sptrans-linhas"

TOKEN = os.getenv("TOKEN-SPTRANS")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Linha/Buscar"
)

producer = get_producer()

linhas_encontradas = {}

for numero in range(10000):

    termo = str(numero).zfill(4)

    try:

        r = requests.get(
            URL,
            headers=HEADERS,
            params={"termosBusca": termo},
            timeout=30
        )

        if r.status_code != 200:
            continue

        dados = r.json()

        for linha in dados:

            chave = linha["cl"]

            if chave not in linhas_encontradas:

                payload = {
                    "codigo_linha": linha["cl"],
                    "circular": linha["lc"],
                    "letreiro": linha["lt"],
                    "sentido": linha["sl"],
                    "tipo": linha["tl"],
                    "origem": linha["tp"],
                    "destino": linha["ts"]
                }

                linhas_encontradas[chave] = payload

                producer.send(
                    TOPIC,
                    payload
                )

        print(f"{termo} processado")

    except Exception as e:
        print(e)

producer.flush()

print(f"Total linhas: {len(linhas_encontradas)}")