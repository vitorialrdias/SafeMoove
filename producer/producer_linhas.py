import os
import time
import requests
from collections import deque
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOPIC = "sptrans-linhas"
TOKEN = os.getenv("TOKEN_SPTRANS")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

URL = (
    "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
    "sptrans/olhovivo/v2.1/Linha/Buscar"
)

REQUEST_INTERVAL = float(os.getenv("LINHAS_REQUEST_INTERVAL", "0.8"))

producer = get_producer()

# -----------------------------
# DATA STRUCTURES
# -----------------------------
linhas_encontradas = {}
seen_terms = set()

queue = deque()

# -----------------------------
# SEEDS INICIAIS REALISTAS
# -----------------------------

# numéricos base (cobre 1007, 2000 etc)
for i in range(10000):
    queue.append(str(i))

# prefixos reais
for p in list("NABCDEFGHIJR"):
    queue.append(p)

# -----------------------------
# PROCESSAMENTO
# -----------------------------

while queue:

    termo = queue.popleft()

    if termo in seen_terms:
        continue

    seen_terms.add(termo)

    try:
        r = requests.get(
            URL,
            headers=HEADERS,
            params={"termosBusca": termo},
            timeout=30
        )

        if r.status_code != 200:
            print(f"{termo} -> status {r.status_code}, ignorado")
            time.sleep(REQUEST_INTERVAL)
            continue

        data = r.json()

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

            print(f"✔ encontrada: {linha['lt']}")

            # -----------------------------
            # 🔥 EXPANSÃO INTELIGENTE
            # -----------------------------
            lt = linha.get("lt")

            if lt and lt not in seen_terms:
                queue.append(lt)

            # tenta variações reais de linha (ex: 546J-10)
            tl = linha.get("tl")
            if lt and tl is not None:
                variant = f"{lt}-{tl}"
                if variant not in seen_terms:
                    queue.append(variant)

        print(f"{termo} processado ({len(linhas_encontradas)} linhas até agora)")

    except requests.exceptions.RequestException as e:
        print(f"Erro de rede em {termo}: {e}")

    except Exception as e:
        print(f"Erro inesperado em {termo}: {e}")

    time.sleep(REQUEST_INTERVAL)

producer.flush()
producer.close()

print(f"Total linhas: {len(linhas_encontradas)}")
