import os
import time
import requests
from collections import deque
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOPIC = "sptrans-linhas"
TOKEN = os.getenv("SafeMooveTOKENolhovivo")

BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"
REQUEST_INTERVAL = float(os.getenv("LINHAS_REQUEST_INTERVAL", "0.8"))

producer = get_producer()

# -----------------------------
# AUTENTICAÇÃO E SESSÃO SPTRANS
# -----------------------------
session = requests.Session()

def autenticar_sptrans():
    """Autentica na API Olho Vivo e salva o cookie de sessão em `session`."""
    url_auth = f"{BASE_URL}/Login/Autenticar"

    # Exige POST com o token na URL e data={} para enviar Content-Length: 0
    res = session.post(url_auth, params={"token": TOKEN}, data={})

    if res.status_code == 200 and res.text.strip().lower() == "true":
        print("✔ Autenticação na SPTrans realizada com sucesso!")
        return True
    else:
        print(f"❌ Falha na autenticação: status {res.status_code} - {res.text}")
        return False

if not TOKEN or not autenticar_sptrans():
    raise Exception("Não foi possível autenticar na API Olho Vivo. Verifique o token fornecido.")

# -----------------------------
# DATA STRUCTURES
# -----------------------------
linhas_encontradas = {}
seen_terms = set()
queue = deque()

# -----------------------------
# SEEDS INICIAIS REALISTAS
# -----------------------------
# Numéricos base
for i in range(10000):
    queue.append(str(i))

# Prefixos reais
for p in list("NABCDEFGHIJR"):
    queue.append(p)

# -----------------------------
# PROCESSAMENTO DA FILA
# -----------------------------
url_buscar = f"{BASE_URL}/Linha/Buscar"

while queue:
    termo = queue.popleft()

    if termo in seen_terms:
        continue

    seen_terms.add(termo)

    try:
        # Usa a sessão autenticada que repassa o cookie automaticamente
        r = session.get(
            url_buscar,
            params={"termosBusca": termo},
            timeout=30
        )

        if r.status_code != 200:
            print(f"{termo} -> status {r.status_code}, ignorado")
            time.sleep(REQUEST_INTERVAL)
            continue

        data = r.json()

        if isinstance(data, list):
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

                print(f"✔ encontrada: {linha.get('lt')}")

                # -----------------------------
                # 🔥 EXPANSÃO INTELIGENTE
                # -----------------------------
                lt = linha.get("lt")

                if lt and lt not in seen_terms:
                    queue.append(lt)

                # Tenta variações reais de linha (ex: 546J-10)
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

print(f"Total linhas encontradas: {len(linhas_encontradas)}")
