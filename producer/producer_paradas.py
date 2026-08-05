import os
import time
import string
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("SafeMooveTOKENolhovivo") or os.getenv("SafeMooveTOKENolhovivo")
TOPIC = "sptrans-paradas"

BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"
REQUEST_INTERVAL = float(os.getenv("PARADAS_REQUEST_INTERVAL", "0.3"))

# -----------------------------
# AUTENTICAÇÃO E SESSÃO SPTRANS
# -----------------------------
session = requests.Session()

def autenticar_sptrans():
    """Autentica na API Olho Vivo e armazena os cookies de sessão no objeto `session`."""
    url_auth = f"{BASE_URL}/Login/Autenticar"

    # POST com o token na URL e data={} para garantir a presença do header Content-Length: 0
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
# PROCESSAMENTO DE PARADAS
# -----------------------------
TERMOS_BUSCA = list(string.digits) + list(string.ascii_lowercase)

producer = get_producer()
paradas_encontradas = {}
url_buscar = f"{BASE_URL}/Parada/Buscar"

for termo in TERMOS_BUSCA:

    try:
        # Usa a sessão autenticada que repassa o cookie apiCredentials automaticamente
        r = session.get(
            url_buscar,
            params={"termosBusca": termo},
            timeout=30
        )

        if r.status_code != 200:
            print(f"'{termo}' -> status {r.status_code}, ignorado")
            time.sleep(REQUEST_INTERVAL)
            continue

        dados = r.json()

        if isinstance(dados, list):
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

        print(f"'{termo}' processado ({len(paradas_encontradas)} paradas até agora)")

    except requests.exceptions.RequestException as e:
        print(f"Erro de rede em '{termo}': {e}")

    except Exception as e:
        print(f"Erro inesperado em '{termo}': {e}")

    time.sleep(REQUEST_INTERVAL)

producer.flush()
producer.close()

print(f"Total paradas encontradas: {len(paradas_encontradas)}")
