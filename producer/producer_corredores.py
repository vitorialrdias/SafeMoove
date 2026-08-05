import os
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("SafeMooveTOKENolhovivo") or os.getenv("SafeMooveTOKENolhovivo")
TOPIC = "sptrans-corredores"

BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"

# -----------------------------
# AUTENTICAÇÃO E SESSÃO SPTRANS
# -----------------------------
session = requests.Session()

def autenticar_sptrans():
    """Autentica na API Olho Vivo e armazena os cookies de sessão no objeto `session`."""
    url_auth = f"{BASE_URL}/Login/Autenticar"

    # POST com o token na URL e data={} para garantir o header Content-Length: 0
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
# PROCESSAMENTO DE CORREDORES
# -----------------------------
producer = get_producer()
url_corredores = f"{BASE_URL}/Corredor"

try:
    # Usa a sessão autenticada que repassa o cookie apiCredentials automaticamente
    r = session.get(url_corredores, timeout=30)

    if r.status_code == 200:
        dados = r.json()

        # Envia cada corredor individualmente ou a lista completa
        if isinstance(dados, list):
            for corredor in dados:
                producer.send(TOPIC, corredor)
        else:
            producer.send(TOPIC, dados)

        producer.flush()
        print("Corredores enviados com sucesso!")
    else:
        print(f"Falha ao buscar corredores: status {r.status_code}")

except requests.exceptions.RequestException as e:
    print(f"Erro de rede ao buscar corredores: {e}")

except Exception as e:
    print(f"Erro inesperado ao buscar corredores: {e}")

finally:
    producer.close()
