import os
import requests
from dotenv import load_dotenv

from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("SafeMooveTOKENolhovivo") or os.getenv("SafeMooveTOKENolhovivo")
TOPIC = "sptrans-velocidade"

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
# PROCESSAMENTO DE VELOCIDADE
# -----------------------------
producer = get_producer()
url_velocidade = f"{BASE_URL}/Velocidade"

try:
    # Usa a sessão autenticada que repassa o cookie apiCredentials automaticamente
    r = session.get(url_velocidade, timeout=30)

    if r.status_code == 200:
        dados = r.json()

        if isinstance(dados, list):
            for item in dados:
                producer.send(TOPIC, item)
        else:
            producer.send(TOPIC, dados)

        producer.flush()
        print("Dados de velocidade enviados com sucesso!")
    else:
        print(f"Falha ao buscar velocidade: status {r.status_code}")

except requests.exceptions.RequestException as e:
    print(f"Erro de rede ao buscar velocidade: {e}")

except Exception as e:
    print(f"Erro inesperado ao buscar velocidade: {e}")

finally:
    producer.close()
