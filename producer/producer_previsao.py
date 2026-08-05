import os
import time
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer, get_consumer

load_dotenv()

TOKEN = os.getenv("SafeMooveTOKENolhovivo")
TOPIC = "sptrans-previsoes"
LINHAS_TOPIC = "sptrans-linhas"

REQUEST_INTERVAL = float(os.getenv("PREVISOES_REQUEST_INTERVAL", "1.0"))
REFRESH_INTERVAL = int(os.getenv("PREVISOES_REFRESH_INTERVAL", "300"))

if not TOKEN:
    raise Exception("Token da SPTrans não encontrado no ambiente (.env).")

api = SPTransAPI(TOKEN)
producer = get_producer()

# Consumer próprio (group_id dedicado) para ler o cadastro de linhas
# publicado pelo producer_linhas, sem interferir no consumer_s3.
linhas_consumer = get_consumer(
    LINHAS_TOPIC,
    group_id="previsao-linhas-reader",
    enable_auto_commit=True,
)

linhas_conhecidas = {}


def atualizar_linhas_conhecidas():
    """Lê (sem bloquear) as mensagens disponíveis no tópico de linhas."""
    registros = linhas_consumer.poll(timeout_ms=2000)

    for mensagens in registros.values():
        for msg in mensagens:
            codigo = msg.value.get("codigo_linha")
            if codigo:
                linhas_conhecidas[codigo] = msg.value


print("Aguardando linhas publicadas em 'sptrans-linhas'...")

while not linhas_conhecidas:
    atualizar_linhas_conhecidas()
    if not linhas_conhecidas:
        print("Nenhuma linha encontrada ainda. Aguardando producer_linhas publicar dados...")
        time.sleep(5)

print(f"{len(linhas_conhecidas)} linhas conhecidas. Iniciando ciclo de previsões...")

ultimo_refresh = time.time()

while True:
    for codigo_linha in list(linhas_conhecidas.keys()):
        try:
            previsao = api.obter_previsao(codigo_linha)

            if previsao:
                previsao["codigo_linha"] = codigo_linha
                producer.send(TOPIC, previsao)

        except Exception as e:
            print(f"Erro ao obter previsão da linha {codigo_linha}: {e}")

        time.sleep(REQUEST_INTERVAL)

    producer.flush()
    print(f"Ciclo completo de previsões ({len(linhas_conhecidas)} linhas).")

    if time.time() - ultimo_refresh >= REFRESH_INTERVAL:
        atualizar_linhas_conhecidas()
        ultimo_refresh = time.time()
