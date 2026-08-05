import os
import time
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer

load_dotenv()

TOKEN = os.getenv("SafeMooveTOKENolhovivo")
TOPIC = "sptrans-posicoes"

POLL_INTERVAL = int(os.getenv("POSICOES_POLL_INTERVAL", "10"))

if not TOKEN:
    raise Exception("Token da SPTrans não encontrado no ambiente (.env).")

api = SPTransAPI(TOKEN)
producer = get_producer()

print("Monitorando posições de todos os veículos em operação...")

while True:
    try:
        dados = api.obter_posicoes()

        if dados and isinstance(dados.get("l"), list):
            horario = dados.get("hr")
            total_veiculos = 0

            for linha in dados["l"]:
                for veiculo in linha.get("vs", []):
                    payload = {
                        "horario_consulta": horario,
                        "timestamp_veiculo": veiculo.get("ta"),
                        "codigo_linha": linha.get("cl"),
                        "letreiro": linha.get("c"),
                        "sentido": linha.get("sl"),
                        "origem": linha.get("lt0"),
                        "destino": linha.get("lt1"),
                        "prefixo_veiculo": veiculo.get("p"),
                        "acessivel": veiculo.get("a"),
                        "latitude": veiculo.get("py"),
                        "longitude": veiculo.get("px"),
                    }

                    producer.send(TOPIC, payload)
                    total_veiculos += 1

            producer.flush()
            print(f"Posições enviadas: {total_veiculos} veículos em {len(dados['l'])} linhas.")
        else:
            print("Aviso: dados de posição vazios ou sessão expirada. Reautenticando...")
            api.autenticar()

    except Exception as e:
        print(f"Erro ao obter/enviar posições: {e}")

    time.sleep(POLL_INTERVAL)
