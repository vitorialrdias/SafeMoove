import os
import time
from dotenv import load_dotenv

from producer.api_sptrans import SPTransAPI
from shared.kafka_config import get_producer, get_consumer
from shared.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

TOPIC = "sptrans-previsoes"
LINHAS_TOPIC = "sptrans-linhas"
TOKEN = os.getenv("SafeMooveTOKENolhovivo")

REQUEST_INTERVAL = float(os.getenv("PREVISOES_REQUEST_INTERVAL", "1.0"))
REFRESH_INTERVAL = int(os.getenv("PREVISOES_REFRESH_INTERVAL", "100"))


def atualizar_linhas_conhecidas(linhas_consumer, linhas_conhecidas):
    """Lê (sem bloquear) as mensagens disponíveis no tópico de linhas."""
    registros = linhas_consumer.poll(timeout_ms=2000)

    for mensagens in registros.values():
        for msg in mensagens:
            codigo = msg.value.get("codigo_linha")
            if codigo:
                linhas_conhecidas[codigo] = msg.value


def achatar_previsao(previsao, codigo_linha, ciclo):
    """
    /Previsao/Linha retorna aninhado:
    {hr, ps: [{cp, np, py, px, vs: [{p, t, a, ta, py, px}]}]}
    Achata para uma mensagem por (parada, veículo previsto).
    """
    horario_consulta = previsao.get("hr")
    registros = []

    for parada in previsao.get("ps", []):
        for veiculo in parada.get("vs", []):
            registros.append({
                "ciclo": ciclo,
                "horario_consulta": horario_consulta,
                "codigo_linha": codigo_linha,
                "codigo_parada": parada.get("cp"),
                "nome_parada": parada.get("np"),
                "latitude_parada": parada.get("py"),
                "longitude_parada": parada.get("px"),
                "prefixo_veiculo": str(veiculo.get("p")) if veiculo.get("p") is not None else None,
                "horario_previsto": veiculo.get("t"),
                "acessivel": veiculo.get("a"),
                "timestamp_previsao": veiculo.get("ta"),
                "latitude_veiculo": veiculo.get("py"),
                "longitude_veiculo": veiculo.get("px"),
            })

    return registros


def main():
    if not TOKEN:
        raise Exception("Token da SPTrans não encontrado no ambiente (.env).")

    api = SPTransAPI(TOKEN)
    producer = get_producer()

    # group_id proprio para nao interferir no offset do consumer_s3
    linhas_consumer = get_consumer(
        LINHAS_TOPIC,
        group_id="previsao-linhas-reader",
        enable_auto_commit=True,
    )

    linhas_conhecidas = {}

    logger.info("Aguardando linhas publicadas em 'sptrans-linhas'...")

    while not linhas_conhecidas:
        atualizar_linhas_conhecidas(linhas_consumer, linhas_conhecidas)
        if not linhas_conhecidas:
            logger.info("Nenhuma linha encontrada ainda. Aguardando producer_linhas publicar dados...")
            time.sleep(5)

    logger.info(f"{len(linhas_conhecidas)} linhas conhecidas. Iniciando ciclo de previsões...")

    ultimo_refresh = time.time()
    ciclo = 0

    while True:
        ciclo += 1

        for codigo_linha in list(linhas_conhecidas.keys()):
            try:
                previsao = api.obter_previsao(codigo_linha)

                if previsao:
                    for registro in achatar_previsao(previsao, codigo_linha, ciclo):
                        producer.send(TOPIC, registro)

            except Exception as e:
                logger.error(f"Erro ao obter previsão da linha {codigo_linha}: {e}")

            time.sleep(REQUEST_INTERVAL)

        producer.flush()
        logger.info(f"Ciclo {ciclo} completo de previsões ({len(linhas_conhecidas)} linhas).")

        if time.time() - ultimo_refresh >= REFRESH_INTERVAL:
            atualizar_linhas_conhecidas(linhas_consumer, linhas_conhecidas)
            ultimo_refresh = time.time()


if __name__ == "__main__":
    main()
