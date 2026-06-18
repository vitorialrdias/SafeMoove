import json
from datetime import datetime

from shared.aws_config import get_s3_client
from shared.kafka_config import get_consumer

BUCKET = "safe-moove-raw"

# MAPEAMENTO DOS TÓPICOS → PASTAS S3
TOPIC_PATH = {
    "sptrans-linhas": "linhas",
    "sptrans-paradas": "paradas",
    "sptrans-posicoes": "posicoes",
    "sptrans-previsoes": "previsoes",
    "sptrans-corredores": "corredores",
    "sptrans-velocidade": "velocidade"
}

consumer = get_consumer(
    topic=list(TOPIC_PATH.keys()),
    group_id="s3-group"
)

s3 = get_s3_client()

print("Consumer S3 iniciado...")

for msg in consumer:

    try:
        payload = msg.value
        topic = msg.topic

        pasta = TOPIC_PATH.get(topic, "unknown")

        agora = datetime.utcnow()

        key = (
            f"raw/{pasta}/"
            f"ano={agora:%Y}/"
            f"mes={agora:%m}/"
            f"dia={agora:%d}/"
            f"{agora:%H%M%S%f}.json"
        )

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"[{topic}] salvo em s3://{BUCKET}/{key}")

    except Exception as e:
        print(f"Erro no consumo: {e}")