import json
from datetime import datetime, timezone

from shared.aws_config import get_s3_client
from shared.kafka_config import get_consumer
from shared.topics import TOPIC_PATH

BUCKET = "safe-moove-raw"

consumer = get_consumer(
    topics=list(TOPIC_PATH.keys()),
    group_id="s3-group",
    enable_auto_commit=False,  # só avançamos o offset após gravar no S3
)

s3 = get_s3_client()

print("Consumer S3 iniciado...")

for msg in consumer:

    payload = msg.value
    topic = msg.topic
    pasta = TOPIC_PATH.get(topic, "unknown")

    agora = datetime.now(timezone.utc)

    key = (
        f"raw/{pasta}/"
        f"ano={agora:%Y}/"
        f"mes={agora:%m}/"
        f"dia={agora:%d}/"
        f"{agora:%H%M%S%f}.json"
    )

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False),
            ContentType="application/json"
        )

        consumer.commit()

        print(f"[{topic}] salvo em s3://{BUCKET}/{key}")

    except Exception as e:
        # Sem commit: a mensagem é reprocessada após reiniciar o consumer
        # (mesmo group_id), em vez de ser perdida silenciosamente.
        print(f"Erro ao salvar [{topic}] no S3, offset não avançado: {e}")