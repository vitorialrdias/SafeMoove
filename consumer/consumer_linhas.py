import json

from datetime import datetime

from shared.aws_config import get_s3_client
from shared.kafka_config import get_consumer

BUCKET = "safe-moove-raw"

consumer = get_consumer(
    topic="sptrans-linhas",
    group_id="linhas-group"
)

s3 = get_s3_client()

print("Consumindo linhas...")

for msg in consumer:

    payload = msg.value

    agora = datetime.utcnow()
    letreiro = str(payload["letreiro"]).replace("/", "-")

    key = (
        f"linhas/"
        f"ano={agora:%Y}/"
        f"mes={agora:%m}/"
        f"dia={agora:%d}/"
        f"{letreiro}-{agora:%Y%m%d_%H%M%S}_{agora.microsecond // 1000:03d}.json"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )

    print(f"Arquivo enviado: s3://{BUCKET}/{key}")