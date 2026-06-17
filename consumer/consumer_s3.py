import json

from datetime import datetime

from shared.aws_config import get_s3_client
from shared.kafka_config import get_consumer


BUCKET = "safe-moove-raw"

consumer = get_consumer(
    topic="sptrans-posicoes",
    group_id="s3-group"
)

s3 = get_s3_client()

for msg in consumer:

    payload = msg.value

    key = (
        f"raw/sptrans/"
        f"{datetime.now():%Y/%m/%d/%H/%M/%S}.json"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload)
    )

    print(f"Arquivo enviado: {key}")