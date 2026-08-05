import io
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import TopicPartition, OffsetAndMetadata

from shared.aws_config import get_s3_client
from shared.kafka_config import get_consumer
from shared.topics import TOPIC_PATH
from shared.schemas import SCHEMAS
from shared.logger import get_logger

logger = get_logger(__name__)

BUCKET = os.getenv("S3_BUCKET", "safe-moove-raw")
BATCH_SIZE = int(os.getenv("S3_BATCH_SIZE", "500"))
BATCH_INTERVAL = int(os.getenv("S3_BATCH_INTERVAL_SECONDS", "60"))

running = True


def _handle_shutdown(signum, frame):
    global running
    logger.info(f"Sinal {signum} recebido, encerrando após o próximo flush...")
    running = False


def flush_topic(consumer, s3, topic, buffers, pending_offsets):
    """Grava o buffer do tópico como um único arquivo Parquet no S3 e só
    então avança o offset das partições envolvidas (nunca antes)."""
    records = buffers.get(topic)
    if not records:
        return

    table = pa.Table.from_pylist(records, schema=SCHEMAS.get(topic))

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    pasta = TOPIC_PATH.get(topic, "unknown")
    agora = datetime.now(timezone.utc)
    key = (
        f"parquet/{pasta}/"
        f"ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/"
        f"{agora:%H%M%S%f}.parquet"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    # Só avança o offset das partições que acabaram de ser gravadas com
    # sucesso — nunca um consumer.commit() sem args, que avançaria a
    # posição de OUTROS tópicos com buffer ainda não persistido no S3.
    offsets = {
        TopicPartition(topic, partition): OffsetAndMetadata(offset + 1, "")
        for partition, offset in pending_offsets[topic].items()
    }
    consumer.commit(offsets)

    logger.info(f"[{topic}] {len(records)} registros gravados em s3://{BUCKET}/{key}")

    buffers[topic] = []
    pending_offsets[topic] = {}


def main():
    global running

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    consumer = get_consumer(
        topics=list(TOPIC_PATH.keys()),
        group_id="s3-group",
        enable_auto_commit=False,  # só avançamos o offset após gravar no S3
    )
    s3 = get_s3_client()

    buffers = defaultdict(list)
    pending_offsets = defaultdict(dict)
    last_flush = defaultdict(time.time)

    logger.info("Consumer S3 iniciado...")

    try:
        while running:
            registros = consumer.poll(timeout_ms=5000)

            for tp, mensagens in registros.items():
                for msg in mensagens:
                    buffers[tp.topic].append(msg.value)
                    pending_offsets[tp.topic][msg.partition] = msg.offset

            agora = time.time()

            for topic in list(buffers.keys()):
                atingiu_tamanho = len(buffers[topic]) >= BATCH_SIZE
                atingiu_tempo = buffers[topic] and (agora - last_flush[topic]) >= BATCH_INTERVAL

                if atingiu_tamanho or atingiu_tempo:
                    try:
                        flush_topic(consumer, s3, topic, buffers, pending_offsets)
                    except Exception as e:
                        # Sem commit: as mensagens são reprocessadas após reiniciar
                        # o consumer (mesmo group_id), em vez de perdidas silenciosamente.
                        logger.error(f"Erro ao gravar [{topic}] no S3, offset não avançado: {e}")
                    last_flush[topic] = agora

    finally:
        for topic in list(buffers.keys()):
            try:
                flush_topic(consumer, s3, topic, buffers, pending_offsets)
            except Exception as e:
                logger.error(f"Erro ao gravar [{topic}] no S3 durante encerramento: {e}")

        consumer.close()
        logger.info("Consumer S3 encerrado.")


if __name__ == "__main__":
    main()
