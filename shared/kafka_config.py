from kafka import KafkaProducer, KafkaConsumer
import json

BOOTSTRAP_SERVERS = "kafka:9092"


def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def get_consumer(topics, group_id, enable_auto_commit=True):
    """
    topics: string com um único tópico OU lista/tupla de tópicos.
    """
    if isinstance(topics, str):
        topics = [topics]

    return KafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=enable_auto_commit,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )