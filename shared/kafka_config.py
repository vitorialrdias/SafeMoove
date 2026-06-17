from kafka import KafkaProducer, KafkaConsumer
import json

BOOTSTRAP_SERVERS = "localhost:9092"


def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def get_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )