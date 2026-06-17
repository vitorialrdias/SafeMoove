from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)

topic = NewTopic(
    name="sptrans-posicoes",
    num_partitions=3,
    replication_factor=1
)

admin.create_topics([topic])