from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from shared.topics import TOPIC_PATH, TOPIC_PARTITIONS

BOOTSTRAP_SERVERS = "kafka:9092"
REPLICATION_FACTOR = 1


def main():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    for nome in TOPIC_PATH.keys():
        topic = NewTopic(
            name=nome,
            num_partitions=TOPIC_PARTITIONS.get(nome, 1),
            replication_factor=REPLICATION_FACTOR,
        )
        try:
            admin.create_topics([topic])
            print(f"Tópico '{nome}' criado")
        except TopicAlreadyExistsError:
            print(f"Tópico '{nome}' já existia, ignorando")
        except Exception as e:
            print(f"Erro ao criar tópico '{nome}': {e}")

    admin.close()


if __name__ == "__main__":
    main()