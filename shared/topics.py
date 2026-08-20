# Mapeamento único de tópicos Kafka -> pastas no S3.
# Usado pelo consumer_s3 e pelo create_topics para evitar duplicação.

TOPIC_PATH = {
    "sptrans-linhas": "linhas",
    "sptrans-posicoes": "posicoes",
    "sptrans-previsoes": "previsoes",
}

# Partições por tópico (mais frequentes ganham mais partições)
TOPIC_PARTITIONS = {
    "sptrans-linhas": 1,
    "sptrans-posicoes": 3,
    "sptrans-previsoes": 3,
}