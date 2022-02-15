## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        "package-name": "kafka_provider",  # Required
        "name": "Airflow Kafka",  # Required
        "description": "Airflow hooks and operators for Kafka",  # Required
        "hook-class-names": [
            "kafka.hooks.kafka_producer.KafkaProducer",
            "kafka.hooks.kafka_consumer.KafkaConsumer",
        ],
        "extra-links": [],
        "versions": ["0.0.1"],  # Required
    }


__version__ = "0.0.0"
