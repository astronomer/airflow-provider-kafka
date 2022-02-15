from typing import Any, Dict, List, Optional
from xmlrpc.client import Boolean

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from confluent_kafka import Consumer


def client_required(method):
    def inner(ref, *args, **kwargs):
        if not ref.consumer:
            ref.get_consumer()
        return method(ref, *args, **kwargs)

    return inner


class ConsumerHook(BaseHook):
    """
    A hook to create a Kafka Producer
    """

    default_conn_name = "kafka_default"

    def __init__(
        self,
        topics: List[str],
        kafka_conn_id: Optional[str] = None,
        config: Optional[Dict[Any, Any]] = None,
        no_broker: Optional[bool] = False,
    ) -> None:
        super().__init__()

        self.kafka_conn_id = kafka_conn_id
        self.config = config
        self.topics = topics
        self.consumer = None
        self.no_broker = no_broker

        if not self.no_broker:
            if not self.config.get("group.id", None):
                raise AirflowException(
                    "The 'group.id' parameter must be set in the config dictionary'. Got <None>"
                )

            if not (self.config.get("bootstrap.servers", None) or self.kafka_conn_id):
                raise AirflowException(
                    f"One of config['bootsrap.servers'] or kafka_conn_id must be provided."
                )

        if self.config.get("bootstrap.servers", None) and self.kafka_conn_id:
            raise AirflowException(f"One of config['bootsrap.servers'] or kafka_conn_id must be provided.")

    def get_consumer(self) -> None:
        """
        Returns a Consumer that has been subscribed to topics.
        """
        extra_configs = {}
        if self.kafka_conn_id:
            conn = self.get_connection(self.kafka_conn_id)
            extra_configs = {"bootstrap.servers": conn}

        consumer = Consumer({**extra_configs, **self.config})
        consumer.subscribe(self.topics)

        self.consumer = consumer
        return self.consumer
