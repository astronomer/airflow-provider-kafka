from typing import Any, Dict, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaAdminClientHook(BaseHook):
    """
    A hook to create a Kafka Producer
    """

    default_conn_name = "kafka_default"

    def __init__(self, kafka_conn_id: Optional[str] = None, config: Optional[Dict[Any, Any]] = None) -> None:
        super().__init__()

        self.kafka_conn_id = kafka_conn_id
        self.config: Dict[Any, Any] = config or {}

        self.extra_configs = {}

        if self.kafka_conn_id:
            conn = self.get_connection(self.kafka_conn_id)
            self.extra_configs = {"bootstrap.servers": conn}

        if not (self.config.get("bootstrap.servers", None) or self.kafka_conn_id):
            raise AirflowException("One of config['bootsrap.servers'] or kafka_conn_id must be provided.")

        if self.config.get("bootstrap.servers", None) and self.kafka_conn_id:
            raise AirflowException("One of config['bootsrap.servers'] or kafka_conn_id must be provided.")

    def get_admin_client(self) -> AdminClient:
        """
        Returns http session to use with requests.

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """

        return AdminClient({**self.config, **self.extra_configs})

    def create_topic(
        self,
        topics: Sequence[Sequence[Any]],
    ) -> None:

        admin_client = self.get_admin_client()

        new_topics = [NewTopic(t[0], num_partitions=t[1], replication_factor=t[2]) for t in topics]

        futures = admin_client.create_topics(new_topics)

        for t, f in futures.items():
            try:
                f.result()
                self.log.info(f"The topic {t} has been created.")
            except Exception as e:
                if e.args[0].name() == "TOPIC_ALREADY_EXISTS":
                    self.log.warning(f"The topic {t} already exists.")
                    pass
