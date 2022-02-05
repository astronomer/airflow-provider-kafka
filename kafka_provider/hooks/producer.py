from typing import Any, Dict, Optional,

from confluent_kafka import Producer

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

class ProducerHook(BaseHook):
    """
    A hook to create a Kafka Producer
    """

    default_conn_name = 'kafka_default'

    def __init__(
        self,
        kafka_conn_id: Optional[str] = None,
        config: Optional[Dict[Any,Any]] = None
    ) -> None:
        super().__init__()

        self.kafka_conn_id = kafka_conn_id
        self.config = config

        if (not config.get('bootstrap.servers',None) or self.kafka_conn_id ) or (config.get('bootstrap.servers',None) and self.kafka_conn_id) :
            raise AirflowException(f"One of config['bootsrap.servers'] or kafka_conn_id must be provided.")
        


    def get_producer(self) -> Producer:
        """
        Returns http session to use with requests.

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        extra_configs = {}
        if self.kafka_conn_id:
            conn = self.get_connection(self.kafka_conn_id)
            extra_configs = {'bootstrap.servers':conn}
        
        return Producer({**extra_configs,**self.config})
