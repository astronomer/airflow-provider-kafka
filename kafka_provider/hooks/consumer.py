from typing import Any, Dict, Optional, List

from confluent_kafka import Consumer

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook



def client_required(method):
    def inner(ref,*args,**kwargs):
        if not ref.consumer:
            ref.get_consumer()
        return method(ref,*args,**kwargs)
    return inner

class ConsumerHook(BaseHook):
    """
    A hook to create a Kafka Producer
    """

    default_conn_name = 'kafka_default'

    def __init__(
        self,
        topics:List[str],
        kafka_conn_id: Optional[str] = None,
        config: Optional[Dict[Any,Any]] = None
    ) -> None:
        super().__init__()

        self.kafka_conn_id = kafka_conn_id
        self.config = config
        self.topics = topics
        self.consumer = None

        if not (config.get('bootstrap.servers',None) or self.kafka_conn_id ):
            raise AirflowException(f"One of config['bootsrap.servers'] or kafka_conn_id must be provided.")

        if config.get('bootstrap.servers',None) and self.kafka_conn_id :
            raise AirflowException(f"One of config['bootsrap.servers'] or kafka_conn_id must be provided.")
        


    def get_consumer(self) -> None:
        """
        Returns http session to use with requests.

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        extra_configs = {}
        if self.kafka_conn_id:
            conn = self.get_connection(self.kafka_conn_id)
            extra_configs = {'bootstrap.servers':conn}
        
        consumer = Consumer({**extra_configs,**self.config})
        consumer.subscribe(topics)

        self.consumer = consumer
        return



        




        

