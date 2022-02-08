from ast import Call
from email import message_from_string
from typing import Any, Callable, Dict, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from kafka_provider.hooks.producer import ProducerHook



class ProduceTopic(BaseOperator):

    BLUE = '#ffefeb'
    ui_color = BLUE

    def __init__(
        self,
        *,
        topic: str,
        producer_function: Callable[...],
        delivery_callback: Optional[Callable[...]] = None,
        kafka_conn_id: Optional[str] = None,
        config: Optional[Dict[Any,Any]] = None,
        **kwargs: Any
        ) -> None:
        super().__init__(**kwargs)
        
        self.kafka_conn_id = kafka_conn_id
        self.config = config
        self.topic = topic
        self.producer_function = producer_function
        self.delivery_callback = delivery_callback
        
    def execute(self) -> Any:

        producer = ProducerHook(kafka_conn_id=self.kafka_conn_id, config = self.config).get_producer()

        for k,v in self.producer_function():
            producer.produce(self.topic, key=k, value=v, on_delivery=self.delivery_callback)
            producer.flush() # We're going full sync production here.
        
        producer.flush() #Being safe
        
        pass