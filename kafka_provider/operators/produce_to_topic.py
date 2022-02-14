from ast import Call
from email import message_from_string
from typing import Any, Callable, Dict, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from kafka_provider.hooks.producer import ProducerHook
from kafka_provider.shared_utils import get_callable


class ProduceToTopic(BaseOperator):

    BLUE = '#ffefeb'
    ui_color = BLUE

    def __init__(
        self,
        *,
        topic: str,
        producer_function: str,
        producer_function_args: Optiona[Sequence[Any]] = None,
        producer_function_kwargs: Optiona[Dict[Any,Any]] = None,
        delivery_callback: Optional[Callable[..., Dict[bytes,bytes]]] = None,
        kafka_conn_id: Optional[str] = None,
        syncronous: Optional[bool] = True,
        config: Optional[Dict[Any,Any]] = None,
        **kwargs: Any
        ) -> None:
        super().__init__(**kwargs)
        
        self.kafka_conn_id = kafka_conn_id
        self.config = config
        self.topic = topic
        self.producer_function = producer_function
        self.producer_function_args = producer_function_args
        self.producer_function_kwargs = producer_function_kwargs
        self.delivery_callback = delivery_callback or (lambda *args, **kwargs: None)
        self.syncronous = syncronous
        
    def execute(self) -> Any:

        # Get producer and callable
        producer = ProducerHook(kafka_conn_id=self.kafka_conn_id, config = self.config).get_producer()
        producer_callable = get_callable(self.producer_function)

        # For each returned k/v in the callable : publish and flush if needed.
        for k,v in producer_callable(*self.producer_function_args, **self.producer_function_kwargs):
            producer.produce(self.topic, key=k, value=v, on_delivery=self.delivery_callback)
            if self.syncronous:
                producer.flush() 
            else:
                producer.poll(0)
        
        producer.flush() 
        producer.close()
        
        pass