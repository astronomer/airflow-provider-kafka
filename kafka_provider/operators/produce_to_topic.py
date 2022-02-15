from functools import partial
from typing import Any, Callable, Dict, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from kafka_provider.hooks.producer import ProducerHook
from kafka_provider.shared_utils import get_callable


class ProduceToTopic(BaseOperator):
    def __init__(
        self,
        topic: str = None,
        producer_function: str = None,
        producer_function_args: Optional[Sequence[Any]] = None,
        producer_function_kwargs: Optional[Dict[Any, Any]] = None,
        delivery_callback: Optional[Callable[..., Dict[bytes, bytes]]] = None,
        kafka_conn_id: Optional[str] = None,
        synchronous: Optional[bool] = True,
        kafka_config: Optional[Dict[Any, Any]] = None,
        no_broker: bool = False,
        flush_timeout: float = 0,
        poll_timeout: float = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.kafka_conn_id = kafka_conn_id
        self.kafka_config = kafka_config
        self.topic = topic
        self.producer_function: str = producer_function or ""
        self.producer_function_args = producer_function_args or ()
        self.producer_function_kwargs = producer_function_kwargs or {}
        self.delivery_callback = delivery_callback or (lambda *args, **kwargs: None)
        self.synchronous = synchronous
        self.no_broker = no_broker
        self.flush_timeout = flush_timeout
        self.poll_timeout = poll_timeout

        if not (self.topic and self.producer_function):
            raise AirflowException(
                "topic and producer_function must be provided. Got topic="
                + f"{self.topic} and producer_function={self.producer_function}"
            )

        return

    def execute(self, context) -> Any:

        # Get producer and callable
        producer = ProducerHook(
            kafka_conn_id=self.kafka_conn_id, config=self.kafka_config, no_broker=self.no_broker
        ).get_producer()
        producer_callable = get_callable(self.producer_function)
        producer_callable = partial(
            producer_callable, *self.producer_function_args, **self.producer_function_kwargs
        )

        # For each returned k/v in the callable : publish and flush if needed.
        for k, v in producer_callable():
            producer.produce(self.topic, key=k, value=v, on_delivery=self.delivery_callback)
            producer.poll(self.poll_timeout)
            if self.synchronous:
                while producer.flush(self.flush_timeout):
                    pass

        while producer.flush(self.flush_timeout):
            pass

        pass
