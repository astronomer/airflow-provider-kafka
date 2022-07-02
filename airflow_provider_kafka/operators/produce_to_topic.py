import logging
from functools import partial
from typing import Any, Callable, Dict, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow_provider_kafka.hooks.producer import KafkaProducerHook
from airflow_provider_kafka.shared_utils import get_callable

local_logger = logging.getLogger("airflow")


def acked(err, msg):
    if err is not None:
        local_logger.error(f"Failed to deliver message: {err}")
    else:
        local_logger.info(
            f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}"
        )


class ProduceToTopicOperator(BaseOperator):
    """ProduceToTopicOperator An operator that produces messages to a Kafka topic

    :param topic: The topic the producer should produce to, defaults to None
    :type topic: str, optional
    :param producer_function: The function that generates key/value pairs as messages for production, defaults to None
    :type producer_function: Union[str, Callable[..., Any]], optional
    :param producer_function_args: Additional arguments to be applied to the producer callable, defaults to None
    :type producer_function_args: Optional[Sequence[Any]], optional
    :param producer_function_kwargs: Additional keyword arguments to be applied to the producer callable,
        defaults to None
    :type producer_function_kwargs: Optional[Dict[Any, Any]], optional
    :param delivery_callback: The callback to apply after delivery(or failure) of a message, defaults to None
    :type delivery_callback: Optional[str], optional
    :param kafka_conn_id: The airflow connection to get brokers address from, defaults to None
    :type kafka_conn_id: Optional[str], optional
    :param synchronous: If writes to kafka should be fully synchronous, defaults to True
    :type synchronous: Optional[bool], optional
    :param kafka_config: the config dictionary for the kafka client (additional information available on the
        confluent-python-kafka documentation), defaults to None
    :type kafka_config: Optional[Dict[Any, Any]], optional
    :param poll_timeout: How long of a delay should be applied when calling poll after production to kafka,
         defaults to 0
    :type poll_timeout: float, optional
    :raises AirflowException: _description_
    """
    
    template_fields = ('topic', 'producer_function', 'producer_function_args', 'producer_function_kwargs')

    def __init__(
        self,
        topic: str = None,
        producer_function: Union[str, Callable[..., Any]] = None,
        producer_function_args: Optional[Sequence[Any]] = None,
        producer_function_kwargs: Optional[Dict[Any, Any]] = None,
        delivery_callback: Optional[str] = None,
        kafka_conn_id: Optional[str] = None,
        synchronous: Optional[bool] = True,
        kafka_config: Optional[Dict[Any, Any]] = None,
        poll_timeout: float = 0,
        **kwargs: Any,
    ) -> None:

        super().__init__(**kwargs)

        if delivery_callback:
            dc = get_callable(delivery_callback)
        else:
            dc = acked

        self.kafka_conn_id = kafka_conn_id
        self.kafka_config = kafka_config
        self.topic = topic
        self.producer_function = producer_function or ""
        self.producer_function_args = producer_function_args or ()
        self.producer_function_kwargs = producer_function_kwargs or {}
        self.delivery_callback = dc
        self.synchronous = synchronous
        self.poll_timeout = poll_timeout

        if not (self.topic and self.producer_function):
            raise AirflowException(
                "topic and producer_function must be provided. Got topic="
                + f"{self.topic} and producer_function={self.producer_function}"
            )

        return

    def execute(self, context) -> Any:

        # Get producer and callable
        producer = KafkaProducerHook(
            kafka_conn_id=self.kafka_conn_id, config=self.kafka_config
        ).get_producer()

        if isinstance(self.producer_function, str):
            self.producer_function = get_callable(self.producer_function)

        producer_callable = self.producer_function
        producer_callable = partial(
            producer_callable, *self.producer_function_args, **self.producer_function_kwargs
        )

        # For each returned k/v in the callable : publish and flush if needed.
        for k, v in producer_callable():
            producer.produce(self.topic, key=k, value=v, on_delivery=self.delivery_callback)
            producer.poll(self.poll_timeout)
            if self.synchronous:
                producer.flush()

        producer.flush()
