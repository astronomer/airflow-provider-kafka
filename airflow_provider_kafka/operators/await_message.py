from typing import Any, Dict, Optional, Sequence

from airflow.models import BaseOperator

from airflow_provider_kafka.triggers.await_message import AwaitMessageTrigger

VALID_COMMIT_CADENCE = {"never", "end_of_batch", "end_of_operator"}


class AwaitKafkaMessageOperator(BaseOperator):
    """AwaitKafkaMessageOperator An Airflow operator that defers until a specific message is published to Kafka.

    The behavior of the consumer for this trigger is as follows:
    - poll the Kafka topics for a message
    - if no message returned, sleep
    - process the message with provided callable and commit the message offset
    - if callable returns any data, raise a TriggerEvent with the return data
    - else continue to next message
    - return event (as default xcom or specific xcom key)

    :param topics: Topics (or topic regex) to use for reading from
    :type topics: Sequence[str]
    :param apply_function: The functoin to apply to messages to determine if an event occurred. As a dot
    notation string.
    :type apply_function: str
    :param apply_function_args: Arguments to be applied to the processing function, defaults to None
    :type apply_function_args: Optional[Sequence[Any]], optional
    :param apply_function_kwargs: Key word arguments to be applied to the processing function,, defaults to None
    :type apply_function_kwargs: Optional[Dict[Any, Any]], optional
    :param kafka_conn_id: The airflow connection storing the Kafka broker address, defaults to None
    :type kafka_conn_id: Optional[str], optional
    :param kafka_config: the config dictionary for the kafka client (additional information available on the
    confluent-python-kafka documentation), defaults to None
    :type kafka_config: Optional[Dict[Any, Any]], optional
    :param poll_timeout: How long the kafka consumer should wait for a message to arrive from the kafka cluster,
         defaults to 1
    :type poll_timeout: float, optional
    :param poll_interval: How long the kafka consumer should sleep after reaching the end of the Kafka log,
         defaults to 5
    :type poll_interval: float, optional
    :param xcom_push_key: the name of a key to push the returned message to, defaults to None
    :type xcom_push_key: _type_, optional
    """

    BLUE = "#ffefeb"
    ui_color = BLUE

    template_fields = ('topics', 'apply_function', 'apply_function_args', 'apply_function_kwargs')
    
    def __init__(
        self,
        topics: Sequence[str],
        apply_function: str,
        apply_function_args: Optional[Sequence[Any]] = None,
        apply_function_kwargs: Optional[Dict[Any, Any]] = None,
        kafka_conn_id: Optional[str] = None,
        kafka_config: Optional[Dict[Any, Any]] = None,
        poll_timeout: float = 1,
        poll_interval: float = 5,
        xcom_push_key=None,
        **kwargs: Any,
    ) -> None:

        super().__init__(**kwargs)

        self.topics = topics
        self.apply_function = apply_function
        self.apply_function_args = apply_function_args
        self.apply_function_kwargs = apply_function_kwargs
        self.kafka_conn_id = kafka_conn_id
        self.kafka_config = kafka_config
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval
        self.xcom_push_key = xcom_push_key

    def execute(self, context) -> Any:

        self.defer(
            trigger=AwaitMessageTrigger(
                topics=self.topics,
                apply_function=self.apply_function,
                apply_function_args=self.apply_function_args,
                apply_function_kwargs=self.apply_function_kwargs,
                kafka_conn_id=self.kafka_conn_id,
                kafka_config=self.kafka_config,
                poll_timeout=self.poll_timeout,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        if self.xcom_push_key:
            self.xcom_push(context, key=self.xcom_push_key, value=event)
        return event
