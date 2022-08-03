from functools import partial
from typing import Any, Callable, Dict, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow_provider_kafka.hooks.consumer import KafkaConsumerHook
from airflow_provider_kafka.shared_utils import get_callable

VALID_COMMIT_CADENCE = {"never", "end_of_batch", "end_of_operator"}


class ConsumeFromTopicOperator(BaseOperator):
    """ConsumeFromTopicOperator An operator that consumes from Kafka a topic(s) and processing the messages.

    The operator creates a Kafka consumer that reads a batch of messages from the cluster and processes them
    using the user supplied callable function. The consumer will continue to read in batches until it reaches
    the end of the log or reads a maximum number of messages is reached.

    :param topics: A list of topics or regex patterns the consumer should subsrcribe to.
    :type topics: Sequence[str]
    :param apply_function: The function that should be applied to all messages fetched.
    :type apply_function: Union[Callable[..., Any], str]
    :param apply_function_args: Additional arguments that should be applied to the callable, defaults to None
    :type apply_function_args: Optional[Sequence[Any]], optional
    :param apply_function_kwargs: Additional key word arguments that should be applied to the callable
        , defaults to None
    :type apply_function_kwargs: Optional[Dict[Any, Any]], optional
    :param kafka_conn_id: The airflow connection id to fetch kafka brokers from, defaults to None
    :type kafka_conn_id: Optional[str], optional
    :param consumer_config: the config dictionary for the kafka client (additional information available on the
        confluent-python-kafka documentation), defaults to None, defaults to None
    :type consumer_config: Optional[Dict[Any, Any]], optional
    :param commit_cadence: When consumers should commit offsets ("never", "end_of_batch","end_of_operator"),
         defaults to "end_of_operator"
    :type commit_cadence: Optional[str], optional
    :param max_messages: The maximum total number of messages an operator should read from Kafka,
         defaults to None
    :type max_messages: Optional[int], optional
    :param max_batch_size: The maximum number of messages a consumer should read when polling, defaults to 1000
    :type max_batch_size: int, optional
    :param poll_timeout: How long the Kafka consumer should wait before determining no more messages are available,
         defaults to 60
    :type poll_timeout: Optional[float], optional
    """

    BLUE = "#ffefeb"
    ui_color = BLUE
    template_fields = ('topics', 'apply_function', 'apply_function_args', 'apply_function_kwargs')
    def __init__(
        self,
        topics: Sequence[str],
        apply_function: Union[Callable[..., Any], str],
        apply_function_args: Optional[Sequence[Any]] = None,
        apply_function_kwargs: Optional[Dict[Any, Any]] = None,
        kafka_conn_id: Optional[str] = None,
        consumer_config: Optional[Dict[Any, Any]] = None,
        commit_cadence: Optional[str] = "end_of_operator",
        max_messages: Optional[int] = None,
        max_batch_size: int = 1000,
        poll_timeout: Optional[float] = 60,
        **kwargs: Any,
    ) -> None:

        super().__init__(**kwargs)

        self.topics = topics
        self.apply_function = apply_function
        self.apply_function_args = apply_function_args or ()
        self.apply_function_kwargs = apply_function_kwargs or {}
        self.kafka_conn_id = kafka_conn_id
        self.config = consumer_config or {}
        self.commit_cadence = commit_cadence
        self.max_messages = max_messages or True
        self.max_batch_size = max_batch_size
        self.poll_timeout = poll_timeout

        if self.commit_cadence not in VALID_COMMIT_CADENCE:
            raise AirflowException(
                f"commit_cadence must be one of {VALID_COMMIT_CADENCE}. Got {self.commit_cadence}"
            )

        if self.max_messages and self.max_batch_size > self.max_messages:
            self.log.warning(
                f"max_batch_size ({self.max_batch_size}) > max_messages"
                f" ({self.max_messages}). Setting max_messages to"
                f" {self.max_batch_size}"
            )

        if self.commit_cadence == "never":
            self.commit_cadence = None

    def execute(self, context) -> Any:

        consumer = KafkaConsumerHook(
            topics=self.topics, kafka_conn_id=self.kafka_conn_id, config=self.config
        ).get_consumer()

        if isinstance(self.apply_function, str):
            self.apply_function = get_callable(self.apply_function)

        apply_callable = self.apply_function
        apply_callable = partial(apply_callable, *self.apply_function_args, **self.apply_function_kwargs)

        messages_left = self.max_messages
        messages_processed = 0

        while messages_left > 0:  # bool(True > 0) == True

            if not isinstance(messages_left, bool):
                batch_size = self.max_batch_size if messages_left > self.max_batch_size else messages_left
            else:
                batch_size = self.max_batch_size

            msgs = consumer.consume(num_messages=batch_size, timeout=self.poll_timeout)
            messages_left -= len(msgs)
            messages_processed += len(msgs)

            if not msgs:  # No messages + messages_left is being used.
                self.log.info("Reached end of log. Exiting.")
                break

            for m in msgs:
                apply_callable(m)

            if self.commit_cadence == "end_of_batch":
                self.log.info(f"committing offset at {self.commit_cadence}")
                consumer.commit()

        if self.commit_cadence:
            self.log.info(f"committing offset at {self.commit_cadence}")
            consumer.commit()

        consumer.close()

        return
