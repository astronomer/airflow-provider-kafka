from email import message_from_string
from functools import partial
from typing import Any, Callable, Dict, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from kafka_provider.hooks.consumer import ConsumerHook
from kafka_provider.shared_utils import get_callable

VALID_COMMIT_CADENCE = {"never", "end_of_batch", "end_of_operator"}


class ConsumeFromTopic(BaseOperator):

    BLUE = "#ffefeb"
    ui_color = BLUE

    def __init__(
        self,
        topics: Sequence[str],
        apply_function: str,
        apply_function_args: Optional[Sequence[Any]] = None,
        apply_function_kwargs: Optional[Dict[Any, Any]] = None,
        kafka_conn_id: Optional[str] = None,
        consumer_config: Optional[Dict[Any, Any]] = None,
        commit_cadence: Optional[str] = "end_of_operator",
        max_messages: Optional[int] = None,
        max_batch_size: int = 1000,
        no_broker: Optional[bool] = False,
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
        self.no_broker = no_broker
        self.poll_timeout = poll_timeout

        if self.commit_cadence not in VALID_COMMIT_CADENCE:
            raise AirflowException(
                f"commit_cadence must be one of {VALID_COMMIT_CADENCE}. Got {self.commit_cadence}"
            )

        if self.max_messages and self.max_batch_size > self.max_messages:
            self.log.warn(
                f"max_batch_size ({self.max_batch_size}) > max_messages ({self.max_messages}). Setting max_messages to {self.max_batch_size}"
            )

        if self.commit_cadence == "never":
            self.commit_cadence = None

    def execute(self, context) -> Any:

        consumer = ConsumerHook(
            topics=self.topics, kafka_conn_id=self.kafka_conn_id, config=self.config, no_broker=self.no_broker
        ).get_consumer()
        apply_callable = get_callable(self.apply_function)
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

            map(apply_callable, msgs)

            if self.commit_cadence == "end_of_batch":
                consumer.commit()

        if self.commit_cadence:
            consumer.commit()

        consumer.close()

        return messages_processed
