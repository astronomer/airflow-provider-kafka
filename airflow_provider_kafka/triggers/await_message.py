import asyncio
from functools import partial
from typing import Any, Dict, Optional, Sequence, Tuple

from airflow import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async

from airflow_provider_kafka.hooks.consumer import KafkaConsumerHook
from airflow_provider_kafka.shared_utils import get_callable


class AwaitMessageTrigger(BaseTrigger):
    """AwaitMessageTrigger A trigger that waits for a message matching specific criteria to arrive in Kafka

    The behavior of the consumer of this trigger is as follows:
    - poll the Kafka topics for a message
        - if no message returned, sleep
    - process the message with provided callable and commit the message offset
        - if callable returns any data, raise a TriggerEvent with the return data
        - else continue to next message

    :param topics: The topic (or topic regex) that should be searched for messages
    :type topics: Sequence[str]
    :param apply_function: the location of the function to apply to messages for determination of matching criteria.
        (In python dot notation as a string)
    :type apply_function: str
    :param apply_function_args: A set of arguments to apply to the callable, defaults to None
    :type apply_function_args: Optional[Sequence[Any]], optional
    :param apply_function_kwargs: A set of key word arguments to apply to the callable, defaults to None,
        defaults to None
    :type apply_function_kwargs: Optional[Dict[Any, Any]], optional
    :param kafka_conn_id: The airflow connection id to fetch kafka brokers from, defaults to None
    :type kafka_conn_id: Optional[str], optional
    :param kafka_config: the config dictionary for the kafka client (additional information available on the
        confluent-python-kafka documentation), defaults to None
    :type kafka_config: Optional[Dict[Any, Any]], optional
    :param poll_timeout: How long the Kafka client should wait before returning from a poll request to
        Kafka (seconds), defaults to 1
    :type poll_timeout: float, optional
    :param poll_interval: How long the the trigger should sleep after reaching the end of the Kafka log (seconds)
        , defaults to 5
    :type poll_interval: float, optional
    """

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
    ) -> None:

        self.topics = topics
        self.apply_function = apply_function
        self.apply_function_args = apply_function_args or ()
        self.apply_function_kwargs = apply_function_kwargs or {}
        self.kafka_conn_id = kafka_conn_id
        self.kafka_config = kafka_config
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "airflow_provider_kafka.triggers.await_message.AwaitMessageTrigger",
            {
                "topics": self.topics,
                "apply_function": self.apply_function,
                "apply_function_args": self.apply_function_args,
                "apply_function_kwargs": self.apply_function_kwargs,
                "kafka_conn_id": self.kafka_conn_id,
                "kafka_config": self.kafka_config,
                "poll_timeout": self.poll_timeout,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        consumer_hook = KafkaConsumerHook(
            topics=self.topics,
            kafka_conn_id=self.kafka_conn_id,
            config=self.kafka_config,
        )

        async_get_consumer = sync_to_async(consumer_hook.get_consumer)
        consumer = await async_get_consumer()

        async_poll = sync_to_async(consumer.poll)
        async_commit = sync_to_async(consumer.commit)

        processing_call = get_callable(self.apply_function)
        processing_call = partial(processing_call, *self.apply_function_args, **self.apply_function_kwargs)
        async_message_process = sync_to_async(processing_call)
        while True:

            message = await async_poll(self.poll_timeout)

            if message is None:
                continue
            elif message.error():
                raise AirflowException(f"Error: {message.error()}")
            else:

                rv = await async_message_process(message)
                if rv:
                    await async_commit(asynchronous=False)
                    yield TriggerEvent(rv)
                else:
                    await async_commit(asynchronous=False)
                    await asyncio.sleep(self.poll_interval)
