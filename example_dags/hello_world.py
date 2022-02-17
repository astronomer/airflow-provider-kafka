import json
import logging
from datetime import datetime, timedelta

from airflow import DAG

from kafka_provider.operators.await_message import AwaitKafkaMessageOperator
from kafka_provider.operators.consume_from_topic import ConsumeFromTopicOperator
from kafka_provider.operators.produce_to_topic import ProduceToTopicOperator

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2021, 7, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def producer_function():
    for i in range(20):
        yield (json.dumps(i), json.dumps(i + 1))


consumer_logger = logging.getLogger("airflow")


def consumer_function(message, prefix=None):
    key = json.loads(message.key())
    value = json.loads(message.value())
    consumer_logger.info(f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}")
    return


def await_function(message):
    if json.loads(message.value()) % 5 == 0:
        return f" Got the following message: {json.loads(message.value())}"


with DAG(
    "kafka-example",
    default_args=default_args,
    description="Examples of Kafka Operators",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic="test_1",
        producer_function="hello_world.producer_function",
        kafka_config={"bootstrap.servers": "broker:29092"},
    )

    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test_1"],
        apply_function="hello_world.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "broker:29092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )

    t3 = ProduceToTopicOperator(
        task_id="produce_to_topic_2",
        topic="test_1",
        producer_function="hello_world.producer_function",
        kafka_config={"bootstrap.servers": "broker:29092"},
    )

    t4 = ConsumeFromTopicOperator(
        task_id="consume_from_topic_2",
        topics=["test_1"],
        apply_function="hello_world.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "broker:29092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=30,
        max_batch_size=10,
    )

    t5 = AwaitKafkaMessageOperator(
        task_id="awaiting_message",
        topics=["test_1"],
        apply_function="hello_world.await_function",
        kafka_config={
            "bootstrap.servers": "broker:29092",
            "group.id": "awaiting_message",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        xcom_push_key="retrieved_message",
    )

    t1 >> t2 >> t3 >> t4 >> t5
