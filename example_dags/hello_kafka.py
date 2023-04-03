import functools
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

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
    consumer_logger.info(
        f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}"
    )
    return


def consumer_function_batch(messages, prefix=None):
    for message in messages:
        key = json.loads(message.key())
        value = json.loads(message.value())
        consumer_logger.info(
            f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}"
        )
    return


def await_function(message):
    if json.loads(message.value()) % 5 == 0:
        return f" Got the following message: {json.loads(message.value())}"


def hello_kafka():
    print("Hello Kafka !")
    return


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
        producer_function="hello_kafka.producer_function",
        kafka_config={"bootstrap.servers": "kafka:29092"},
    )

    t1.doc_md = "Takes a series of messages from a generator function and publishes them to the `test_1` topic of our kafka cluster."

    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test_1"],
        apply_function="hello_kafka.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "kafka:29092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )

    t2.doc_md = "Reads a series of messages from the `test_1` topic, and processes them with a consumer function with a keyword argument."

    t3 = ProduceToTopicOperator(
        task_id="produce_to_topic_2",
        topic="test_1",
        producer_function=producer_function,
        kafka_config={"bootstrap.servers": "kafka:29092"},
    )

    t3.doc_md = "Does the same thing as the t1 task, but passes the callable directly instead of using the string notation."

    t4 = ConsumeFromTopicOperator(
        task_id="consume_from_topic_2",
        topics=["test_1"],
        apply_function=functools.partial(consumer_function, prefix="consumed:::"),
        consumer_config={
            "bootstrap.servers": "kafka:29092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=30,
        max_batch_size=10,
    )

    t4b = ConsumeFromTopicOperator(
        task_id="consume_from_topic_2b",
        topics=["test_1"],
        apply_function_batch=functools.partial(
            consumer_function_batch, prefix="consumed:::"
        ),
        consumer_config={
            "bootstrap.servers": "kafka:29092",
            "group.id": "foo2",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=30,
        max_batch_size=10,
    )

    t4.doc_md = "Does the same thing as the t2 task, but passes the callable directly instead of using the string notation."

    t5 = AwaitKafkaMessageOperator(
        task_id="awaiting_message",
        topics=["test_1"],
        apply_function="hello_kafka.await_function",
        kafka_config={
            "bootstrap.servers": "kafka:29092",
            "group.id": "awaiting_message",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        xcom_push_key="retrieved_message",
    )

    t5.doc_md = "A deferable task. Reads the topic `test_1` until a message with a value divisible by 5 is encountered."

    t6 = PythonOperator(task_id="hello_kafka", python_callable=hello_kafka)

    t6.doc_md = (
        "The task that is executed after the deferable task returns for execution."
    )

    t1 >> t2 >> t3 >> [t4, t4b] >> t5 >> t6
