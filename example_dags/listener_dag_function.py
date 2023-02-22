# listener_dag_function.py

import json
import random
import string

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

from airflow_provider_kafka.operators.event_triggers_function import (
    EventTriggersFunctionOperator,
)


def generate_uuid():
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(6))


with DAG(
    dag_id="listener_dag_function",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
):

    def await_function(message):
        val = json.loads(message.value())
        print(f"Value in message is {val}")
        if val % 3 == 0:
            return val
        if val % 5 == 0:
            return val

    def pick_downstream_dag(message, **context):
        if message % 15 == 0:
            print(f"encountered {message} - executing external dag!")
            TriggerDagRunOperator(
                trigger_dag_id="fizz_buzz", task_id=f"{message}{generate_uuid()}"
            ).execute(context)
        else:
            if message % 3 == 0:
                print(f"encountered {message} FIZZ !")
            if message & 5 == 0:
                print(f"encountered {message} BUZZ !")

    listen_for_message = EventTriggersFunctionOperator(
        task_id="listen_for_message_fizz_buzz",
        topics=["test_1"],
        apply_function="listener_dag_function.await_function",  # this needs to be passed in as a module, function direct does not work!!!!
        kafka_config={
            "bootstrap.servers": "broker:29092",
            "group.id": "fizz_buzz",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        event_triggered_function=pick_downstream_dag,
    )

with DAG(
    dag_id="fizz_buzz",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
):

    def hello_4():
        print("FIZZ BUZZ")

    t1 = PythonOperator(task_id="hello_kafka", python_callable=hello_4)
