# listener_dag.py 

import os
import json
from pendulum import datetime

from airflow import DAG, Dataset
from include.event_triggers_function import EventTriggersFunctionOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

confluent_dataset_1 = Dataset(f"confluent://{os.environ['BOOSTRAP_SERVER']}/{os.environ['KAFKA_TOPIC_NAME']}")

my_topic = os.environ["KAFKA_TOPIC_NAME"]

connection_config = {
    "bootstrap.servers": os.environ["BOOSTRAP_SERVER"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"]
}

with DAG(
    dag_id="listener_dag_function",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
):

    def await_function(message):
        if message is not None:
            print(json.loads(message.value()))
            if int(json.loads(message.value())["rating_id"]) % 2 == 0:
                return json.loads(message.value())

    def pick_downstream_dag(message, **context):
        print(message)
        if message["channel"] == "web":
            TriggerDagRunOperator(trigger_dag_id="worker_dag_web", task_id="tdr").execute(context)
        if message["channel"] == "android":
            TriggerDagRunOperator(trigger_dag_id="worker_dag_android", task_id="tdr").execute(context)
        else:
            TriggerDagRunOperator(trigger_dag_id="worker_dag_general", task_id="tdr").execute(context)

    listen_for_message = EventTriggersFunctionOperator(
        task_id=f"listen_for_message_in_{my_topic}",
        topics=[my_topic],
        apply_function="listener_dag_function.await_function", #this needs to be passed in as a module, function direct does not work!!!!
        kafka_config={
            **connection_config,
            "group.id": "airflow_consumer_2",
            "enable.auto.commit": True,
            "auto.offset.reset": "beginning",
        },
        xcom_push_key="retrieved_message",
        event_triggered_function=pick_downstream_dag
    )