# listener_dag.py 

import os
import json
from pendulum import datetime

from airflow import DAG, Dataset
from include.event_triggers_dag import EventTriggersDagOperator
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
    dag_id="listener_dag",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
):

    def await_function(message):
        if message is not None:
            print(json.loads(message.value()))
            if int(json.loads(message.value())["rating_id"]) % 2 == 0:
                return json.loads(message.value())

    def pick_downstream_dag(message):
        print(message)
        if message["channel"] == "web":
            return "worker_dag_web"
        if message["channel"] == "android":
            return "worker_dag_android"
        else:
            return "worker_dag_general"

    listen_for_message = EventTriggersDagOperator(
        task_id=f"listen_for_message_in_{my_topic}",
        topics=[my_topic],
        apply_function="listener_dag.await_function", #this needs to be passed in as a module, function direct does not work!!!!
        kafka_config={
            **connection_config,
            "group.id": "listen_for_message",
            "enable.auto.commit": True,
            "auto.offset.reset": "beginning",
        },
        xcom_push_key="retrieved_message",
        trigger_dag_run_instance=TriggerDagRunOperator(
            task_id="tdro",
            trigger_dag_id="listener_dag.pick_downstream_dag"
        )
    )