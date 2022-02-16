
from datetime import datetime, timedelta
from textwrap import dedent


from airflow import DAG
from kafka_provider.operators.produce_to_topic import ProduceToTopic


default_args = {
    'owner'                 : 'airflow',
    'depend_on_past'        : False,
    'start_date'            : datetime(2021, 7, 20),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=5)
}



def producer_function():
    for i in range(20):
        yield (bytes(i),bytes(i+1))


with DAG(
    'kafka-example',
    default_args=default_args,
    description='Examples of Kafka Operators',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = ProduceToTopic(
        task_id = 'produce_to_topic',
        topic='test_1',
        producer_function='hello_world.producer_function',
        kafka_config={"bootstrap.servers": "broker:29092"}

    )