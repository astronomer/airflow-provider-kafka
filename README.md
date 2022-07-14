# Kafka Airflow Provider


An airflow provider to: 
- interact with kafka clusters
- read from topics
- write to topics
- wait for specific messages to arrive to a topic

This package currently contains

3 hooks :
- `airflow_provider_kafka.hooks.admin_client.KafkaAdminClientHook` - a hook to work against the actual kafka admin client
- `airflow_provider_kafka.hooks.consumer.KafkaConsumerHook` - a hook that creates a consumer and provides it for interaction
- `airflow_provider_kafka.hooks.producer.KafkaProducerHook` - a hook that creates a producer and provides it for interaction

3 operators : 
- `airflow_provider_kafka.operators.await_message.AwaitKafkaMessageOperator` - a deferable operator (sensor) that awaits to encounter a message in the log before triggering down stream tasks.
- `airflow_provider_kafka.operators.consume_from_topic.ConsumeFromTopicOperator` - an operator that reads from a topic and applies a function to each message fetched. 
- `airflow_provider_kafka.operators.produce_to_topic.ProduceToTopicOperator` - an operator that uses a iterable to produce messages as key/value pairs to a kafka topic. 

1 trigger : 
- `airflow_provider_kafka.triggers.await_message.AwaitMessageTrigger`


## Quick start

` pip install airflow-provider-kafka`

```python 
    # hello_kafka.py 
    
    from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
    from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
    from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

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

    t1 = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic="test_1",
        producer_function="hello_kafka.producer_function",
        kafka_config={"bootstrap.servers": "broker:29092"},
    )

    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test_1"],
        apply_function="hello_kafka.consumer_function",
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

    AwaitKafkaMessageOperator(
        task_id="awaiting_message",
        topics=["test_1"],
        apply_function="hello_kafka.await_function",
        kafka_config={
            "bootstrap.servers": "broker:29092",
            "group.id": "awaiting_message",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        xcom_push_key="retrieved_message",
    )
```

## FAQs 

**Why confluent kafka and not (other library) ?** A few reasons: the [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) library is guaranteed to be 1:1 functional with librdkafka, is faster, and is maintained by a company with a commercial stake in ensuring the continued quality and upkeep of it as a product. 

**Why not release this into airflow directly ?** I could probably make the PR and get it through, but the airflow code base is getting huge and I don't want to burden the maintainers with code that they don't own for maintainence. Also there's been multiple attempts to get a Kafka provider in before and this is just faster. 

**Why is most of the configuration handled in a dict ?** Because that's how `confluen-kafka` does it. I'd rather maintain interfaces that people already using kafka are comfortable with as a starting point - I'm happy to add more options/ interfaces in later but would prefer to be thoughtful about it to ensure that there difference between these operators and the actual client interface are minimal. 

## Development

### Unit Tests

Unit tests are located at `tests/unit`, a kafka server isn't required to run these tests.
execute with `pytest`


### Setup on M1 Mac
Installing on M1 chip means a brew install of the librdkafka library before you can pip install confluent-kafka
```bash
brew install librdkafka
export C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/1.8.2/include
export LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/1.8.2/lib
pip install confluent-kafka
```
