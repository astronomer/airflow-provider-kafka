from airflow_provider_kafka.triggers.await_message import AwaitMessageTrigger


def test_trigger_serialization():

    trigger = AwaitMessageTrigger(
        apply_function="test.noop",
        topics="noop",
        apply_function_args=[1, 2],
        apply_function_kwargs=dict(one=1, two=2),
        kafka_conn_id="sample_id",
        kafka_config=dict(this="wont_work"),
        poll_timeout=10,
        poll_interval=5,
    )

    assert isinstance(trigger, AwaitMessageTrigger)

    classpath, kwargs = trigger.serialize()

    assert (
        classpath == "airflow_provider_kafka.triggers.await_message.AwaitMessageTrigger"
    )
    assert kwargs == dict(
        apply_function="test.noop",
        topics="noop",
        apply_function_args=[1, 2],
        apply_function_kwargs=dict(one=1, two=2),
        kafka_conn_id="sample_id",
        kafka_config=dict(this="wont_work"),
        poll_timeout=10,
        poll_interval=5,
    )
