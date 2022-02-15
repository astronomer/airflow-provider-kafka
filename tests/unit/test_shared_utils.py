from kafka_provider.shared_utils import get_callable


def test_get_callable():
    func_as_callable = get_callable("kafka_provider.shared_utils.no_op")
    rv = func_as_callable(42, test=1)
    assert rv == ((42,), {"test": 1})


def test_simple_producer():
    func_as_callable = get_callable("kafka_provider.shared_utils.simple_producer")
    rv = func_as_callable(42, 42)
    assert rv == [(42, 42)]
