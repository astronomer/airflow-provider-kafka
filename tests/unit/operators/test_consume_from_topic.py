import logging
import unittest
from unittest import mock

# Import Operator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.shared_utils import no_op

log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_KAFKA_SAMPLE="localhost:9092")
class TestConsumeFromTopic(unittest.TestCase):
    """
    Test ConsumeFromTopic
    """

    def test_operator(self):

        operator = ConsumeFromTopicOperator(
            topics=["test"],
            apply_function="airflow_provider_kafka.shared_utils.no_op",
            consumer_config={"socket.timeout.ms": 10, "group.id": "test", "bootstrap.servers": "test"},
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})

    def test_operator_callable(self):

        operator = ConsumeFromTopicOperator(
            topics=["test"],
            apply_function=no_op,
            consumer_config={"socket.timeout.ms": 10, "group.id": "test", "bootstrap.servers": "test"},
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})


if __name__ == "__main__":
    unittest.main()
