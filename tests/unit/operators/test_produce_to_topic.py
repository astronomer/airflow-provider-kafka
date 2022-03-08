"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_sample_operator.TestSampleOperator

"""


import logging
import unittest
from unittest import mock

from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_KAFKA_SAMPLE="localhost:9092")
class TestProduceToTopic(unittest.TestCase):
    """
    Test ConsumeFromTopic
    """

    def test_operator(self):
        operator = ProduceToTopicOperator(
            topic="test_1",
            producer_function="airflow_provider_kafka.shared_utils.simple_producer",
            producer_function_args=(b"test", b"test"),
            task_id="test",
            kafka_config={"socket.timeout.ms": 10, "message.timeout.ms": 10, "bootstrap.servers": "test"},
            synchronous=False,
        )

        operator.execute(context={})


if __name__ == "__main__":
    unittest.main()
