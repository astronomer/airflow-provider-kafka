"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_sample_operator.TestSampleOperator

"""

import logging
import unittest
from unittest import mock

# Import Operator
from kafka_provider.operators.consume_from_topic import ConsumeFromTopic

log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_KAFKA_SAMPLE="localhost:9092")
class TestConsumeFromTopic(unittest.TestCase):
    """
    Test ConsumeFromTopic
    """

    def test_operator(self):

        operator = ConsumeFromTopic(
            topics=["test"],
            apply_function="kafka_provider.shared_utils.no_op",
            consumer_config={"socket.timeout.ms": 10, "group.id": "test"},
            task_id="test",
            no_broker=True,
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})


if __name__ == "__main__":
    unittest.main()
