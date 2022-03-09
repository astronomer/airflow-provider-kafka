import unittest
from unittest import mock

import pytest
from airflow import AirflowException

# Import Hook
from airflow_provider_kafka.hooks.consumer import KafkaConsumerHook


# Mock the `conn_sample` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_KAFKA_SAMPLE="localhost:9092")
class TestConsumerHook(unittest.TestCase):
    """
    Test consumer hook.
    """

    def test_init(self):
        """test initialization of AdminClientHook"""

        # Standard Init
        extra_configs = {"socket.timeout.ms": 10, "group.id": "test"}
        KafkaConsumerHook(["test_1"], kafka_conn_id="kafka_sample", config=extra_configs)

        # Too Many Args
        with pytest.raises(AirflowException):
            extra_configs = {"bootstrap.servers": "localhost:9092", "group.id": "test2"}
            KafkaConsumerHook(["test_1"], kafka_conn_id="kafka_sample", config=extra_configs)

        # Not Enough Args
        with pytest.raises(AirflowException):
            extra_configs = {"group.id": "test3"}
            KafkaConsumerHook(["test_1"], config=extra_configs)

        with pytest.raises(AirflowException):
            extra_configs = {}
            KafkaConsumerHook(["test_1"], config=extra_configs)


if __name__ == "__main__":
    unittest.main()
