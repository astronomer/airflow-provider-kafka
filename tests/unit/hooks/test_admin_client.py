
import logging
import unittest
from unittest import mock

import pytest
from airflow import AirflowException

# Import Hook
from airflow_provider_kafka.hooks.admin_client import KafkaAdminClientHook

log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_KAFKA_SAMPLE="localhost:9092")
class TestSampleHook(unittest.TestCase):
    """
    Test Admin Client Hook.
    """

    def test_init(self):
        """test initialization of AdminClientHook"""

        # Standard Init
        extra_configs = {"socket.timeout.ms": 10}
        KafkaAdminClientHook(kafka_conn_id="kafka_sample", config=extra_configs)

        # Too Many Args
        with pytest.raises(AirflowException):
            extra_configs = {"bootstrap.servers": "localhost:9092"}
            KafkaAdminClientHook(kafka_conn_id="kafka_sample", config=extra_configs)

        # Not Enough Args
        with pytest.raises(AirflowException):
            extra_configs = {}
            KafkaAdminClientHook(config=extra_configs)

    def test_get_admin_client(self):
        """test getting our client"""
        extra_configs = {"socket.timeout.ms": 10}
        h = KafkaAdminClientHook(kafka_conn_id="kafka_sample", config=extra_configs)
        h.extra_configs[
            "bootstrap.servers"
        ] = None  # This is a hack to make sure we don't actually try to connect to the client
        h.get_admin_client()

    def test_create_topic(self):
        """test topic creation"""
        extra_configs = {"socket.timeout.ms": 10}
        h = KafkaAdminClientHook(kafka_conn_id="kafka_sample", config=extra_configs)
        h.extra_configs[
            "bootstrap.servers"
        ] = None  # This is a hack to make sure we don't actually try to connect to the client
        h.create_topic(topics=[("test_1", 3, 3), ("test_2", 1, 1)])


if __name__ == "__main__":
    unittest.main()
