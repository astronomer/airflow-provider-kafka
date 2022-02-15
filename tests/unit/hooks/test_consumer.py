"""
Unittest module to test Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_sample_hook.TestSampleHook

"""

import logging
import os
import unittest
from ast import excepthandler
from unittest import mock

import pytest
from airflow import AirflowException

#
from confluent_kafka import Consumer

# Import Hook
from kafka_provider.hooks.consumer import ConsumerHook

log = logging.getLogger(__name__)


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
        c = ConsumerHook(["test_1"], kafka_conn_id="kafka_sample", config=extra_configs)

        # Too Many Args
        with pytest.raises(AirflowException):
            extra_configs = {"bootstrap.servers": "localhost:9092", "group.id": "test2"}
            c = ConsumerHook(["test_1"], kafka_conn_id="kafka_sample", config=extra_configs)

        # Not Enough Args
        with pytest.raises(AirflowException):
            extra_configs = {"group.id": "test3"}
            c = ConsumerHook(["test_1"], config=extra_configs)

        with pytest.raises(AirflowException):
            extra_configs = {}
            c = ConsumerHook(["test_1"], config=extra_configs)


if __name__ == "__main__":
    unittest.main()
