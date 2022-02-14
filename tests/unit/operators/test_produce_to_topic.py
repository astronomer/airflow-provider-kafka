"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_sample_operator.TestSampleOperator

"""

from ast import operator
import json
import logging
import os
import pytest
import unittest
from unittest import mock


from kafka_provider.operators.produce_to_topic import ProduceToTopic

log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_KAFKA_SAMPLE='localhost:9092')
class TestProduceToTopic(unittest.TestCase):
    """
    Test ConsumeFromTopic
    """

    def test_operator(self):
        operator = ProduceToTopic(
            topic = 'test_1',
            producer_function = 'kafka_provider.shared_utils.simple_producer',
            producer_function_args= (b'test',b'test'),
            task_id = 'test',
            kafka_config={'socket.timeout.ms': 10,
                          'message.timeout.ms': 10},
            no_broker = True,
            synchronous=False
        )

        response_payload = operator.execute(context={})

if __name__ == '__main__':
    unittest.main()