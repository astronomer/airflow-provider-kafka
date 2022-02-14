"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_sample_operator.TestSampleOperator

"""

import json
import logging
import os
import pytest
import unittest
from unittest import mock


# Import Operator
from kafka_provider.operators.consume_from_topic import ConsumeFromTopic


log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_KAFKA_SAMPLE='localhost:9092')
class TestConsumerFromTopic(unittest.TestCase):
    """
    Test Sample Operator.
    """

    def test_operator(self):

        operator = ConsumeFromTopic(
            topics=['test'],
            apply_function='kafka_provider.shared_utils.no_op',
            consumer_config = {'socket.timeout.ms': 10, 'group.id' : 'test'}, 
            task_id = 'test',
            no_broker = True,
            poll_timeout=.0001
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        
        with mock.patch
        
        response_payload = operator.execute(context={})

        print(response_payload)


if __name__ == '__main__':
    unittest.main()