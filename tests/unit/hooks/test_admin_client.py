"""
Unittest module to test Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_sample_hook.TestSampleHook

"""

import logging
import os
from airflow import AirflowException
import pytest
import unittest
from unittest import mock


#
from confluent_kafka.admin import AdminClient

# Import Hook
from kafka_provider.hooks.admin_client import AdminClientHook


log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_KAFKA_SAMPLE='localhost:9092')
class TestSampleHook(unittest.TestCase):
    """
    Test Admin Client Hook.
    """

    def test_init(self):
        ''' test initialization of AdminClientHook'''

        #Standard Init
        extra_configs = {'socket.timeout.ms': 10}
        h = AdminClientHook(kafka_conn_id='kafka_sample', config=extra_configs)
        assert not h.admin_client
        

        #Too Many Args
        with pytest.raises(AirflowException):
            extra_configs = {'bootstrap.servers': 'localhost:9092'}
            h = AdminClientHook(kafka_conn_id='kafka_sample', config=extra_configs)
            assert not h.admin_client
        
        #Not Enough Args
        with pytest.raises(AirflowException):
            extra_configs = {}
            h = AdminClientHook(config=extra_configs)
            assert not h.admin_client


    def test_get_admin_client(self):
        """ test getting our client """
        extra_configs = {'socket.timeout.ms': 10}
        h = AdminClientHook(kafka_conn_id='kafka_sample', config=extra_configs)
        
        h.kafka_conn_id = None # This is a hack to make sure we don't actually try to connect to the client

        h.get_admin_client()
        assert isinstance(h.admin_client,AdminClient)

        
    
    def test_create_topic(self):
        """ test topic creation """
        extra_configs = {'socket.timeout.ms': 10}
        h = AdminClientHook(kafka_conn_id='kafka_sample', config=extra_configs)
        h.kafka_conn_id = None # This is a hack to make sure we don't actually try to connect to the client
        h.create_topic(topics=[('test_1',3,3),('test_2',1,1)])





if __name__ == '__main__':
    unittest.main()
