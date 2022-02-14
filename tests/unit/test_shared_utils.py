import logging
import os
from airflow import AirflowException
import pytest
import unittest
from unittest import mock


from kafka_provider.shared_utils import get_callable



def test_get_callable():
    func_as_callable = get_callable('kafka_provider.shared_utils.no_op')
    rv = func_as_callable(42,test=1)
    assert rv == ((42,),{'test':1})