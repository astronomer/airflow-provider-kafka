import importlib
from typing import Any, Callable, List, Tuple


def get_callable(function_string: str) -> Callable:
    """get_callable import a function based on its dot notation format as as string

    :param function_string: the dot notation location of the function. (i.e. foo_module.bar_function)
    :type function_string: str
    :return: the function request
    :rtype: Callable[...]
    """
    module_name, function_name = function_string.rsplit(".", 1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    return function


def no_op(*args, **kwargs) -> Any:
    """no_op A function that returns its arguments

    :return: whatever was passed in
    :rtype: Any
    """
    return args, kwargs


def simple_producer(key, value) -> List[Tuple[Any, Any]]:
    """simple_producer A function that returns the key,value passed in for production via "KafkaProducerOperator"

    :param key: the key for the message
    :type key: Any
    :param value: the value for the message
    :type value: Any
    :return: The Key / Value pair for production via the operator
    :rtype: List[Tuple[Any, Any]]
    """
    return [(key, value)]
