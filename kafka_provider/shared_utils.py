from ast import Call
from typing import Callable,Any
import importlib



def get_callable(function_string: str) -> Callable:
    """get_callable import a function based on its dot notation format as as tring

    :param function_string: the dot notation location of the function. (i.e. foo_module.bar_function)
    :type function_string: str
    :return: the function request
    :rtype: Callable[...]
    """
    module_name, function_name = function_string.rsplit('.',1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    return function


def no_op(*args,**kwargs):
    return args,kwargs