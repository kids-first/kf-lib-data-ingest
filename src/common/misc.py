import os
import json
import yaml
import importlib
import inspect
from itertools import tee


def import_module_from_file(filepath):
    """
    Import a Python module given a filepath
    """
    module_name = os.path.basename(filepath).split(".")[0]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    imported_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(imported_module)

    return imported_module


def read_yaml(filepath):
    with open(filepath, 'r') as yaml_file:
        return yaml.load(yaml_file)


def read_json(filepath):
    with open(filepath, 'r') as json_file:
        return json.load(json_file)


def write_json(data, filepath):
    with open(filepath, 'w') as json_file:
        json.dump(data, json_file, sort_keys=True, indent=4,
                  separators=(',', ':'))


def iterate_pairwise(iterable):
    """
    Iterate over an iterable in consecutive pairs
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def kwargs_from_frame(current_frame, start_arg_pos=1):
    """
    Create a dict with the keyword arguments from a method, represented by
    current_frame.

    :param current_frame: frame object for the caller’s stack frame.
    :param start_arg_pos: the index in the list of args after which all keyword
    args will be collected.
    """
    args, _, _, values = inspect.getargvalues(current_frame)
    kwargs = {arg: values[arg] for arg in args[start_arg_pos:]}

    return kwargs
