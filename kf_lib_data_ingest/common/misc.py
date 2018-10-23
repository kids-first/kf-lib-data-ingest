import importlib
import json
import os
import re

import inspect
from itertools import tee

import numpy
import yaml


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


def intsafe_str(val):
    """Converts numbers to str while collapsing "1.0" to "1", etc.
    in case of wrong numeric encoding of integers in a spreadsheet.

    Args:
        val: Any basic type

    Returns:
        string: Representation of `val`
    """
    if val is None or (isinstance(val, float) and numpy.isnan(val)):
        return None
    val = str(val).strip()
    # I hate PEP 515.
    # Now we have to check that we only have digits and dots. Whyyyyyyyyyy.
    if not re.fullmatch(r'\d*(\.\d*)?', val):
        return val
    if (len(val) - len(val.lstrip('0'))) > 0:
        # leading zeroes indicate a real string
        return val
    try:
        f_val = float(val)
        i_val = int(f_val)
        if i_val == f_val:
            return str(i_val)
    except Exception:
        pass
    return val


def obj_attrs_to_dict(cls):
    """
    Create a dict of obj attributes and values, including inherited attrs
    """
    # Get non function attributes
    attributes = inspect.getmembers(cls, lambda x: not(inspect.isroutine(x)))

    # Get non-hidden attrs
    attributes = [a for a in attributes
                  if not(a[0].startswith('__') and
                         a[0].endswith('__'))]
    return dict(attributes)
