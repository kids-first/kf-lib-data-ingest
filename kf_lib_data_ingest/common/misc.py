import importlib
import inspect
import json
import os
import re
from itertools import tee

from pandas import isnull
import yaml

from kf_lib_data_ingest.common.type_safety import assert_safe_type


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


def to_str_with_floats_downcast_to_ints_first(val, replace_na=False, na=None):
    """
    Converts values to stripped strings while collapsing downcastable floats.

    Examples:
        to_str_with_floats_downcast_to_ints_first(1) -> "1"
        to_str_with_floats_downcast_to_ints_first(1.0) -> "1"
        to_str_with_floats_downcast_to_ints_first("1_1  ") -> "1_1"
        to_str_with_floats_downcast_to_ints_first(None) -> None
        to_str_with_floats_downcast_to_ints_first(None, True, "") -> ""

    If you're wondering what this is good for, try the following:
        import pandas
        df1 = pandas.DataFrame({"a":[1,2,3,None]}, dtype=object)
        df2 = pandas.read_json(df1.to_json(), dtype=object)
        str(df1['a'][0]) == str(df2['a'][0])  # this returns False. Yuck.
        df1 = df1.applymap(to_str_with_floats_downcast_to_ints_first)
        df2 = df2.applymap(to_str_with_floats_downcast_to_ints_first)
        str(df1['a'][0]) == str(df2['a'][0])  # this returns True. Good.

    :param val: any basic type
    :param replace_na: should None/NaN values be replaced with something
    :type replace_na: boolean
    :param na: if replace_na is True, what should None/NaN values be replaced
        with


    :return: new representation of `val`
    """
    if isnull(val):
        if replace_na:
            return na
        else:
            return val
    val = str(val).strip()
    if val != '':
        # Don't automatically change anything with leading zeros, scientific
        # notation, or underscores (I don't care what PEP 515 says).
        if (val[0] == '0') or (not re.fullmatch(r'[\d.]+', val)):
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


def multisplit(string: str, delimiters: list):
    """Split a string by multiple delimiters.

    :param string: the string to split
    :type string: str
    :param delimiters: the list of delimiters to split by
    :type delimiters: list
    :return: the split substrings
    :rtype: list
    """

    assert_safe_type(string, str)
    assert_safe_type(delimiters, list)
    regexPattern = '|'.join(map(re.escape, delimiters))
    return re.split(regexPattern, string)
