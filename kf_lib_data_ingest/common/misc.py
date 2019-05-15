import datetime
import importlib
import inspect
import json
import os
import re
import time
from itertools import tee

import jsonpickle
import yaml
from pandas import isnull

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
        return yaml.load(yaml_file, Loader=yaml.FullLoader)


def read_json(filepath, default=None, use_jsonpickle=True):
    """
    Read JSON file into Python dict. If default is not None and the file
    does not exist, then return default.

    :param filepath: path to JSON file
    :type filepath: str
    :param default: default return value if file not found, defaults to None
    :type default: any, optional
    :param use_jsonpickle: pickle JSON-incompatible types, defaults to True
    :type use_jsonpickle: bool, optional
    :return: your data
    :rtype: dict
    """
    if (default is not None) and (not os.path.isfile(filepath)):
        return default

    with open(filepath, 'r') as json_file:
        if use_jsonpickle:
            json_str = json_file.read()
            return jsonpickle.decode(json_str)
        else:
            return json.load(json_file)


def write_json(data, filepath, use_jsonpickle=True, **kwargs):
    r"""
    Write Python dict to JSON file.

    :param data: your data
    :type data: dict
    :param filepath: where to write your JSON file
    :type filepath: str
    :param use_jsonpickle: pickle JSON-incompatible types, defaults to True
    :type use_jsonpickle: bool, optional
    :param \**kwargs: keyword arguments to pass to json.dump
    """
    if 'indent' not in kwargs:
        kwargs['indent'] = 4
    if 'sort_keys' not in kwargs:
        kwargs['sort_keys'] = True
    with open(filepath, 'w') as json_file:
        if use_jsonpickle:
            data = json.loads(jsonpickle.encode(data))
        json.dump(data, json_file, **kwargs)


def iterate_pairwise(iterable):
    """
    Iterate over an iterable in consecutive pairs
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def numeric_to_str(val, replace_na=False, na=None):
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
    :param replace_na: should None/NaN/blank values be replaced with something
    :type replace_na: boolean
    :param na: if replace_na is True, what should None/NaN/blank values be
        replaced with


    :return: new representation of `val`
    """
    if isnull(val):
        if replace_na:
            return na
        else:
            return val

    val = str(val).strip()
    if val != '':
        # Try downcasting val
        i_val = None
        f_val = None
        try:
            f_val = float(val)
            i_val = int(f_val)
        except ValueError:
            pass

        # Don't automatically change anything with leading zeros
        # (except something that equates to int 0), scientific
        # notation, or underscores (I don't care what PEP 515 says).
        if (i_val != 0) and ((val[0] == '0') or
                             (not re.fullmatch(r'[\d.]+', val))):
            return val

        # Return str version of downcasted val
        if i_val == f_val:
            return str(i_val)

    elif replace_na:
        return na

    return val


def clean_up_df(df):
    """
    We can't universally control which null type will get used by a data
    file loader, and it might also change, so let's always push them all
    to None because other nulls are not our friends. It's easier for a
    configurator to equate empty spreadsheet cells with None than e.g.
    numpy.nan.

    Typed loaders like pandas.read_json force us into storing numerically
    typed values. And then nulls, which read_json does not let you handle
    inline, cause pandas to convert perfectly good ints into ugly floats.
    So here we get any untidy values back to nice and tidy strings.

    :param df: a pandas DataFrame
    :return: Dataframe with numbers converted to strings and NaNs/blanks
        converted to None
    """

    return df.applymap(
        lambda x: numeric_to_str(x, replace_na=True, na=None)
    )


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


def upper_camel_case(snake_str):
    """
    Convert a snake case str to upper camel case
    """
    words = snake_str.split('_')
    return ''.join([w.title() for w in words])


def timestamp():
    """
    Helper to create an ISO 8601 formatted string that represents local time
    and includes the timezone info.
    """
    # Calculate the offset taking into account daylight saving time
    # https://stackoverflow.com/questions/2150739/iso-time-iso-8601-in-python
    if time.localtime().tm_isdst:
        utc_offset_sec = time.altzone
    else:
        utc_offset_sec = time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    t = datetime.datetime.now().replace(
        tzinfo=datetime.timezone(offset=utc_offset)).isoformat()

    return str(t)
