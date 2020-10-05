"""
Contains file readers for file types that benefit from extra help.
"""
import io
import json
import os

import jsonpickle
import pandas
import xlrd
import yaml
from pandas.api.types import is_file_like
from cchardet import UniversalDetector
from functools import partial


def __detect_encoding(potentially_binary_buffer):
    def detect(buffer):
        detector = UniversalDetector()
        for chunk in iter(partial(buffer.read, 1024), b""):
            detector.feed(chunk)
            if detector.done:
                break
        detector.close()
        buffer.seek(0)
        return detector.result["encoding"]

    try:
        res = detect(potentially_binary_buffer)
    except AttributeError:
        with open(potentially_binary_buffer, "rb") as f:
            res = detect(f)

    return res


def __text_wrap(potentially_binary_buffer, encoding):
    try:
        return io.TextIOWrapper(potentially_binary_buffer, encoding=encoding)
    except Exception:
        return potentially_binary_buffer


def read_excel_df(filepath_or_buffer, **kwargs):
    """
    Return contents of an excel spreadsheet as a pandas DataFrame.

    :param filepath_or_buffer: an xls or xlsx spreadsheet file
    :type filepath_or_buffer: string (path) or file-like object
    :param **kwargs: See docs for pandas.read_excel
    :return: The structured contents of the file
    :rtype: pandas.Dataframe
    """
    # Pre-opening the workbook with xlrd lets us suppress noisy warnings
    # like "WARNING *** OLE2 inconsistency: SSCS size is 0 but SSAT size is
    # non-zero" by pushing them to /dev/null
    with open(os.devnull, "w") as devnull:
        if is_file_like(filepath_or_buffer):
            wb = xlrd.open_workbook(
                file_contents=filepath_or_buffer.read(), logfile=devnull
            )
        else:
            wb = xlrd.open_workbook(
                filename=filepath_or_buffer, logfile=devnull
            )

    kwargs["engine"] = "xlrd"
    kwargs["dtype"] = str
    kwargs["na_filter"] = False
    return pandas.read_excel(wb, **kwargs)


def read_delimited_text_df(filepath_or_buffer, **kwargs):
    """
    Return contents of a delimited table file (like csv) as a pandas DataFrame.

    :param filepath_or_buffer: a delimited file
    :type filepath_or_buffer: string (path) or file-like object
    :param **kwargs: See docs for pandas.read_csv
    :return: The structured contents of the file
    :rtype: pandas.Dataframe
    """
    kwargs["sep"] = kwargs.pop("delimiter", None) or kwargs.get("sep")
    kwargs["engine"] = "python"
    kwargs["dtype"] = str
    kwargs["na_filter"] = False
    kwargs["encoding"] = kwargs.get("encoding") or __detect_encoding(
        filepath_or_buffer
    )
    f = __text_wrap(filepath_or_buffer, kwargs["encoding"])
    return pandas.read_csv(f, **kwargs)


def read_json_df(filepath_or_buffer, **kwargs):
    """
    Return contents of a json file (dict of lists or list of dicts) as a pandas
    DataFrame.

    :param filepath_or_buffer: a json file
    :type filepath_or_buffer: string (path) or file-like object
    :param **kwargs: See docs for pandas.read_json
    :return: The structured contents of the file
    :rtype: pandas.Dataframe
    """
    kwargs["convert_dates"] = False
    kwargs["encoding"] = kwargs.get("encoding") or __detect_encoding(
        filepath_or_buffer
    )
    return pandas.read_json(
        __text_wrap(filepath_or_buffer, kwargs["encoding"]), **kwargs
    )


def read_df(filepath_or_buffer, original_name=None, **kwargs):
    """
    Return contents of a data file as a pandas DataFrame. Wraps more specific
    readers.

    :param filepath_or_buffer: a data file
    :type filepath_or_buffer: string (path) or file-like object
    :param original_name: filename from which we can infer file type if the
        primary argument doesn't provide it
    :type original_name: string
    :param **kwargs: See docs for `pandas IO tools`
    :return: The structured contents of the file
    :rtype: pandas.Dataframe
    """
    if original_name is None:
        if isinstance(filepath_or_buffer, str):
            original_name = filepath_or_buffer
        elif hasattr(filepath_or_buffer, "original_name"):
            original_name = filepath_or_buffer.original_name
        else:
            original_name = filepath_or_buffer.name

    if original_name.endswith((".xlsx", ".xls")):
        read_func = read_excel_df
    elif original_name.endswith((".tsv", ".csv", ".txt")):
        read_func = read_delimited_text_df
    elif original_name.endswith(".json"):
        read_func = read_json_df
    else:
        raise Exception(
            f"Could not determine appropriate reader for '{original_name}'."
            " You may need to define a custom read function."
        )

    try:
        return read_func(filepath_or_buffer, **kwargs)
    except Exception as e:
        raise Exception(f"In {read_func.__name__} : {str(e)}") from e


def read_yaml(filepath):
    with open(filepath, "r") as yaml_file:
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
    """
    if (default is not None) and (not os.path.isfile(filepath)):
        return default

    with open(filepath, "r") as json_file:
        if use_jsonpickle:
            json_str = json_file.read()
            return jsonpickle.decode(json_str, keys=True)
        else:
            return json.load(json_file)


def write_json(data, filepath, use_jsonpickle=True, **kwargs):
    r"""
    Write Python data to JSON file.

    :param data: your data
    :param filepath: where to write your JSON file
    :type filepath: str
    :param use_jsonpickle: pickle JSON-incompatible types, defaults to True
    :type use_jsonpickle: bool, optional
    :param \**kwargs: keyword arguments to pass to json.dump
    """
    if "indent" not in kwargs:
        kwargs["indent"] = 4
    if "sort_keys" not in kwargs:
        kwargs["sort_keys"] = True
    with open(filepath, "w") as json_file:
        if use_jsonpickle:
            data = json.loads(jsonpickle.encode(data, keys=True))
        json.dump(data, json_file, **kwargs)
