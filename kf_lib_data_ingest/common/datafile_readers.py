"""
Contains file readers (file -> dataframe) for file types that benefit from
extra help.
"""
import io
import os

import pandas
import xlrd
from pandas.api.types import is_file_like


def __ascii_wrap(potentially_binary_buffer):
    try:
        return io.TextIOWrapper(potentially_binary_buffer)
    except Exception:
        return potentially_binary_buffer


def read_excel_file(filepath_or_buffer, **kwargs):
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


def read_table_file(filepath_or_buffer, **kwargs):
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
    return pandas.read_csv(__ascii_wrap(filepath_or_buffer), **kwargs)


def read_json_file(filepath_or_buffer, **kwargs):
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
    return pandas.read_json(__ascii_wrap(filepath_or_buffer), **kwargs)
