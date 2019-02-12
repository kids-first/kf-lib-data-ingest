"""
Contains file readers (file -> dataframe) for file types that benefit from
extra help.
"""
import os

import pandas
import xlrd
from pandas.api.types import is_file_like


def read_excel_file(filepath_or_buffer, **kwargs):
    """Return contents of an excel spreadsheet as a pandas DataFrame.

    :param filepath_or_buffer: an xls or xlsx spreadsheet file
    :type filepath_or_buffer: string (path) or file-like object
    :param **kwargs: See docs for pandas.read_excel
    :return: The structured contents of the file
    :rtype: pandas.Dataframe
    """
    # Pre-opening the workbook with xlrd lets us suppress noisy warnings
    # like "WARNING *** OLE2 inconsistency: SSCS size is 0 but SSAT size is
    # non-zero" by pushing them to /dev/null
    with open(os.devnull, 'w') as devnull:
        if is_file_like(filepath_or_buffer):
            wb = xlrd.open_workbook(
                file_contents=filepath_or_buffer.read(), logfile=devnull
            )
        else:
            wb = xlrd.open_workbook(
                filename=filepath_or_buffer, logfile=devnull
            )
    kwargs['engine'] = 'xlrd'
    kwargs['dtype'] = object
    return pandas.read_excel(wb, **kwargs)
