import os

import pandas
import xlrd


def read_excel_file(file_path, **kwargs):
    """Return contents of an excel spreadsheet as a pandas DataFrame.

    Args:
        file_path (string): Path to an xls or xlsx spreadsheet file.
        **kwargs: See docs for pandas.read_excel

    Returns:
        pandas.Dataframe: The structured contents of the file.
    """
    # Pre-opening the workbook with xlrd lets us suppress noisy warnings,
    # like: "WARNING *** OLE2 inconsistency: SSCS size is 0 but SSAT size is
    # non-zero"
    wb = xlrd.open_workbook(file_path, logfile=open(os.devnull, 'w'))
    return pandas.read_excel(wb, **kwargs)
