"""
Tests for src/common/pandas_utils.py
"""

import pandas
import pytest

from common import pandas_utils


def test_try_pop():
    df = pandas.DataFrame({'COL_A': ['1', '2', '3']})
    assert pandas_utils.try_pop(df, 'COL_A', 'foo').equals(
        pandas.Series(['1', '2', '3'])
    )
    assert pandas_utils.try_pop(df, 'COL_A', 'foo') == 'foo'
    assert pandas_utils.try_pop(df, 'COL_A') is None


def test_get_col():
    df = pandas.DataFrame({1: ['a', 'b', 'c'], 'COL_A': ['1', '2', '3']})
    assert pandas_utils.get_col(df, 'COL_A') is df['COL_A']
    assert pandas_utils.get_col(df, 0) is df[1]
    assert pandas_utils.get_col(df, 1) is df[1]


def test_safe_pandas_replace_no_cascade():
    cascading_replace = {
        '1': '2',  # normal pandas replace would cascade these changes
        '2': '3',
        '3': '4'
    }
    # on a dataframe
    assert pandas_utils.safe_pandas_replace(
        pandas.DataFrame({'COL_A': ['1', '2', '3']}), cascading_replace
    ).equals(pandas.DataFrame({'COL_A': ['2', '3', '4']}))

    # on a series
    assert pandas_utils.safe_pandas_replace(
        pandas.Series(['1', '2', '3']), cascading_replace
    ).equals(pandas.Series(['2', '3', '4']))
