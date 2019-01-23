"""
Tests for kf_lib_data_ingest/common/pandas_utils.py
"""

import pandas
import numpy
import pytest

from kf_lib_data_ingest.common import pandas_utils


@pytest.fixture(scope='function')
def dfs():
    nrows = 3
    df1 = pandas.DataFrame({'A': [f'A{i}' for i in range(nrows)],
                            'B': [f'B{i}' for i in range(nrows)],
                            'C': [f'C{i}' for i in range(nrows)]})

    df2 = df1.copy()
    df2['B'] = 'foo'
    df2['C'] = 'bar'

    return [df1, df2]


def test_try_pop():
    df = pandas.DataFrame({'COL_A': ['1', '2', '3']})
    assert pandas_utils.try_pop(df, 'COL_A', 'foo').equals(
        pandas.Series(['1', '2', '3'])
    )
    assert pandas_utils.try_pop(df, 'COL_A', 'foo') == 'foo'
    assert pandas_utils.try_pop(df, 'COL_A') is None


def test_get_col():
    df = pandas.DataFrame({'COL_A': ['1', '2', '3'], 1: ['a', 'b', 'c']})
    assert pandas_utils.get_col(df, 'COL_A') is df['COL_A']
    assert pandas_utils.get_col(df, 1) is df[1]
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


def test_merge_wo_duplicates(dfs):
    df1 = dfs[0]
    df2 = dfs[1]

    # Test when all cols match
    expected_df = df1.copy()
    merged = pandas_utils.merge_wo_duplicates(df1, df2, on='A', how='outer')
    assert merged.equals(expected_df)

    # Test when NaNs result from outer merge
    df2.at[2, 'A'] = 'bloo'
    expected_df = df1.copy().append(df2.tail(1), ignore_index=True)
    merged = pandas_utils.merge_wo_duplicates(df1, df2, on='A', how='outer')
    assert merged.equals(expected_df)


def test_outer_merge(dfs):
    df1 = dfs[0]
    df2 = dfs[1]
    df2.at[2, 'A'] = 'bloo'

    ret_val = pandas_utils.outer_merge(df1, df2, on='A')
    merged, in_both, left_only, right_only = ret_val
    expected = df1.copy().append(df2.tail(1), ignore_index=True)

    assert len(ret_val) == 4
    assert expected.equals(merged)
    assert in_both.equals(df1.head(2))
    assert left_only.equals(df1.tail(1))
    assert numpy.array_equal(right_only.values, df2.tail(1).values)

    df = pandas_utils.outer_merge(df1, df2, on='A',
                                  with_merge_detail_dfs=False)
    assert isinstance(df, pandas.DataFrame)
    assert expected.equals(df)

