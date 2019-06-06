"""
Tests for kf_lib_data_ingest/common/pandas_utils.py
"""
import logging

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


def test_merge_wo_duplicates(info_caplog, dfs):
    df1 = dfs[0]
    df2 = dfs[1]

    # Test when all cols match
    expected_df = df1.copy()
    merged = pandas_utils.merge_wo_duplicates(df1, df2, left_name='df1',
                                              right_name='df2',
                                              on='A', how='outer')
    assert merged.equals(expected_df)
    assert 'Outer merge df1 with df2' in info_caplog.text

    # Same as above, but with custom suffixes
    merged = pandas_utils.merge_wo_duplicates(
        df1, df2, on='A', how='outer', suffixes=('__left__', '__right__'))
    assert merged.equals(expected_df)
    assert 'Outer merge Left with Right' in info_caplog.text

    # Test when NaNs result from outer merge
    df2.at[2, 'A'] = 'bloo'
    expected_df = df1.copy().append(df2.tail(1), ignore_index=True)
    merged = pandas_utils.merge_wo_duplicates(df1, df2, on='A', how='outer')
    assert merged.equals(expected_df)

    # Test default behavior
    pandas_utils.merge_wo_duplicates(df1, df2, on='A')
    assert 'Inner merge Left with Right' in info_caplog.text

    # Test missing merge col(s)
    with pytest.raises(Exception) as e:
        pandas_utils.merge_wo_duplicates(df1, df2, on='__lkasjdf__')
    assert 'not found in Left' in str(e.value)
    assert 'not found in Right' in str(e.value)

    with pytest.raises(Exception) as e:
        pandas_utils.merge_wo_duplicates(df1, df2, left_on='__lkasjdf__',
                                         right_on='A')
    assert 'not found in Left' in str(e.value)
    assert 'not found in Right' not in str(e.value)

    with pytest.raises(Exception) as e:
        pandas_utils.merge_wo_duplicates(df1, df2, left_on='__lkasjdf__')
    assert 'Missing merge column keyword argument(s)' in str(e.value)

    merged = pandas_utils.merge_wo_duplicates(dfs[0], dfs[1],
                                              left_on='A', right_on='A',
                                              how='outer')
    assert merged.equals(expected_df)


def test_outer_merge(info_caplog, dfs):
    df1 = dfs[0]
    df2 = dfs[1]
    df2.at[2, 'A'] = 'bloo'

    ret_val = pandas_utils.outer_merge(df1, df2,
                                       left_name='df1',
                                       right_name='df2', on='A')
    merged, in_both, left_only, right_only = ret_val
    expected = df1.copy().append(df2.tail(1), ignore_index=True)

    assert 'Outer merge df1 with df2' in info_caplog.text
    assert len(ret_val) == 4
    assert expected.equals(merged)
    assert in_both.equals(df1.head(2))
    assert left_only.equals(df1.tail(1))
    assert numpy.array_equal(right_only.values, df2.tail(1).values)

    df = pandas_utils.outer_merge(df1, df2, on='A',
                                  with_merge_detail_dfs=False)
    assert 'Outer merge Left with Right' in info_caplog.text
    assert isinstance(df, pandas.DataFrame)
    assert expected.equals(df)


def test_split_df_rows():
    df_splits = pandas.DataFrame(
        {
            'a': [pandas_utils.Split(['1', '2'], group=1)],
            'b': [pandas_utils.Split(['3', '4', '5'], group=1)],
            'c': [pandas_utils.Split(['6', '7'])]
        }
    )
    df_delims = pandas.DataFrame(
        {
            'a': ['1,2'],
            'b': ['3,4,5'],
            'c': ['6,7']
        }
    )
    expected = pandas.DataFrame(
        {
            'a': ['1', '1', '2', '2', None, None],
            'b': ['3', '3', '4', '4', '5', '5'],
            'c': ['6', '7', '6', '7', '6', '7']
        }, index=[0, 0, 0, 0, 0, 0]
    )

    out_on_splits = pandas_utils.split_df_rows_on_splits(df_splits)
    out_on_delims = pandas_utils.split_df_rows_on_delims(
        pandas_utils.split_df_rows_on_delims(
            df_delims, ['a', 'b'], [','], False
        ),
        ['c'], [',']
    )

    for out in [out_on_splits, out_on_delims]:
        # DF comparisons are super unfriendly to rows being out of order,
        # and pandas.testing.assert_frame_equal with check_like=True raises an
        # exception if any indices repeat in the latest pandas on 05/24/2019
        # (0.24.2)

        # row-wise comparison via merge, ignoring indices
        assert pandas.merge(
            out_on_splits, expected, on=out_on_splits.columns.tolist()
        ).reset_index(
            drop=True
        ).equals(
            out.reset_index(drop=True)
        )
        # now compare indices
        assert out.index.equals(expected.index)
