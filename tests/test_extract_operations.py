"""
Tests for kf_lib_data_ingest/extract/operations.py
"""

import pandas
import pytest

from kf_lib_data_ingest.common.type_safety import function
from kf_lib_data_ingest.etl.extract import operations
from test_type_safety import type_exemplars

df = pandas.DataFrame({"COL_A": ["1", "2", "3"]})
other_df = pandas.DataFrame({"COL_B": ["4", "5", "6"]})
bigger_df = pandas.DataFrame(
    {
        "COL_A": ["1", "2", "3"],
        "COL_B": ["a", "b", "c"],
        "COL_C": ["z", "y", "x"],
        "COL_D": ["3", "2", "1"],
    }
)
longvalue_df = pandas.DataFrame({"COL_A": ["1a1", "2a2", "3a3"]})


def _in_out_variants(map_wrap, val, in_col=None, out_col=None):
    """
    Calls some function map_wrap with arguments according to what is optionally
    passed in here.
    """
    if in_col is not None and out_col is not None:
        map_wrap(val, in_col, out_col)
    elif in_col is not None:
        map_wrap(val, in_col)
    elif out_col is not None:
        map_wrap(val, out_col)
    else:
        map_wrap(val)


def _test_map_allowed_types(map_wrap, allowed_types, in_col=None, out_col=None):
    """
    For a given map_wrap function, tests that all unallowed argument types
    raise a TypeError and all allowed argument types do not.
    """
    for k, v in type_exemplars:
        if v not in allowed_types:
            with pytest.raises(TypeError):
                _in_out_variants(map_wrap, k, in_col, out_col)
        else:
            _in_out_variants(map_wrap, k, in_col, out_col)


def test_df_map():
    """
    Test that df_map returns a df and doesn't modify the original.
    """
    # tests passing allowed and disallowed types
    _test_map_allowed_types(operations.df_map, {function})

    # verify that df_map doesn't modify the original df
    func = operations.df_map(lambda df: df)
    assert df is not func(df)

    # and that the contents are equal
    assert func(df).equals(df)

    # and that the above test isn't a trick
    func = operations.df_map(lambda df: other_df)
    assert func(df).equals(other_df)

    # verify that df mapping func may only return a DataFrame
    func = operations.df_map(lambda df: None)
    with pytest.raises(TypeError) as e:
        func(df)
    assert "DataFrame" in str(e.value)


def test_keep_map():
    # verify that keep_map doesn't modify the original df
    func = operations.keep_map("COL_A", "COL_A")
    assert df is not func(df)

    func = operations.keep_map("COL_B", "OUT_COL")
    assert func(bigger_df).equals(
        pandas.DataFrame({"OUT_COL": bigger_df["COL_B"]})
    )


def test_value_map():
    # tests passing allowed and disallowed types
    _test_map_allowed_types(
        operations.value_map,
        {function, dict, str},
        in_col="COL_A",
        out_col="OUT_COL",
    )

    # verify that value_map doesn't modify the original df
    func = operations.value_map(lambda x: x, "COL_A", "COL_A")
    assert df is not func(df)

    # mapper function
    func = operations.value_map(lambda x: 5, "COL_A", "OUT_COL")
    assert func(df).equals(pandas.DataFrame({"OUT_COL": [5, 5, 5]}))

    # mapper dict
    func = operations.value_map(
        {"1": "a", "2": "b", "3": "c"}, "COL_A", "OUT_COL"
    )
    assert func(df).equals(pandas.DataFrame({"OUT_COL": ["a", "b", "c"]}))

    # mapper regex
    func = operations.value_map(r"^\d(.)\d$", "COL_A", "OUT_COL")
    assert func(longvalue_df).equals(
        pandas.DataFrame({"OUT_COL": ["a", "a", "a"]})
    )


def test_row_map():
    # tests passing allowed and disallowed types
    _test_map_allowed_types(operations.row_map, {function}, out_col="OUT_COL")

    func = operations.row_map(lambda row: 5, "OUT_COL")
    assert func(df).equals(pandas.DataFrame({"OUT_COL": [5, 5, 5]}))

    def row_transform(row):
        if row["COL_A"] == row["COL_D"]:
            return "=="
        elif int(row["COL_A"]) > int(row["COL_D"]):
            return row["COL_B"]
        else:
            return row["COL_C"]

    func = operations.row_map(row_transform, "OUT_COL")
    assert func(bigger_df).equals(
        pandas.DataFrame({"OUT_COL": ["z", "==", "c"]})
    )


def test_constant_map():
    func = operations.constant_map(5, "OUT_COL")
    assert func(df).equals(pandas.DataFrame({"OUT_COL": [5, 5, 5]}))


def test_column_map():
    # tests passing allowed and disallowed types
    _test_map_allowed_types(
        operations.column_map, {function}, in_col="COL_A", out_col="OUT_COL"
    )

    in_df = bigger_df.copy()
    func = operations.column_map(lambda col: col, "COL_A", "OUT_COL")
    out_df = func(bigger_df)
    assert out_df["OUT_COL"].equals(bigger_df["COL_A"])
    assert bigger_df.equals(in_df)

    func = operations.column_map(
        # reverse order
        lambda col: col[::-1].reset_index(drop=True),
        "COL_A",
        "OUT_COL",
    )
    assert func(df).equals(pandas.DataFrame({"OUT_COL": ["3", "2", "1"]}))


def test_melt_map():
    # tests passing allowed and disallowed types
    for k, v in type_exemplars:
        for l, w in type_exemplars:
            if (v not in {dict}) or (w not in {dict, function}):
                with pytest.raises(TypeError):
                    operations.melt_map("VAR_COL", k, "VAL_COL", l)
            else:
                operations.melt_map("VAR_COL", k, "VAL_COL", l)

    func = operations.melt_map(
        "VAR_COL",
        {"COL_A": "NEW_A", "COL_B": "NEW_B"},
        "VAL_COL",
        lambda x: x * 2,
    )

    # input:
    #   COL_A COL_B COL_C COL_D
    # 0     1     a     z     3
    # 1     2     b     y     2
    # 2     3     c     x     1
    out_df = func(bigger_df)

    # expected output:
    #   VAR_COL VAL_COL
    # 0   NEW_A      11
    # 1   NEW_A      22
    # 2   NEW_A      33
    # 0   NEW_B      aa
    # 1   NEW_B      bb
    # 2   NEW_B      cc
    expected_output = pandas.DataFrame(
        {
            "VAR_COL": ["NEW_A", "NEW_A", "NEW_A", "NEW_B", "NEW_B", "NEW_B"],
            "VAL_COL": ["11", "22", "33", "aa", "bb", "cc"],
        },
        index=[0, 1, 2, 0, 1, 2],
    )

    assert out_df.equals(expected_output)
