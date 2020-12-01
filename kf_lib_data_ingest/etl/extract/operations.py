"""
Extract-stage predefined operations.

Every extract config file declares a sequence of operations for pulling columns
of data out of a clinical file. Each operation is defined as a function that
takes the original data as input and returns a DataFrame with one or more
columns with row index values equal to the relevant original indices.

This module stores pre-made function wrappers that when invoked will return
functions for performing common actions such as fetching a column and replacing
its values with other values.

See: docs/design/extract_config_format.py for function details
"""
from pandas import DataFrame

from kf_lib_data_ingest.common.pandas_utils import (  # noqa: F401
    Split,
    get_col,
    safe_pandas_replace,
)
from kf_lib_data_ingest.common.type_safety import assert_safe_type


def df_map(m):
    """
    Wraps the DataFrame mapping operation in a function that takes only a
    DataFrame as input and returns a new DataFrame.

    For when you just can't figure out how to do what you want to do with the
    other predefined operations. Take a whole DataFrame, do something with it,
    and return a DataFrame with whichever standard concept columns
    you want (plus index).

    :param m: A function to apply to a DataFrame
    :return: A function that applies the specified m operation
    """
    assert_safe_type(m, callable)

    def df_map_func(df):
        new_df = m(df.copy())
        # Output of df mapping func must be a DataFrame
        assert_safe_type(new_df, DataFrame)
        return new_df

    return df_map_func


def value_map(m, in_col, out_col):
    """
    Wraps the value mapping operation in a function that takes a DataFrame as
    input and returns a single column DataFrame (plus index).

    :param m: Can be a dict, regex pattern string, or a function that takes a
        string.

        If a function, the return column values wil be the result of applying
        that function to each cell of the <in_col> column in the DataFrame.

        If a dict, each dict key is a regex pattern string that is forced to
        have ^$ anchors if not already present (or none type), and each dict
        value is either a constant or a function that takes one or more basic
        strings. When a cell in the <in_col> column in the DataFrame matches
        the regex pattern, corresponding cells in the <out_col> column of the
        output will be either replaced by the indicated constant value or they
        will be filled by application of the indicated function to the value of
        the cell in <in_col>. If the regex pattern has capture groups, those
        become the function inputs instead of the entire cell value.
        For more details, read the docstring for safe_pandas_replace in
        common/pandas_utils.py

        If a regex string, the result will be the same as if it had been
        `{m: lambda x: x}`.

    :param in_col: The name of the column in the input DataFrame (the file)
    :param out_col: The standard concept column in the extract output to
    populate
    :return: A function that applies the specified m operation
    """
    assert_safe_type(m, callable, dict, str)

    def value_map_func(df):
        new_df = DataFrame()
        if callable(m):
            new_df[out_col] = get_col(df, in_col).apply(m)
        elif isinstance(m, str):
            new_df[out_col] = safe_pandas_replace(
                get_col(df, in_col), {m: lambda x: x}, True
            )
        else:  # dict
            new_df[out_col] = safe_pandas_replace(get_col(df, in_col), m, True)
        assert new_df.index.equals(df.index)
        return new_df

    return value_map_func


def row_map(m, out_col):
    """
    Wraps the row mapping operation in a function that takes a DataFrame as
    input and returns a single column DataFrame with index values equal to the
    relevant original indices.

    :param m: A function that takes a row from the input DataFrame (the
        file) and returns a single value using the cells in that row
    :param out_col: The standard concept column in the extract output to
        populate
    :return: A function that applies the specified m operation
    """
    assert_safe_type(m, callable)

    def row_map_func(df):
        new_df = DataFrame()
        new_df[out_col] = df.apply(m, axis=1)
        assert new_df.index.equals(df.index)
        return new_df

    return row_map_func


def constant_map(m, out_col):
    """
    This is a special case map that returns a column filled with all the same
    value.

    Wraps the const mapping operation in a function that takes only a DataFrame
    as input.

    :param m: A constant value
    :param out_col: The standard concept column in the extract output to
        populate
    :return: A function that applies the specified m operation
    """

    def constant_map_func(df):
        new_df = DataFrame(index=df.index.copy())
        new_df[out_col] = [m] * len(df)
        assert new_df.index.equals(df.index)
        return new_df

    return constant_map_func


def column_map(m, in_col, out_col):
    """
    Wraps the column mapping operation in a function that takes only a
    DataFrame as input.

    :param m: A function to apply to an entire column from the input
        DataFrame (the file)
    :param in_col: Which column from the input DataFrame to use
    :param out_col: The standard concept column in the extract output to
        populate
    :return: A function that applies the specified m operation
    """
    assert_safe_type(m, callable)

    def column_map_func(df):
        new_df = DataFrame()
        new_df[out_col] = m(get_col(df, in_col).copy())
        return new_df

    return column_map_func


def keep_map(in_col, out_col):
    """
    Shorthand for value_map or column_map with `m=lambda x: x`

    :param in_col: Which column from the input DataFrame to use
    :param out_col: The standard concept column in the extract output to
        populate
    :return: A function that transfers values from in_col to out_col
    """
    return column_map(lambda x: x, in_col, out_col)


def melt_map(var_name, map_for_vars, value_name, map_for_values):
    """
    Wraps the melt mapping operation in a function that takes only a DataFrame
    as input.

    melt_map is a combination of the pandas melt operation and the value_map
    operation. It both reshapes data and maps values.

    :param var_name: What to name the melt's resulting var column
    :param map_for_vars: A dict with keys equal to column names or numeric
        indices from the input DataFrame (the file) and values equal to
        replacement names
    :param value_name: What to name the melt's resulting value column
    :param map_for_values: See the definition for the m argument of the
        value_map operation
    """
    assert_safe_type(map_for_vars, dict)
    assert_safe_type(map_for_values, dict, callable)

    def melt_map_func(df):
        new_df = DataFrame()
        for k, v in map_for_vars.items():
            col_melt_df = DataFrame({v: get_col(df, k)}).melt(
                var_name=var_name, value_name=value_name
            )
            col_melt_df.index = df.index.copy()
            new_df = new_df.append(col_melt_df, sort=False)

        new_df[value_name] = value_map(map_for_values, value_name, value_name)(
            new_df
        )
        return new_df

    return melt_map_func
