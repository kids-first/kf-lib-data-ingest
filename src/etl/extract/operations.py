"""
Extract-stage predefined operations.

Every extract config file declares a sequence of operations for pulling columns
of data out of a clinical file. Each operation is defined as a function that
takes the original data as input and returns one or more columns.

This module stores pre-made function wrappers that when invoked will return
functions for performing common actions such as fetching a column and
replacing its values with other values.

See: docs/design/extract_config_format.py for function details
"""
from pandas import DataFrame
from common.pandas_utils import get_col, safe_pandas_replace
from common.type_safety import function, assert_safe_type


def df_map(m):
    """
    For when you just can't figure out how to do what you want to do with the
    other predefined operations. Take a whole dataframe and return a dataframe
    with as many standard concept columns as you want.

    Wraps the dataframe mapping operation in a function that takes only a
    dataframe as input and returns a new dataframe.

    :param m: A function to apply to a DataFrame
    :returns: A function that applies the specified m operation
    """
    assert_safe_type(m, function)

    def df_map_func(df):
        new_df = df.copy()
        res_df = m(new_df)
        # Output of df mapping func must be a DataFrame
        assert_safe_type(res_df, DataFrame)
        return res_df

    return df_map_func


def value_map(m, in_col, out_col):
    """
    Wraps the value mapping operation in a function that takes a dataframe as
    input and returns a single column.

    :param m: Can be a dict or a function that takes a basic string. If a
        function, the return columns values wil be the result of applying that
        function to each cell of the <in_col> column in the dataframe. If a
        dict, each dict key is a regex pattern that is forced to have ^$
        anchors if not already present (or none type) and each dict value is
        either a constant or a function that takes a basic string. When a cell
        in the <in_col> column in the dataframe matches the regex pattern,
        corresponding cells in the <out_col> column of the output will be
        either replaced by the indicated constant value or they will be filled
        by application of the indicated function to the value of the cell in
        <in_col>. If the regex pattern has capture groups, those become the
        function inputs instead of the entire cell value.
    :param in_col: The name of the column in the input dataframe (the file)
    :param out_col: The standard concept column in the extract output to
        populate
    :returns: A function that applies the specified m operation
    """
    assert_safe_type(m, function, dict)

    def value_map_func(df):
        new_df = DataFrame()
        if function(m):
            new_df[out_col] = get_col(df, in_col).apply(m)
        else:  # dict
            new_df[out_col] = safe_pandas_replace(
                get_col(df, in_col), m, True
            )
        return new_df

    return value_map_func


def row_map(m, out_col):
    """
    Wraps the row mapping operation in a function that takes only a dataframe
    as input. See: docs/design/extract_config_format.py for details

    :param m: A function that takes a row from the input dataframe (the
        file) and returns a single value using the cells in that row
    :param out_col: The standard concept column in the extract output to
        populate
    :returns: A function that applies the specified m operation
    """
    assert_safe_type(m, function)

    def row_map_func(df):
        new_df = DataFrame()
        new_df[out_col] = df.apply(m, axis=1)
        return new_df

    return row_map_func


def constant_map(m, out_col):
    """
    This is a special case of the row_map that returns a column filled with
    all the same value.

    Wraps the const mapping operation in a function that takes only a dataframe
    as input. See: docs/design/extract_config_format.py for details

    :param m: A constant value
    :param out_col: The standard concept column in the extract output to
        populate
    :returns: A function that applies the specified m operation
    """
    assert_safe_type(m, int, float, bool, str, bytes)

    def constant_map_func(df):
        new_df = DataFrame()
        new_df[out_col] = [m] * len(df)
        return new_df

    return constant_map_func


def column_map(m, in_col, out_col):
    """
    Wraps the column mapping operation in a function that takes only a
    dataframe as input. See: docs/design/extract_config_format.py for details

    :param m: A function to apply to an entire column from the input
        dataframe (the file)
    :param in_col: Which column from the input dataframe to use
    :param out_col: The standard concept column in the extract output to
        populate
    :returns: A function that applies the specified m operation
    """
    assert_safe_type(m, function)

    def column_map_func(df):
        new_df = DataFrame()
        new_df[out_col] = get_col(df, in_col)
        new_df[out_col] = m(new_df[out_col])
        return new_df

    return column_map_func


def melt_map(var_name, map_for_vars, value_name, map_for_values):
    """
    Wraps the melt mapping operation in a function that takes only a dataframe
    as input. See: docs/design/extract_config_format.py for details

    melt_map is a combination of the pandas melt operation and the value_map
    operation. It both reshapes data and maps values.

    :param var_name: What to name the melt's resulting var column
    :param map_for_vars: A dict with keys equal to column names or numeric
        indices from the input dataframe (the file) and values equal to
        replacement names
    :param value_name: What to name the melt's resulting value column
    :param map_for_values: See the definition for the m argument of the
        value_map operation
    """
    assert_safe_type(map_for_vars, dict)
    assert_safe_type(map_for_values, dict, function)

    def melt_map_func(df):
        new_df = DataFrame()
        for k, v in map_for_vars.items():
            new_df[v] = get_col(df, k)

        new_df = new_df.melt(
            var_name=var_name,
            value_name=value_name
        )

        new_df[value_name] = value_map(
            map_for_values, value_name, value_name
        )(new_df)
        return new_df

    return melt_map_func
