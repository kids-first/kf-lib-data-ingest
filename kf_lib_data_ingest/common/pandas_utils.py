"""
Utility functions to improve Pandas's rough edges and deficiencies.
"""

import logging
import re
from collections import defaultdict
from inspect import signature

import numpy
import pandas

from kf_lib_data_ingest.common.misc import multisplit
from kf_lib_data_ingest.common.type_safety import assert_safe_type

logger = logging.getLogger(__name__)


class Split:
    """Object for use with split_df_rows_on_splits to differentiate between
    regular lists of things and lists of things to split.

    Replaces lists with a Split object containing the list.
    """

    def __init__(self, things, group=None):
        assert_safe_type(things, list)
        self.things = things
        self.group = group


def split_df_rows_on_splits(df):
    """
    Take a DataFrame and split any cell values that contain Splits into
    multiple rows. Generally, multiple Splits on the same row will multiply to
    produce the cartesian product of the values in those Splits. However, if
    Splits are assigned a `group` value, then Splits on the same row in the
    same group will be linked to each other so that they do not form a
    cartesian product.

    e.g.:

    a row that looks like...

    {'a': Split([1, 2], 1), 'b': Split([3, 4, 5], 1), 'c': Split([6, 7])}

    ...will become...

    [
        {'a': 1,    'b': 3, 'c': 6},
        {'a': 2,    'b': 4, 'c': 6},
        {'a': None, 'b': 5, 'c': 6},
        {'a': 1,    'b': 3, 'c': 7},
        {'a': 2,    'b': 4, 'c': 7},
        {'a': None, 'b': 5, 'c': 7}
    ]

    :param df: a DataFrame
    :return: a new DataFrame
    :rtype: DataFrame
    """

    def split_row(df_row_dict):
        # Collate groups
        split_groups = defaultdict(dict)
        split_group_lengths = defaultdict(int)
        for k, v in df_row_dict.items():
            if isinstance(v, Split):
                split_groups[v.group][k] = v.things
                split_group_lengths[v.group] = max(
                    split_group_lengths[v.group], len(v.things)
                )
                # split_groups = {group_1: {col1: [1, 2, 3], col2: [a, b]}}
                # split_group_lengths = {group_1: 3}
        row_list = []
        for group, col_dict in split_groups.items():
            if group is not None:  # Non-cartesian-product splits
                for col, things in col_dict.items():
                    col_dict[col] += [None] * (
                        split_group_lengths[group] - len(things)
                    )
                # Now group_1 = {col1: [1, 2, 3], col2: [a, b, None]}}
                for i in range(split_group_lengths[group]):
                    new_row = df_row_dict.copy()
                    for col, things in col_dict.items():
                        new_row[col] = things[i]
                    row_list += split_row(new_row)
                break
            else:  # Cartesian product splits
                for col, things in col_dict.items():
                    for val in things:
                        new_row = df_row_dict.copy()
                        new_row[col] = val
                        row_list += split_row(new_row)
                    break
        return row_list or [df_row_dict]

    split_out = []
    index_key = "__index_IQDBDZVSME7L8YSKX__"  # don't name a column this :)
    df.index.name = index_key
    for row in df.reset_index().to_dict(orient="records"):
        split_out += split_row(row)
    df = pandas.DataFrame.from_dict(split_out, dtype=object).set_index(
        index_key
    )
    df.index.name = None
    return df


def split_df_rows_on_delims(df, delimiters, cols=None, cartesian=True):
    """
    Split row into multiple rows based on delimited strings for all columns in
    cols or df.columns.

    Converts delimited entries into Split objects and then calls
    split_df_rows_on_splits. If cartesian is false, the Splits get assigned to
    a collective group. See docstring for split_df_rows_on_splits.

    :param df: a DataFrame
    :param delimiters: delimiters to split on
    :type delimiters: list
    :param cols: columns to split in (optional)
    :type cols: list
    :param cartesian: generate the cartesian product of split values
    :type cartesian: bool
    :return: a new DataFrame
    :rtype: DataFrame
    """
    assert_safe_type(cols, list, None)
    assert_safe_type(delimiters, list)
    assert_safe_type(cartesian, bool)
    group = None if cartesian else 1
    cols = cols or df.columns
    df = df.copy()
    for i, row in df.iterrows():
        for col in cols:
            val = multisplit(row[col], delimiters)
            if len(val) > 1:
                row[col] = Split(val, group=group)
            else:
                row[col] = val[0]
    return split_df_rows_on_splits(df)


def try_pop(df, key, default=None):
    """
    Like pandas.DataFrame.pop but accepts a default return like dict.pop does.
    """
    try:
        return df.pop(key)
    except KeyError:
        return default


def get_col(df, key):
    """
    Return a DataFrame column by either name or numeric index.

    :param df: A pandas.DataFrame
    :param key: A string or int
    :return: The indicated column from the dataframe, either by name or order.
    """
    try:
        if isinstance(key, int) and (key not in df.columns):
            return df[df.columns[key - 1]]  # the keyth column
        else:
            return df[key]
    except KeyError as e:
        raise KeyError(
            f"Column {key} not found in df. Options are: {list(df.columns)}"
        ) from e


def safe_pandas_replace(data, mappings, regex=False):
    """
    Apply dict-based replacement to DataFrame and Series without cascading
    changes. Once a mapping is applied to a cell it will not be mapped again.
    pandas Series.replacement and DataFrame.replacement methods currently
    will cascade dict-based mappings in whatever arbitrary order Python
    happens to have internally stored the dict keys. That behavior is
    inappropriate and dangerous, and should be considered a critical bug.
    It means that s.replace({'A':'B', 'B':'C'}) will turn As into Cs but
    s.replace({'B':'C', 'A':'B'}) will not, despite the two dicts being
    logically identical and unambiguous.

    The replacement value can also be a function instead of a string, and it
    will receive any regex captures, or the whole match if there aren't any
    captures, and replace the matched cell with the function call result.

    :param data: a DateFrame or Series
    :param mappings: Dictionary with form {column: { original: replacement, ...}, ...},
        or just {original: replacement, ...}, or some combination of the two as in
        {column: {original1: replacement, ...}, original2: replacement}. The combined case
        will apply non-column-specific replacements on all columns after any column-specific
        replacements are applied.
    :type mappings: Dictionary
    :param: regex: Whether to treat mapping match patterns as regular expressions.
    :type regex: Boolean
    :return: Same as pandas.Series.replace and pandas.DataFrame.replace, except less wrong.
    """

    assert_safe_type(data, pandas.DataFrame, pandas.Series)

    if not isinstance(mappings, dict):
        return data

    if isinstance(data, pandas.DataFrame):
        # DataFrames get split into single column Series and then those are run
        # through individually before doing the top level replacements.
        series_maps = {}
        dataframe_maps = {}
        for k, v in mappings.items():
            if isinstance(v, dict):
                series_maps[k] = v
            else:
                dataframe_maps[k] = v
        output = pandas.DataFrame(index=data.index)
        for k, s in data.items():
            # apply dataframe-global map after all series-local maps
            output[k] = safe_pandas_replace(
                safe_pandas_replace(s, series_maps.get(k), regex),
                dataframe_maps,
                regex,
            )
    else:  # one column Series
        # Forcing dtype to `object` allows differentiating None from np.nan,
        # which means that you can replace cells with None and not have those
        # changes overwritten.
        output = pandas.Series(index=data.index, dtype=object)

        for k, v in mappings.items():
            # Keep track of which output cells are still nan, because those are
            # the only ones we're allowed to fill from the next replacement.
            holes = output.apply(lambda x: x is numpy.nan)

            if pandas.isnull(k) or (k == "") or (regex and (k == "^$")):
                k = str(numpy.nan)
            else:
                k = str(k)

            # Turn everything into regex. Always anchor patterns to prevent
            # accidentally catching the "male" inside of "female" and turning
            # it into "feMale" or other similar shenanigans.
            if regex:
                if not k.startswith("^"):
                    k = "^" + k
                if not k.endswith("$"):
                    # not technically right but likely good enough
                    k = k + "$"
            else:
                k = "^" + re.escape(k) + "$"

            if callable(v):  # Apply functions
                # Find the arguments to pass to the function
                try:
                    # Look for capture groups
                    matches = data.str.extract(k)
                except Exception:
                    # If the pattern has no captures, capture the whole thing
                    k = "^(" + k[1:-1] + ")$"
                    matches = data.str.extract(k)

                new = data.copy()
                # create new values using the returns from the function calls
                new[pandas.notnull(matches[0])] = matches.astype(str).apply(
                    lambda row: v(*row), axis=1
                )
            else:  # basic replacement
                new = data.replace({k: v}, regex=True)

            # Fill only the remaining holes with new values
            output[holes] = new[new.astype(str) != data.astype(str)][holes]

        # Fill any remaining holes with original values
        holes = output.apply(lambda x: x is numpy.nan)
        output[holes] = data[holes]

    return output


def merge_wo_duplicates(left, right, left_name=None, right_name=None, **kwargs):
    """
    Merge two dataframes and return a dataframe with no duplicate columns.

    If duplicate columns result from the merge, resolve duplicates by
    filling nans on the left column with values from the right column.

    :param left: left dataframe
    :type left: Pandas.DataFrame
    :param left_name: Optional name of left DataFrame to use in logging
    the DataFrame's uniques using nunique()
    :type left_name: str
    :param right_name: Optional name of right DataFrame to use in logging
    the DataFrame's uniques using nunique()
    :type right_name: str
    :param right: right dataframe
    :type right: Pandas.DataFrame
    :param kwargs: keyword args expected by Pandas.merge function
    """
    left = left.astype(object)
    right = right.astype(object)
    left_name = left_name or "Left"
    right_name = right_name or "Right"

    # Check if merge col(s) are present in DataFrame
    def check_merge_col(merge_on, df_name, df, err_msgs):
        if isinstance(merge_on, str):
            merge_on = [merge_on]
        for col in merge_on:
            if col not in df.columns:
                err_msgs.append(f"'{col}' not found in {df_name}: {df.columns}")
        return err_msgs

    err = []
    if "on" in kwargs:
        on = kwargs["on"]
        err = check_merge_col(on, left_name, left, err)
        err = check_merge_col(on, right_name, right, err)
    elif ("left_on" in kwargs) and ("right_on" in kwargs):
        err = check_merge_col(kwargs["left_on"], left_name, left, err)
        err = check_merge_col(kwargs["right_on"], right_name, right, err)
    else:
        err = [
            (
                "Missing merge column keyword argument(s). Must supply either `on`"
                " or both `left_on` and `right_on` arguments."
            )
        ]

    if err:
        raise Exception("\n".join(err))

    def resolve_duplicates(df, suffixes):
        l_suffix = suffixes[0]
        r_suffix = suffixes[1]

        while True:
            to_del = set()
            for coll in df.columns:
                if coll.endswith(l_suffix):
                    firstpart = coll.split(l_suffix)[0]
                    colr = firstpart + r_suffix
                    inconsistent = (
                        (df[coll] != df[colr])
                        & df[coll].notna()
                        & df[colr].notna()
                    )
                    if any(inconsistent):
                        raise Exception(
                            "Inconsistent data between left and right DFs.\n"
                            f"Kwargs: {kwargs}\n"
                            f"Left side was:\n{left}\n"
                            f"Right side was:\n{right}\n"
                            f"Intermediate was:\n{df}\n"
                            f"Merge collision between: {coll} and {colr}\n"
                            "Mismatching values:\n"
                            f"{df[[coll, colr]][inconsistent]}"
                        )
                    df[firstpart] = df[coll].fillna(df[colr])
                    to_del.update([coll, colr])
            if not to_del:
                break
            else:
                for c in to_del:
                    del df[c]
        return df

    merged = pandas.merge(left, right, **kwargs)
    reduced = resolve_duplicates(merged, kwargs.pop("suffixes", ("_x", "_y")))

    default_how = signature(pandas.merge).parameters["how"].default

    # Hopefully this will help us know that we didn't lose anything important
    # in the merge
    collective_uniques = defaultdict(set)
    for c in left.columns:
        collective_uniques[c] |= set(left[c])
    for c in right.columns:
        collective_uniques[c] |= set(right[c])
    collective_uniques = pandas.DataFrame(
        {c: [len(v)] for c, v in collective_uniques.items()}
    )
    msg = (
        f'*** {kwargs.get("how", default_how).title()} merge {left_name} with '
        f"{right_name}***\n"
        f"-- Left+Right Collective Uniques --\n{collective_uniques}\n"
        f"-- Merged DataFrame Uniques --\n{reduced.nunique()}"
    )
    logger.info(msg)

    return reduced


def outer_merge(
    df1,
    df2,
    with_merge_detail_dfs=True,
    left_name=None,
    right_name=None,
    **kwargs,
):
    """
    Do Pandas outer merge, return merge result and 3 additional dfs if
    with_merge_details=True. The 3 merge detail dataframes are useful for
    quickly identifying missing data:

        both - a dataframe of rows that matched in both the left and right
        dataframes (equivalent to the df returned by an inner join)
        left_only - a dataframe of rows that were ONLY in the left dataframe
        right_only - a dataframe of rows that were ONLY in the right dataframe

    See https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.merge.html # noqa

    :param df1: the left dataframe to be merged
    :type df1: Pandas.DataFrame
    :param df2: the right dataframe to be merged
    :type df2: Pandas.DataFrame
    :param with_merge_detail_dfs: boolean specifying whether to output
    additional dataframes
    :type with_merge_details: boolean
    :param left_name: Name to use in log statements pertaining to df1
    :type left_name: str
    :param left_name: Name to use in log statements pertaining to df2
    :type left_name: str
    the DataFrame's uniques using nunique()
    :param kwargs: keyword args expected by Pandas.merge
    :type kwargs: dict
    :return: 1 dataframe or tuple of 4 dataframes
    """
    kwargs["how"] = "outer"
    kwargs["indicator"] = with_merge_detail_dfs
    outer = merge_wo_duplicates(
        df1, df2, left_name=left_name, right_name=right_name, **kwargs
    )

    if with_merge_detail_dfs:
        detail_dfs = [
            outer[outer["_merge"] == keyword]
            for keyword in ["both", "left_only", "right_only"]
        ]

        ret = []
        for df in [outer] + detail_dfs:
            del df["_merge"]
            ret.append(df.dropna(how="all"))

        return tuple(ret)

    else:
        return outer
