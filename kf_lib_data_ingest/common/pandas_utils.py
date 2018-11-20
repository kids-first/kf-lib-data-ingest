"""
Utility functions to improve Pandas's rough edges and deficiencies.
"""
import re

import numpy
import pandas

from common.type_safety import assert_safe_type


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
    :returns: The indicated column from the dataframe, either by name or order.
    """
    if isinstance(key, int) and (key not in df.columns):
        return df[df.columns[key-1]]  # the keyth column
    else:
        return df[key]


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

    Args:
        data: a DataFrame or Series
        mappings (dict): with form {column:{original:replacement, ...}, ...},
            or just {original:replacement, ...}, or some combination of the two
            as in {column:{original1:replacement, ...}, original2:replacement}.
            The combined case will apply non-column-specific replacements on
            all columns after any column-specific replacements are applied.
        regex (bool): Whether to treat mapping match patterns as regular
            expressions.

    Returns:
        Same as pandas.Series.replace and pandas.DataFrame.replace, except less
        wrong.
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
                regex
            )
    else:   # one column Series
        # Forcing dtype to `object` allows differentiating None from np.nan,
        # which means that you can replace cells with None and not have those
        # changes overwritten.
        output = pandas.Series(index=data.index, dtype=object)

        for k, v in mappings.items():
            # Keep track of which output cells are still nan, because those are
            # the only ones we're allowed to fill from the next replacement.
            nulls = output.apply(lambda x: x is numpy.nan)

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
                new[pandas.notnull(matches[0])] = (
                    matches.astype(str).apply(lambda row: v(*row), axis=1)
                )
            else:  # basic replacement
                new = data.replace({k: v}, regex=True)

            # Fill only the remaining holes with new values
            output[nulls] = new[new != data][nulls]

        # Fill any remaining holes with original values
        nulls = output.apply(lambda x: x is numpy.nan)
        output[nulls] = data[nulls]

    return output
