from collections import defaultdict
import logging
from functools import reduce
from math import gcd

import pandas

from kf_lib_data_ingest.common.concept_schema import concept_set
from kf_lib_data_ingest.common.misc import clean_up_df
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    is_function,
)
from kf_lib_data_ingest.common.pandas_utils import split_df_rows_on_splits
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)
from kf_lib_data_ingest.etl.configuration.extract_config import ExtractConfig
from kf_lib_data_ingest.etl.extract import operations as extract_operations


def lcm(number_list):
    """
    Returns the least common multiple from a list of numbers.
    """
    return reduce(lambda x, y: x * y // gcd(x, y), number_list)


def ordinal(n):
    """
    Convert a positive integer into its ordinal representation
    """
    suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    return str(n) + suffix


class Extractor(object):
    """
    Encapsulates the functionality to clean and standardize a source DataFrame
    according to the define operations in the extract configuration file for
    the DataFrame.

    Used internally by the ExtractStage or can be used as a standalone utility.
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(type(self).__name__)
        self.messages = []

    def _log_operation(self, op, nth):
        """Log execution of an extract operation.

        :param op: an extract operation
        :type op: function
        :param nth: which operation number
        :type nth: int
        """
        opname = op.__qualname__
        if op.__module__ == extract_operations.__name__:
            opname = opname.split(".")[0]
        msg = f"Applying {ordinal(nth)} operation: {opname}"
        if op.__closure__:
            msg += " with " + str(
                {
                    k: v.cell_contents
                    for k, v in zip(op.__code__.co_freevars, op.__closure__)
                }
            )
        self.logger.info(msg)

    def _obvert_visibility(self, df):
        """
        If something is visible, then also record that it's not hidden, and
        vice versa.
        """

        def flip_col(in_col):
            out_col = in_col.copy()
            out_col[out_col.notnull()] = (
                ~in_col[in_col.notnull()].astype(bool)
            ).astype(object)
            return out_col

        for concept in concept_set:
            if concept.HIDDEN in df and concept.VISIBLE not in df:
                df[concept.VISIBLE] = flip_col(df[concept.HIDDEN])
            elif concept.VISIBLE in df and concept.HIDDEN not in df:
                df[concept.HIDDEN] = flip_col(df[concept.VISIBLE])
            elif concept.VISIBLE in df and concept.HIDDEN in df:
                assert df[concept.HIDDEN].equals(flip_col(df[concept.VISIBLE]))

    def _chain_operations(self, df_in, operations, _nth=1, _is_nested=False):
        """
        Performs the operations sequence for extracting columns of data from
        the source data files.

        :param df_in: A pandas dataframe containing all of the file data
        :param operations: List of operations to perform
        :return: A pandas dataframe containing extracted mapped data
        :rtype: DataFrame
        """
        skip_messages = []
        out_cols = defaultdict(lambda: pandas.Series(dtype=object))
        original_length = df_in.index.size

        # collect columns of extracted data
        for i, op in enumerate(operations):
            # apply operation(s), get result
            if is_function(op):
                self._log_operation(op, i + _nth)
                res = op(df_in)
                if isinstance(res, extract_operations.SkipOptional):
                    nc = res.needed_columns
                    waswere = (
                        f" '{nc[0]}' was" if len(nc) == 1 else f"s {nc} were"
                    )
                    msg = (
                        f"⚠️ {ordinal(i + _nth)} operation was skipped because"
                        f" it is marked optional and column{waswere} not found."
                    )
                    skip_messages.append(msg)
                    self.logger.warning(msg)
                    continue

                # result length must be a whole multiple of the original
                # length, otherwise we've lost rows
                if res.index.size % original_length != 0:
                    raise ConfigValidationError(
                        "Operation result length is not a multiple of the "
                        "source data length."
                    )

                if (res.index.size / original_length > 1) and not _is_nested:
                    raise ConfigValidationError(
                        "Operations returning results longer than the source "
                        "data length are required to be nested in "
                        "context-appropriate sublists as a safeguard against "
                        "accidental misuse. To learn about the importance of "
                        "nested operation sublists, read "
                        "https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract/index.html#nested-operations-sublists"  # noqa E501
                    )
            else:  # list
                self.logger.info("Diving into nested operation sublist.")
                res, skip_ms = self._chain_operations(df_in, op, i + _nth, True)
                skip_messages.extend(skip_ms)

            for col_name, col_series in res.iteritems():
                out_cols[col_name] = out_cols[col_name].append(
                    col_series, ignore_index=False
                )

        self.logger.info("Done with the operations list.")

        if not out_cols:
            raise Exception("No columns were extracted.")

        # the output dataframe length will be the least common multiple of the
        # extracted column lengths
        length_lcm = lcm(list(map(len, out_cols.values())))

        self.logger.debug("Extracted column lengths are:")
        for col_name, col_series in out_cols.items():
            self.logger.debug("- %s: %d", col_name, col_series.size)

        self.logger.info("Equalizing column lengths to the LCM: %d", length_lcm)

        # Given a set of different length columns, we need to make a resulting
        # dataframe whose length is the least common multiple of their lengths
        # by repeating each column the right number of times. This is
        # predicated on the assumption that no rows were added or removed.
        #
        # A data file that looks like...
        #
        #  index   A   B   C
        #  0       1   3   5
        #  1       2   4   6
        #
        # ... (with B and C each being melted) temporarily extracts to ...
        #
        #  A_index A | DESCRIPTION_index DESCRIPTION | VALUE_index VALUE
        #  0       1 | 0                 B           | 0           3
        #  1       2 | 1                 B           | 1           4
        #            | 0                 C           | 0           5
        #            | 1                 C           | 1           6
        #
        # ... and will then repeat column A here to fill the DataFrame ...
        #
        #  index   A   DESCRIPTION   VALUE
        #  0       1   B             3
        #  1       2   B             4
        #  0       1   C             5
        #  1       2   C             6

        df_out = pandas.DataFrame(dtype=object)
        index = None
        for col_name, col_series in out_cols.items():
            if not col_series.empty:
                length_multiplier = length_lcm / col_series.size
                assert length_multiplier == round(length_multiplier)
                # repeat the series length_multiplier times
                df_out[col_name] = pandas.Series(
                    list(col_series) * round(length_multiplier), dtype=object
                )
                col_index = list(col_series.index) * round(length_multiplier)
                if index:
                    if index != col_index:
                        raise Exception(
                            "Inconsistent column indices.", index, col_index
                        )
                else:
                    index = col_index
        df_out.index = index
        return df_out, skip_messages

    def extract(self, df, extract_cfg_or_path, apply_after_read_func=True):
        """
        Apply the operations in an extract config to the DataFrame to
        produce a clean DataFrame with columns mapped to the standard
        set of columns defined in the concept schema:
        kf_lib_data_ingest.common.concept_schema

        :param df: source data
        :type df: pandas.DataFrame
        :param extract_cfg_or_path: either the ExtractConfig object or path to the
        extract config so the ExtractConfig object can be instantiated
        :type extract_cfg_or_path: str or ExtractConfig

        :returns: the extracted pandas.DataFrame
        """
        self.messages = []

        # Check inputs
        assert_safe_type(df, pandas.DataFrame)
        assert_safe_type(extract_cfg_or_path, str, ExtractConfig)
        if df.empty:
            raise Exception("Extraction failed! DataFrame cannot be empty")

        # Load extract config
        if isinstance(extract_cfg_or_path, str):
            self.extract_config = ExtractConfig(extract_cfg_or_path)
        else:
            self.extract_config = extract_cfg_or_path
            self.extract_config._validate()

        extract_config = self.extract_config

        # Clean df
        df_in = clean_up_df(df)

        # Optionally post-process df
        if apply_after_read_func and extract_config.do_after_read:
            self.logger.info("Calling custom do_after_read function.")
            df_in = extract_config.do_after_read(df)

            # Check for empty df
            if len(df_in.index) == 0:
                raise ConfigValidationError(
                    "Source DataFrame is empty. Check your do_after_read function"
                )

        # Describe Dataframe
        self.logger.debug(f"Read DataFrame with dimensions {df_in.shape}")
        self.logger.debug(f"Column headers are: {list(df_in.columns)}")

        # Check operations
        if not extract_config.operations:
            self.logger.info("The operation list is empty. Nothing to do.")
            return None

        # Run extraction
        df_out, file_skips = self._chain_operations(
            df_in, extract_config.operations
        )

        # record skipped operations
        if file_skips:
            self.messages.append(
                f"In file {extract_config.config_file_relpath}:"
            )
            self.messages.extend(f"\t{s}" for s in file_skips)

        # split value lists into separate rows
        df_out = split_df_rows_on_splits(df_out.reset_index()).set_index(
            "index"
        )
        df_out.index.name = None

        # VISIBLE = not HIDDEN
        self._obvert_visibility(df_out)

        # standardize values again after operations
        return clean_up_df(df_out)
