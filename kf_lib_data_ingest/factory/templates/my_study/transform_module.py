"""
Auto-generated transform module

Replace the contents of transform_function with your own code

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing transform_function.
"""

# Use these merge funcs, not pandas.merge
from kf_lib_data_ingest.common.pandas_utils import (
    outer_merge,
    merge_without_duplicates
)


def transform_function(mapped_df_dict):
    # Expected to return a single merged dataframe

    return list(mapped_df_dict.values())[0]
