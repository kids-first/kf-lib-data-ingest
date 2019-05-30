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
    merge_wo_duplicates
)
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    """
    Merge DataFrames in mapped_df_dict into 1 DataFrame if possible.

    Return a dict that looks like this:

    {
        'default': all_merged_data_df
    }

    If not possible to merge all DataFrames into a single DataFrame then
    you can return a dict that looks something like this:

    {
        '<name of target concept>': df_for_<target_concept>,
        'default': all_merged_data_df
    }

    Target concept instances will be built from the default DataFrame, unless
    another DataFrame is explicitly provided via a key, value pair in the
    output dict. They key must match the name of an existing target concept.
    The value will be the DataFrame to use when building instances of the
    target concept.

    A typical example would be:

    {
        'family_relationship': family_relationship_df,
        'default': all_merged_data_df
    }

    """

    df = list(mapped_df_dict.values())[0]

    return {
        DEFAULT_KEY: df
    }
