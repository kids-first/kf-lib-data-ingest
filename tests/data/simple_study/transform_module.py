"""
Auto-generated transform module

Replace the contents of transform_function with your own code

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing transform_function.
"""
import os

# Use these merge funcs, not pandas.merge
from kf_lib_data_ingest.common.pandas_utils import (
    outer_merge,
    merge_wo_duplicates
)
from kf_lib_data_ingest.common.concept_schema import CONCEPT


def transform_function(mapped_df_dict):
    dfs = {
        os.path.basename(fp): df
        for fp, df in
        mapped_df_dict.items()
    }

    return dfs['extract_config.py']
