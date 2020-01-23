"""
Auto-generated transform module

Replace the contents of transform_function with your own code

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing transform_function.
"""
import os

from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    dfs = {os.path.basename(fp): df for fp, df in mapped_df_dict.items()}

    return {DEFAULT_KEY: dfs["extract_config.py"]}
