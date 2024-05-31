"""
Auto-generated transform module

Replace the contents of transform_function with your own code

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing transform_function.
"""

from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    sr = mapped_df_dict["sample_relationship.py"]
    sr = sr.dropna()

    return {DEFAULT_KEY: mapped_df_dict["sample.py"], "sample_relationship": sr}
