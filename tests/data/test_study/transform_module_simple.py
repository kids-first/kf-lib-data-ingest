from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):

    return {DEFAULT_KEY: list(mapped_df_dict.values())[0]}
