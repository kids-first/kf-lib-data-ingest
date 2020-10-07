"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the mapped
source data tables should be merged into a single table containing all of the
mapped source data
"""
import os

import pandas

from kf_lib_data_ingest.common.misc import clean_up_df
from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule,
)
from kf_lib_data_ingest.etl.transform.transform import TransformStage


class GuidedTransformStage(TransformStage):
    def __init__(self, transform_function_path, *args, **kwargs):
        self.transform_module = TransformModule(transform_function_path)
        super().__init__(*args, **kwargs)
        self.transform_func_dir = os.path.join(
            self.stage_cache_dir, "transform_function_output"
        )

    def _apply_transform_funct(self, data_dict):
        """
        Apply user supplied transform function to merge dataframes in data_dict
        into a new set of dataframes, so that each target entity has 1
        dataframe with all of its mapped data from extract stage.

        :param df_dict: a dict of mapped dataframes from the ExtractStage
        :type df_dict: dict
        :return: a dict of dataframes
        :rtype: dict
        """
        filepath = self.transform_module.config_filepath
        self.logger.info(
            "Applying user supplied transform function " f"{filepath} ..."
        )

        # Apply user supplied transform function
        transform_funct = self.transform_module.transform_function
        merged_df_dict = transform_funct(data_dict)

        # Validation of transform function output
        assert_safe_type(merged_df_dict, dict)
        assert_all_safe_type(merged_df_dict.keys(), str)
        assert_all_safe_type(merged_df_dict.values(), pandas.DataFrame)

        return merged_df_dict

    def _run(self, data_dict):
        """
        See TransformStage._run
        """
        # Apply user transform func
        output = self._apply_transform_funct(data_dict)

        # Clean up dfs
        for key, df in output.items():
            output[key] = clean_up_df(df)

        return output
