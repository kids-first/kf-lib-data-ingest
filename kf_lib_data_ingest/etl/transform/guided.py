"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the source data tables
should be merged in order to yield a single table per target service entity.
"""
import logging
from pprint import pformat

import pandas

from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)


class GuidedTransformer():
    def __init__(self, target_api_config, transform_function_path):
        self.logger = logging.getLogger(type(self).__name__)
        self.transform_module = TransformModule(transform_function_path)
        self.target_api_config = target_api_config

    def _apply_transform_funct(self, data_dict):
        """
        Apply user supplied transform function to merge dataframes in data_dict
        into a new set of dataframes, so that each target entity has 1
        dataframe with all of its mapped data from extract stage.

        :param df_dict: a dict of mapped dataframes from the ExtractStage
        :type df_dict: dict
        :returns: a dict of dataframes
        """
        filepath = self.transform_module.config_filepath
        self.logger.info(f'Applying user supplied transform function '
                         '{filepath} ...')

        # Reformat the data_dict into expected form for transform_funct
        # Turn data_dict[<extract config url>] = (source data url, dataframe)
        # into df_dict[<extract config url>] = dataframe
        df_dict = {k: v[1] for k, v in data_dict.items()}

        # Apply user supplied transform function
        transform_funct = self.transform_module.transform_function
        entity_df_dict = transform_funct(df_dict)

        # -- Validation of transform function output --
        # Must return a dict of dataframes
        assert_safe_type(entity_df_dict, dict)
        assert_all_safe_type(entity_df_dict.values(), pandas.DataFrame)

        # Keys must be valid target service entities
        target_concepts = set(self.target_api_config.concept_schemas.keys())
        error = KeyError(f"Error in transform_function's "
                         f"return value, file: {filepath}. Keys in dict "
                         "must be valid target_concepts. Must be one of: \n"
                         f"{pformat(target_concepts)}")
        if not entity_df_dict.keys():
            raise error
        for key in entity_df_dict.keys():
            if key not in target_concepts:
                raise error

        return entity_df_dict

    def run(self, data_dict):
        """
        Transform the tabular mapped data into a unified standard form,
        then transform again from the standard form into a dict of lists.
        Keys are target entity types and values are lists of target entity
        dicts.

        :param data_dict: the output (a dict of mapped DataFrames) from
        ExtractStage.run. See TransformStage._validate_run_parameters for
        a more detailed description.
        :type data_dict: dict
        """
        output = {}
        entity_df_dict = self._apply_transform_funct(data_dict)

        # TODO - more coming soon

        # Temporary - set output to raw entity_df_dict
        output = entity_df_dict

        return output
