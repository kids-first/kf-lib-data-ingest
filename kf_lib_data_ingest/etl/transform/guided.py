"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the source data tables
should be merged in order to yield a single table per target service entity.
"""
import logging
from pprint import pformat

from pandas import DataFrame

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

    def _apply_transform_funct(self, df_dict):
        """
        Apply user supplied transform function to merge dataframes in df_dict
        into a new set of dataframes, so that each target entity has 1
        dataframe with all of its data.

        :param df_dict: a dict of mapped dataframes from the ExtractStage
        :type df_dict: dict
        :returns: a dict of dataframes
        """
        filepath = self.transform_module.config_filepath
        self.logger.info(f'Applying user supplied transform function '
                         '{filepath} ...')

        # Apply user supplied transform function
        transform_funct = self.transform_module.transform_function
        entity_df_dict = transform_funct(df_dict)

        # Must return a dict of dataframes
        assert_safe_type(entity_df_dict, dict)
        assert_all_safe_type(entity_df_dict.values(), DataFrame)

        # Keys must be valid target service entities
        target_concepts = set(self.target_api_config.concept_schemas.keys())
        error = KeyError(f"Error in transform_function's "
                         f"return value, file: {filepath}. Keys in dict "
                         "must be valid target_concepts, one of: \n"
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

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined in _validate_run_parameters.
        """
        self.logger.info('Begin guided transformation ...')
        # Reformat the data_dict into expected form for transform_funct
        # Turn data_dict[<extract config url>] = (source data url, dataframe)
        # into df_dict[<extract config url>] = dataframe
        df_dict = {k: v[1] for k, v in data_dict.items()}

        entity_df_dict = self._apply_transform_funct(df_dict)

        return entity_df_dict
