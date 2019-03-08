"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the source data tables
should be merged in order to yield a single table per target service entity.
"""
from collections import defaultdict
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
from kf_lib_data_ingest.common.concept_schema import UNIQUE_ID_ATTR
from kf_lib_data_ingest.etl.transform.transform import TransformStage


class GuidedTransformStage(TransformStage):
    def __init__(self, transform_function_path, *args, **kwargs):
        self.transform_module = TransformModule(transform_function_path)
        super().__init__(*args, **kwargs)

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
        self.logger.info('Applying user supplied transform function '
                         f'{filepath} ...')

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

    def _standard_to_target(self, entity_df_dict):
        """
        Convert a dict of standard concept DataFrames into a dict of lists of
        dicts. The list of dicts represent lists of target concept instances.

        For example,

        This:

        {
            'participant':

                |CONCEPT.PARTICIPANT.ID | CONCEPT.PARTICIPANT.GENDER|
                |-----------------------|---------------------------|
                |        P1             |            Female         |
                |        P2             |            Male           |
        }

        Turns into:

        {
            'participant': [
                {
                    'id': 'P1',
                    'properties': {
                        'gender': 'Female'
                    }
                },
                {
                    'id': 'P2',
                    'properties': {
                        'gender': 'Male'
                    }
                }
            ]
        }

        :param entity_df_dict: the output of the user transform function
        :type entity_df_dict: dict
        :returns target_instances: dict (keyed by target concept) of lists
        of dicts (target concept instances)
        """
        self.logger.info('Begin transformation from standard concepts '
                         'to target concepts ...')

        target_instances = defaultdict(list)
        for (target_concept,
             config) in self.target_api_config.concept_schemas.items():

            # Get DataFrame for the target_concept
            df = entity_df_dict.get(target_concept)

            # No data exists for the target_concept
            if df is None:
                self.logger.info('No table found for target concept: '
                                 f'{target_concept}, skipping transformation')
                continue

            # Unique key for the target concept must exist
            standard_concept = config.get('standard_concept')
            std_concept_ukey = getattr(standard_concept, UNIQUE_ID_ATTR)
            if std_concept_ukey not in df.columns:
                self.logger.info(
                    'No unique key found in table for target '
                    f'concept: {target_concept}. Skip instance creation')
                continue

            # Drop duplicates using unique key of std concept
            df = df.drop_duplicates(subset=std_concept_ukey)

            # Build target instances for target_concept (i.e. participant)
            total = df.shape[0]
            self.logger.info(f'Building {total} {target_concept} concepts ...')
            for _, row in df.iterrows():
                target_instance = {}
                # id
                target_instance['id'] = row[std_concept_ukey]

                # endpoint
                target_instance['endpoint'] = config['endpoint']

                # properties
                target_instance['properties'] = defaultdict()
                for (target_attr,
                     std_concept_attr) in config['properties'].items():
                    target_instance[
                        'properties'][target_attr] = row.get(std_concept_attr)

                # links
                target_instance['links'] = defaultdict()
                if 'links' in config:
                    for (target_attr,
                         std_concept_attr) in config['links'].items():
                        target_instance[
                            'links'][target_attr] = row.get(std_concept_attr)

                target_instances[target_concept].append(target_instance)

        return target_instances

    def _do_transform(self, data_dict):
        """
        See TransformStage._do_transform
        """

        # Apply user transform func
        target_concept_df_dict = self._apply_transform_funct(data_dict)

        # Transform from standard concepts to target concepts
        target_instances = self._standard_to_target(target_concept_df_dict)

        return target_instances
