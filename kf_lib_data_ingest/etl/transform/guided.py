"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the mapped
source data tables should be merged into a single table containing all of the
mapped source data
"""
import os
from collections import defaultdict

import pandas

from kf_lib_data_ingest.common.type_safety import assert_safe_type
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)
from kf_lib_data_ingest.common.concept_schema import UNIQUE_ID_ATTR
from kf_lib_data_ingest.etl.transform.transform import TransformStage


class GuidedTransformStage(TransformStage):
    def __init__(self, transform_function_path, *args, **kwargs):
        self.transform_module = TransformModule(transform_function_path)
        super().__init__(*args, **kwargs)

    def _write_output(self, output):
        """
        Write normal transform stage output (see TransformStage._write_output)
        and also the write out the dataframe from the user defined transform
        function (output from _apply_transform_funct).

        :param output: See TransformStage._write_output
        :type output: dict
        """
        fp = os.path.join(self.stage_cache_dir, 'transform_func_df.tsv')
        self.logger.info(f'Writing intermediate data - output of user defined'
                         f' transform func to {fp}')
        self.transform_func_df.to_csv(fp, sep='\t', index=True)

        super()._write_output(output)

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
        merged_df = transform_funct(df_dict)

        # Validation of transform function output
        assert_safe_type(merged_df, pandas.DataFrame)
        self.transform_func_df = merged_df

        return merged_df

    def _standard_to_target(self, all_data_df):
        """
        Convert a DataFrame containing all of the mapped source data into a
        dict of lists of dicts. The list of dicts represent lists of target
        concept instances.

        For example,

        This:

        |CONCEPT.PARTICIPANT.ID | CONCEPT.PARTICIPANT.GENDER| CONCEPT.BIOSPECIMEN.ID| # noqa E5501
        |-----------------------|---------------------------|-----------------------| # noqa E5501
        |        P1             |            Female         |            B1         | # noqa E5501
        |        P2             |            Male           |            B2         | # noqa E5501

        Turns into:

        {
            'participant': [
                {
                    'id': 'P1',
                    'properties': {
                        'gender': 'Female',
                         ...
                    }
                },
                {
                    'id': 'P2',
                    'properties': {
                        'gender': 'Male'
                         ...
                    }
                }
            ],
            'biospecimen': [
                {
                    'id': 'B1',
                    'properties': {
                         ...
                    }
                },
                {
                    'id': 'B2',
                    'properties': {
                         ...
                    }
                }
            ]
        }

        :param all_data_df: the output of the user transform function
        :type all_data_df: pandas.DataFrame
        :returns target_instances: dict (keyed by target concept) of lists
        of dicts (target concept instances)
        """
        self.logger.info('Begin transformation from standard concepts '
                         'to target concepts ...')

        target_instances = defaultdict(list)
        for (target_concept,
             config) in self.target_api_config.concept_schemas.items():

            # Unique key for the target concept must exist
            standard_concept = config.get('standard_concept')
            std_concept_ukey = getattr(standard_concept, UNIQUE_ID_ATTR)
            if std_concept_ukey not in all_data_df.columns:
                self.logger.info(
                    'No unique key found in table for target '
                    f'concept: {target_concept}. Skip instance creation')
                continue

            # Drop duplicates using unique key of std concept
            df = all_data_df.drop_duplicates(subset=std_concept_ukey)

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
        all_data_df = self._apply_transform_funct(data_dict)

        # Insert unique key columns
        self._insert_unique_keys(
            {
                self.transform_module.config_filepath: (
                    'Transform Module Output: all_data_df',
                    all_data_df)
            }
        )

        # Transform from standard concepts to target concepts
        target_instances = self._standard_to_target(all_data_df)

        return target_instances
