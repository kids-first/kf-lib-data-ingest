"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the mapped
source data tables should be merged into a single table containing all of the
mapped source data
"""
import os
from collections import defaultdict
from pprint import pformat

import pandas

from kf_lib_data_ingest.config import DEFAULT_KEY
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError
)
from kf_lib_data_ingest.common.concept_schema import (
    UNIQUE_ID_ATTR,
    concept_property_split,
    str_to_CONCEPT
)
from kf_lib_data_ingest.common.misc import clean_up_df
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)
from kf_lib_data_ingest.etl.transform.transform import TransformStage


class GuidedTransformStage(TransformStage):
    def __init__(self, transform_function_path, *args, **kwargs):
        self.transform_module = TransformModule(transform_function_path)
        super().__init__(*args, **kwargs)
        self.transform_func_dir = os.path.join(self.stage_cache_dir,
                                               'transform_function_output')

    def _write_output(self, output):
        """
        Write normal transform stage output (see TransformStage._write_output)
        and also the write out the dataframe from the user defined transform
        function (output from _apply_transform_funct).

        :param output: See TransformStage._write_output
        :type output: dict
        """
        self.logger.info(f'Writing intermediate data - output of user defined'
                         f' transform func to {self.transform_func_dir}')

        os.makedirs(self.transform_func_dir, exist_ok=True)

        for key, df in self.transform_func_output.items():
            fp = os.path.join(self.transform_func_dir, key + '.tsv')
            df.to_csv(fp, sep='\t', index=True)

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
        merged_df_dict = transform_funct(df_dict)

        # Validation of transform function output
        assert_safe_type(merged_df_dict, dict)
        assert_all_safe_type(merged_df_dict.keys(), str)
        assert_all_safe_type(merged_df_dict.values(), pandas.DataFrame)

        valid_keys = set(self.target_api_config.target_concepts.keys())
        valid_keys.add(DEFAULT_KEY)
        for key, df in merged_df_dict.items():
            if key not in valid_keys:
                raise ConfigValidationError(
                    f'Invalid dict key "{key}" found in transform function '
                    f'output! A Key must be one of:\n {pformat(valid_keys)} '
                    f'\nCheck your transform module: '
                    f'{self.transform_module.config_filepath}'
                )

        return merged_df_dict

    def _standard_to_target(self, df_dict):
        """
        For each DataFrame in df_dict, convert the DataFrame containing all of
        the mapped source data into a dict of lists of dicts. The list of dicts
        represent lists of target concept instances.

        When building instances of a particular target concept type, first
        check to see whether an explicit key for that target concept exists.
        If it does, then build the target instances for this target concept
        using the value for that key, which will be a DataFrame.

        Otherwise, use the DataFrame stored in the value for the default key.
        See kf_lib_data_ingest.config.DEFAULT_KEY for the name of the default
        key.

        For example,

        This:

        {
            'diagnosis': diagnosis_df
            'default': merged_df
        }

        where diagnosis_df looks like:

        | DIAGNOSIS.UNIQUE_KEY | PARTICIPANT.UNIQUE_KEY | DIAGNOSIS.EVENT_AGE_DAYS | DIAGNOSIS.NAME  | # noqa E501
        |----------------------|------------------------|--------------------------|-----------------| # noqa E501
        |    P1-CDH-400        |             P1         |         400              |     CDH         | # noqa E501
        |    P2-CNS-500        |             P2         |         500              |     CNS         | # noqa E501

        and merged_df looks like:

        |     BIOSPECIMEN.UNIQUE_KEY    |  BIOSPECIMEN.ANALYTE  |  PARTICIPANT.UNIQUE_KEY | ... | # noqa E501
        |-------------------------------|-----------------------|-------------------------|-----| # noqa E501
        |            B1                 |            DNA        |           P1            | ... | # noqa E501
        |            B2                 |            RNA        |           P2            | ... | # noqa E501

        Turns into:

        {
            'diagnosis': [
                {
                    'id': 'P1-CDH-400',
                    'properties': {
                         'age_at_event_days': 400,
                         'source_text': 'CDH',
                         ...
                    },
                    links: {
                        'participant_id': 'P1'
                    }
                },
                {
                    'id': 'P2-CNS-500',
                    'properties': {
                         'age_at_event_days': 500,
                         'source_text': 'CNS',
                         ...
                    },
                    links: {
                        'participant_id': 'P2'
                    }
                },
            ],
            'biospecimen': [
                {
                    'id': 'B1',
                    'properties': {
                        'analyte_type': DNA
                         ...
                    },
                    links: {
                        'participant_id': 'P1'
                    }
                },
                {
                    'id': 'B2',
                    'properties': {
                        'analyte_type': RNA
                         ...
                    },
                    links: {
                        'participant_id': 'P2'
                    }
                }
            ],
            ...
        }

        :param df_dict: the output of the user transform function after
        unique keys have been inserted
        :type df_dict: a dict of pandas.DataFrames
        :returns target_instances: dict (keyed by target concept) of lists
        of dicts (target concept instances)
        """
        self.logger.info('Begin transformation from standard concepts '
                         'to target concepts ...')

        target_instances = defaultdict(list)
        for (target_concept,
             config) in self.target_api_config.target_concepts.items():

            # Get the df containing data for this target concept
            all_data_df = df_dict.get(target_concept)
            if all_data_df is not None:
                self.logger.info(f'Using {target_concept} DataFrame')
            else:
                all_data_df = df_dict.get(DEFAULT_KEY)

            if all_data_df is not None:
                self.logger.info(f'Using {DEFAULT_KEY} DataFrame')
            else:
                self.logger.warning(
                    f'Cannot build target concept instances for '
                    f'{target_concept}! No DataFrame found in transform '
                    'function output dict.'
                )
                continue

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
            self.logger.info(f'Building {target_concept} concepts ...')
            for _, row in df.iterrows():
                target_instance = {}
                # id
                target_instance['id'] = row[std_concept_ukey]

                # Skip building target instances with null ids
                if not target_instance['id']:
                    continue

                # endpoint
                target_instance['endpoint'] = config['endpoint']

                # properties
                target_instance['properties'] = defaultdict()
                for (target_attr,
                     std_concept_attr) in config['properties'].items():
                    target_instance[
                        'properties'][target_attr] = row.get(std_concept_attr)

                # links
                target_instance['links'] = []
                if 'links' in config:
                    for link_dict in config['links']:
                        unique_k_col = getattr(link_dict['standard_concept'],
                                               UNIQUE_ID_ATTR)
                        unique_k_val = row.get(unique_k_col)
                        target_instance['links'].append(
                            {
                                link_dict['target_attribute']: unique_k_val,
                                'target_concept': link_dict['target_concept']
                            }
                        )
                target_instances[target_concept].append(target_instance)

            self.logger.info(f'Built {len(target_instances[target_concept])} '
                             f'{target_concept} concepts')

        return target_instances

    def _do_transform(self, data_dict):
        """
        See TransformStage._do_transform
        """

        # Apply user transform func
        self.transform_func_output = self._apply_transform_funct(data_dict)

        for key, df in self.transform_func_output.items():
            # Clean up df
            self.transform_func_output[key] = clean_up_df(df)

            # Insert unique key columns
            self._insert_unique_keys(
                {
                    os.path.join(self.transform_func_dir, key + '.tsv'): (
                        f'Transform Module Output: {key} df',
                        self.transform_func_output[key])
                }
            )

        # Transform from standard concepts to target concepts
        target_instances = self._standard_to_target(self.transform_func_output)

        return target_instances

    def _postrun_concept_discovery(self, run_output):
        """
        See the docstring for IngestStage._postrun_concept_discovery
        """
        sources = defaultdict(dict)
        links = defaultdict(
            lambda: defaultdict(set)
        )
        values = defaultdict(
            lambda: defaultdict(set)
        )

        for df_name, df in self.transform_func_output.items():
            self.logger.info(
                f'Doing concept discovery for {df_name} DataFrame in '
                'transform function output'
            )
            for key in df.columns:
                sk = sources[key]
                for val in df[key]:
                    # sources entry
                    sk[val] = True

            for _, row in df.iterrows():
                for keyA in df.columns:
                    vA = row[keyA]
                    for keyB in df.columns:
                        if keyB != keyA:
                            vB = row[keyB]
                            if vA and vB:
                                # links entry
                                links[keyA + '::' + keyB][vA].add(vB)

                                # values entry
                                CA, attrA = concept_property_split[keyA]
                                CB, attrB = concept_property_split[keyB]
                                if attrA == UNIQUE_ID_ATTR:
                                    CA = str_to_CONCEPT[CA]
                                    CB = str_to_CONCEPT[CB]
                                    if CA == CB:
                                        values[keyB][vB].add(vA)

        return {'sources': sources, 'links': links, 'values': values}
