"""
Module for transforming source data DataFrames to the standard model.
"""
import os
from pprint import pformat

import pandas

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)
from kf_lib_data_ingest.etl.transform.standard_model.concept_schema import (
    concept_from,
    concept_attr_from,
    unique_key_composition as DEFAULT_KEY_COMP,
    UNIQUE_ID_ATTR,
    DELIMITER
)
from kf_lib_data_ingest.etl.transform.auto import AutoTransformer
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformer


class TransformStage(IngestStage):
    def __init__(self, target_api_config_path, ingest_output_dir=None,
                 transform_function_path=None):

        super().__init__(ingest_output_dir=ingest_output_dir)

        self.target_api_config = TargetAPIConfig(target_api_config_path)

        if not transform_function_path:
            self.transformer = AutoTransformer(self.target_api_config)
        else:
            self.transformer = GuidedTransformer(self.target_api_config,
                                                 transform_function_path)

    def _read_output(self):
        """
        Read previously written transform stage output

        :returns: dict of pandas.DataFrames keyed by strings
        representing target concepts (i.e. participant, biospecimen, etc)
        """
        output = {
            os.path.splitext(filename)[0]: pandas.read_csv(
                os.path.join(self.stage_cache_dir, filename),
                delimiter='\t')
            for filename in os.listdir(self.stage_cache_dir)
            if filename.endswith('.tsv')
        }
        self.logger.info(f'Reading {type(self).__name__} output:\n'
                         f'{pformat(list(output.keys()))}')

        return output

    def _write_output(self, output):
        """
        Write output of transform stage to file

        :param output: output created by TransformStage._run
        :type output: a dict of pandas.DataFrames
        """
        assert_safe_type(output, dict)
        assert_all_safe_type(output.values(), pandas.DataFrame)
        paths = []
        for key, df in output.items():
            fp = os.path.join(self.stage_cache_dir, key + '.tsv')
            paths.append(fp)
            df.to_csv(fp, sep='\t', index=False)

        self.logger.info(f'Writing {type(self).__name__} output:\n'
                         f'{pformat(paths)}')

    def _validate_run_parameters(self, data_dict):
        """
        Validate the parameters being passed into the _run method. This
        method gets executed before the body of _run is executed.

        A key in df_dict should be a string containing the URL to the
        extract config module used to produce the Pandas DataFrame in the
        value tuple.

        A value in df_dict should be a tuple where the first member is a
        string containing the URL to the source data file, and the second
        member of the tuple is a Pandas DataFrame containing the mapped
        source data.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined above.
        """
        try:
            # Check types
            assert_safe_type(data_dict, dict)
            assert_all_safe_type(data_dict.keys(), str)

            # Check that values are tuples of (string, DataFrames)
            for extract_config_url, df in data_dict.values():
                assert_safe_type(extract_config_url, str)
                assert_safe_type(df, pandas.DataFrame)

        except TypeError as e:
            raise InvalidIngestStageParameters from e

    def _insert_unique_keys(self, df_dict):
        """
        Iterate over mapped dataframes and insert the unique key columns

        :param df_dict: a dict of Pandas DataFrames. A key is an extract config
        URL and a value is a tuple of (source file url, DataFrame).
        """
        for extract_config_url, (source_file_url, df) in df_dict.items():
            # Insert unique key columns
            df = self._add_unique_key_cols(df)

            # If no unique key columns are present raise an error.
            # This means that the dataframe does not have anything to uniquely
            # identify concepts in the data. In the case of auto transform
            # this means no ConceptNodes can be created and inserted into the
            # ConceptGraph.
            is_any_unique_keys = any([concept_attr_from(col) == UNIQUE_ID_ATTR
                                      for col in df.columns])
            if not is_any_unique_keys:
                raise ValueError(
                    'Error inserting dataframe into ConceptGraph! There must '
                    'be at least 1 unique key column in the DataFrame. Source '
                    f'of error is {extract_config_url} : {source_file_url}'
                )

    def _add_unique_key_cols(self, df,
                             unique_key_composition=DEFAULT_KEY_COMP):
        """
        Construct and insert unique key columns for each concept present in the
        mapped df. Only do this if there isn't already an existing unique key
        column for a particular concept.

        The unique key is a special standard concept attribute which is
        reserved to uniquely identify concept instances of the same type.

        If the unique key column hasn't been explicitly provided in the mapped
        data, then this method will insert a unique key column using the values
        from other columns in the data. The columns it uses are defined in
        etl.transform.standard_model.concept_schema.unique_key_compositionself.

        A unique key column for a concept will only be added if all of the
        columns required to compose the unique key for that concept exist in
        the DataFrame.

        The value of a unique key will be a delimited string containing the
        values from required unique key columns.

        For example, given the mapped dataframe:

            CONCEPT.OUTCOME.ID | CONCEPT.OUTCOME.VITAL_STATUS
            -------------------------------------------------
                OT1                 Deceased

        the unique_key_composition:
                {
                    CONCEPT.OUTCOME: {CONCEPT.OUTCOME.ID,
                                      CONCEPT.OUTCOME.VITAL_STATUS}
                }

        and the unique key delimiter: -

        the output dataframe would be:

            CONCEPT.OUTCOME.ID | CONCEPT.OUTCOME.VITAL_STATUS | CONCEPT.OUTCOME.UNIQUE_KEY # noqa E501
            ------------------------------------------------------------------------------ # noqa E501
                OT1                 Deceased                        OT1-Deceased # noqa E501

        :param df: the Pandas DataFrame that will be modified
        :param unique_key_composition: a dict where a key is a standard concept
        string and a value is a list of required columns needed to compose
        a unique key for the concept.
        """
        # Iterate over all concepts and try to insert a unique key column
        # for each concept
        for concept_name in unique_key_composition.keys():
            # Determine the cols needed to compose a unique key for the concept
            output_key_cols = []
            try:
                self._unique_key_cols(concept_name,
                                      df, unique_key_composition,
                                      output_key_cols)
            except AssertionError as e:
                # Unique key composition not defined for a concept, dev error
                if 'key composition not defined' in str(e):
                    raise e
                # One of the required cols to make the unique key did not exist
                # Cannot add a unique key col for this concept, move on
                # self.logger.debug(str(e))
            else:
                # Insert unique key column for the concept
                unique_key_col = f'{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}'
                df[unique_key_col] = df.apply(
                    lambda row: DELIMITER.join([str(row[c])
                                                for c in output_key_cols]),
                    axis=1)

        return df

    def _unique_key_cols(self, concept_name, df, unique_key_composition,
                         output_key_cols):
        """
        Compose the list of column names that are needed to build a unique key
        for a particular concept.

        The required columns for a concept's unique key are defined in
        etl.transform.standard_model.concept_schema.unique_key_composition.

        A concept's unique key can be composed of other concept's unique keys.
        This is a recursive method that collects the required columns needed to
        build a unique key column for a concept. If one of the required columns
        is a unique key itself, then the method will recurse in order to get
        the columns that make up that unique key.

        :param concept_name: a string and the name of the concept for which a
        unique key will be made
        :param df: a Pandas DataFrame
        :param output_key_cols: the output list of columns needed to build the
        unique key column for a concept.
        """
        # If unique key col for this concept already exists return that
        output_key_col_name = f'{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}'
        if output_key_col_name in df.columns:
            output_key_cols.append(output_key_col_name)
            return

        # Get the required cols needed to make a unique key for this concept
        required_cols = unique_key_composition.get(concept_name)
        # If required cols don't exist for a concept, then we have made a dev
        # error in concept_schema.py
        assert required_cols, ('Unique key composition not defined in concept '
                               f'schema for concept {concept_name}!')

        # Add required cols to cols needed for the unique key
        cols = set(df.columns)
        for req_col in required_cols:
            if concept_attr_from(req_col) == UNIQUE_ID_ATTR:
                # The required col is a unique key itself, so recurse
                self._unique_key_cols(concept_from(req_col),
                                      df, unique_key_composition,
                                      output_key_cols)
            else:
                # If all of the required cols are not present then we cannot
                # make the unique key
                assert req_col in cols, ('Did not create unique key for '
                                         f'{concept_name}. Missing 1 or more '
                                         'required columns')

                output_key_cols.append(req_col)

    def _run(self, data_dict):
        """
        Transform the tabular mapped data into a dict of lists.
        Keys are target entity types and values are lists of target entity
        dicts.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined in _validate_run_parameters.
        """

        # Insert unique key columns before running transformation
        self._insert_unique_keys(data_dict)

        target_entities = self.transformer.run(data_dict)

        return target_entities
