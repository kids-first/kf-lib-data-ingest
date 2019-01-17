"""
Module for transforming source data DataFrames to the standard model.
"""
from pandas import DataFrame

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)
from kf_lib_data_ingest.etl.transform.auto import AutoTransformer
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformer


class TransformStage(IngestStage):
    def __init__(self, target_api_config_path, auto_transform,
                 transform_function_path=None):
        super().__init__()
        self.target_api_config = TargetAPIConfig(target_api_config_path)

        if auto_transform:
            self.transformer = AutoTransformer(self.target_api_config)
        else:
            self.transformer = GuidedTransformer(self.target_api_config,
                                                 transform_function_path)

    def _read_output(self):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _write_output(self, output):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

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
                assert_safe_type(df, DataFrame)

        except TypeError as e:
            raise InvalidIngestStageParameters from e

    def _run(self, data_dict):
        """
        Transform the tabular mapped data into a dict of lists.
        Keys are target entity types and values are lists of target entity
        dicts.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined in _validate_run_parameters.
        """
        target_entities = self.transformer._run(data_dict)

        return target_entities
