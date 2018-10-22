"""
Module for transforming source data DataFrames to the standard model.
"""
from pandas import DataFrame

from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage
from common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from etl.transform.standard_model.model import StandardModel


class TransformStage(IngestStage):
    def __init__(self):
        super().__init__()
        # TODO we dont know what this takes yet
        pass

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
        Transform the tabular mapped data into a unified standard form

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined in _validate_run_parameters.
        """

        model = StandardModel(logger=self.logger)
        model.populate(data_dict)

        return model
