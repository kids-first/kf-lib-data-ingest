"""
Module for transforming source data DataFrames to the standard model.
"""
from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage


class TransformStage(IngestStage):
    def __init__(self):
        super().__init__()
        # TODO we dont know what this takes yet
        pass

    def _serialize_output(self, output):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _deserialize_output(self, filepath):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _validate_run_parameters(self, df_dict):
        # Should raise a InvalidIngestStageParameters if any
        # parameters are missing.
        # TODO
        # Check that df_dict contains non-empty dataframes keyed by the
        # extract config filepath
        # Each dataframe should have columns mapped to the standard model
        # concepts and properties
        pass

    def _run(self, df_dict):
        # TODO
        standard_model = None
        # convert the input dataframes into unified form
        for key, df in df_dict.items():
            # do stuff
            pass
        return standard_model
