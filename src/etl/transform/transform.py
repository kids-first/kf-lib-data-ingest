"""
Module for transforming source data DataFrames to the standard model.
"""
from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage
from config import TRANSFORM_OP


class TransformStage(IngestStage):
    def __init__(self):
        super().__init__()
        # TODO we dont know what this takes yet
        pass

    def write_output(self, output):
        # An ingest stage is responsible for writing out the data that it
        # produces via its run method
        pass  # TODO

    def read_output(self, filepath):
        # An ingest stage is responsible for reading the data it wrote out from
        # its run method
        pass  # TODO

    def _operation(self):
        return TRANSFORM_OP

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
