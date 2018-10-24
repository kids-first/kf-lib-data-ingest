"""
Module for transforming source data DataFrames to the standard model.
"""
from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage
from config import TRANSFORM_OP


class TransformStage(IngestStage):

    operation = TRANSFORM_OP

    def __init__(self):
        super().__init__()
        # TODO we dont know what this takes yet
        pass

    def write_output(self, output, output_dir, overwrite):
        # TODO
        # Write output files. If overwrite=True then write out files without
        # timestamp appended to name. If overwrite=False, write out files
        # with timestamp appended to name
        pass

    @classmethod
    def read_output(cls, output_dir):
        # TODO
        # Read output files and construct objects that would have been output
        # by the _run method
        pass

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
