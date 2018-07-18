"""
Module for transforming source data DataFrames to the standard model.
"""
from src.etl.common.errors import InvalidIngestStageParameters
from src.etl.common.stage import IngestStage


class Transformer(IngestStage):
    def __init__(self, study_config_filepath):
        super().__init__(study_config_filepath)

    def _validate_run_parameters(*args, **kwargs):
        # Should raise a InvalidIngestStageParameters if any
        # parameters are missing.
        # TODO
        raise InvalidIngestStageParameters

    def _serialize_output(self, output):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _deserialize_output(self, filepath):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO


class ConceptGraphTransformer(Transformer):
    def _run(self, *args, **kwargs):
        # convert the input dataframes into unified form
        pass  # TODO
