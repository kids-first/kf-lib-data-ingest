"""
Module for transforming source data DataFrames to the standard model.
"""
from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage


class Transformer(IngestStage):
    def __init__(self, dataset_ingest_config_filepath):
        super().__init__(dataset_ingest_config_filepath)

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

    def _run(self, *args, **kwargs):
        # convert the input dataframes into unified form
        pass  # TODO
