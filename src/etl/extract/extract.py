
from src.etl.common.errors import InvalidIngestStageParameters
from src.etl.configuration.extract_config import ExtractConfig
from src.etl.common.stage import IngestStage


class Extracter(IngestStage):

    def __init__(self, study_config_filepath):
        super().__init__(study_config_filepath)
        self.extract_configs = [ExtractConfig(config_filepath)
                                for config_filepath
                                in self.study_config.extract_config_paths]

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

    def _run(self):
        # Do fetch
        # Read file(s)
        # Pull raw subdata from file
        # Do operations to produce table from subdata columns
        pass  # TODO
