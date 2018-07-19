
from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage
from etl.configuration.extract_config import ExtractConfig
from etl.common.file_retriever import FileRetriever

class Extractor(IngestStage):

    def __init__(self, dataset_ingest_config_filepath):
        super().__init__(dataset_ingest_config_filepath)
        self.extract_configs = [
            ExtractConfig(config_filepath)
            for config_filepath
            in self.dataset_ingest_config.extract_config_paths]

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
        # TODO
        for extract_config in self.extract_configs:
            # use file retriever to do fetch
            # read file
            # Do operations to produce table from subdata columns
        # return dictionary of all dataframes keyed by extract config paths
        pass

    def _read_file(self, file_path, read_method=None, **kwargs):
        # TODO returns (partial?)contents of the file as a dataframe
        pass
