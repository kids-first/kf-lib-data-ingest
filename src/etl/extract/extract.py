import pandas as pd

from common.stage import IngestStage
from common.file_retriever import FileRetriever
from etl.configuration.extract_config import ExtractConfig


class ExtractStage(IngestStage):

    def __init__(self, extract_config_paths):
        super().__init__()
        self.extract_configs = [ExtractConfig(config_filepath)
                                for config_filepath
                                in extract_config_paths]

    def _serialize_output(self, output):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _deserialize_output(self, filepath):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _validate_run_parameters(self):
        # Extract stage does not expect any args so we can pass validation
        pass

    def _run(self):
        # TODO
        output = {}
        for extract_config in self.extract_configs:
            # use file retriever to do fetch
            # read file
            # Do operations to produce table from subdata columns

            # TODO - Replace None with actual dataframes
            output[extract_config.config_filepath] = pd.DataFrame()

        # return dictionary of all dataframes keyed by extract config paths
        return output

    def _read_file(self, file_path, read_method=None, **kwargs):
        # TODO returns (partial?)contents of the file as a dataframe
        pass
