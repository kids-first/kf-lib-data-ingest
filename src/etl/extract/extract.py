import pandas as pd

from common.stage import IngestStage
from common.file_retriever import FileRetriever
from etl.configuration.extract_config import ExtractConfig

from config import EXTRACT_OP


class ExtractStage(IngestStage):

    operation = EXTRACT_OP

    def __init__(self, extract_config_paths):
        super().__init__()
        self.extract_configs = [ExtractConfig(config_filepath)
                                for config_filepath
                                in extract_config_paths]

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
