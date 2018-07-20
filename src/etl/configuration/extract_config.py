
from etl.configuration.base_config import PyModuleConfig


class ExtractConfig(PyModuleConfig):

    def __init__(self, filepath):
        super().__init__(filepath)

    def _deserialize(self):
        # TODO
        # Add other attributes later after config design is complete
        self.source_data_filepath = self.contents.source_data_filepath
