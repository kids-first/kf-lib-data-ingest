
from etl.configuration.base_config import PyModuleConfig


class ExtractConfig(PyModuleConfig):

    def __init__(self, filepath):
        super().__init__(filepath)
        self._deserialize()

    def _validate(self):
        # TODO - Check that required module attributes are present
        pass

    def _deserialize(self):
        # TODO
        # Add other attributes later after config design is complete
        self.source_data_url = self.contents.source_data_url
