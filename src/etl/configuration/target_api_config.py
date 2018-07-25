
from etl.configuration.base_config import PyModuleConfig


class TargetAPIConfig(PyModuleConfig):

    def __init__(self, filepath):
        super().__init__(filepath)
        self._deserialize()

    def _validate(self):
        # TODO - Check that required module attributes are present
        pass

    def _deserialize(self):
        # TODO
        # Add other attributes later after config design is complete

        self.target_model_schema = self.contents.target_model_schema
