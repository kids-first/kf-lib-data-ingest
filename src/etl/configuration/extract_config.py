from etl.configuration.base_config import PyModuleConfig
from common.type_safety import assert_safe_type, assert_all_safe_type, function


class ExtractConfig(PyModuleConfig):

    def __init__(self, filepath):
        super().__init__(filepath)
        self.source_data_url = self.contents.source_data_url
        self.loading_params = self.contents.source_data_loading_parameters
        self.operations = self.contents.operations

    def _validate(self):
        assert_safe_type(self.contents.source_data_url, str)
        assert_safe_type(self.contents.source_data_loading_parameters, dict)
        assert_safe_type(self.contents.operations, list)
        assert_all_safe_type(self.contents.operations, function, list)
        for op in self.contents.operations:
            if isinstance(op, list):
                assert_all_safe_type(op, function)
