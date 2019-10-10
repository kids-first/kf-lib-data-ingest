from kf_lib_data_ingest.etl.configuration.base_config import PyModuleConfig
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type,
    function,
)


class ExtractConfig(PyModuleConfig):
    def __init__(self, filepath):
        super().__init__(filepath)

    def _validate(self):
        assert_safe_type(self.source_data_url, str)
        assert_safe_type(self.source_data_read_func, None, function)
        assert_safe_type(self.source_data_read_params, None, dict)
        assert_safe_type(self.do_after_read, None, function)

        def validate_operations(operations):
            assert_safe_type(operations, list)
            assert_all_safe_type(operations, function, list)
            for op in operations:
                if isinstance(op, list):
                    validate_operations(op)

        validate_operations(self.operations)
