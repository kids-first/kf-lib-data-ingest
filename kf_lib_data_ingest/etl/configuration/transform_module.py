import os
from kf_lib_data_ingest.etl.configuration.base_config import (
    PyModuleConfig,
    ConfigValidationError
)
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    function
)


class TransformModule(PyModuleConfig):

    def __init__(self, filepath):
        # Since the path to the transform function py file is an optional key
        # in dataset_ingest_config.yaml, it could be None so we should check
        # for that.
        if filepath is None:
            raise ConfigValidationError(
                'Guided transformation requires a '
                'a valid path to the user defined transform function .py file '
                'Path must be specified in the dataset_ingest_config.yaml')

        super().__init__(filepath)

        self.transform_function = self.contents.transform_function

    def _validate(self):
        """
        Validate that TransformModule has the required `transform_function`
        method
        """
        if not hasattr(self.contents, 'transform_function'):
            raise ConfigValidationError(f'{self.config_filepath} missing '
                                        'required method: transform_function')
        assert_safe_type(self.contents.transform_function, function)
