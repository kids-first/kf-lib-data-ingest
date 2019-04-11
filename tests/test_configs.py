import os

import pytest

from conftest import TEST_DATA_DIR
from kf_lib_data_ingest.etl.configuration.base_config import (
    AbstractConfig,
    ConfigValidationError,
    YamlConfig,
    PyModuleConfig
)


def test_config_abs_cls():
    # Declare a concrete ingest stage class
    class InvalidConfig(AbstractConfig):
        pass

    # Test that TypeError is raised if all abstract classes are not impl
    with pytest.raises(TypeError) as e:
        InvalidConfig()

    abstract_methods = ['_read_file', '_validate']

    for m in abstract_methods:
        assert m in str(e)

    class Config(AbstractConfig):
        def _read_file(self, filepath):
            pass

        def _validate(self):
            pass

    # Test that FileNotFoundError raised on file not exists
    with pytest.raises(FileNotFoundError):
        Config('foo')

    Config(os.path.join(TEST_DATA_DIR, 'valid_yaml_config.yml'))


def test_yaml_config():
    schema_path = os.path.join(TEST_DATA_DIR, 'yaml_schema.yml')
    config_path = os.path.join(TEST_DATA_DIR, 'invalid_yaml_config.yml')

    with pytest.raises(ConfigValidationError):
        try:
            YamlConfig(config_path, schema_path=schema_path)
        except ConfigValidationError as e:
            assert config_path in str(e)
            raise

    config_path = os.path.join(TEST_DATA_DIR, 'valid_yaml_config.yml')
    YamlConfig(config_path, schema_path=schema_path)


def test_attr_forwarding():
    pmc = PyModuleConfig(
        os.path.join(TEST_DATA_DIR, 'test_study', 'transform_module.py')
    )
    assert pmc.contents.transform_function == pmc.transform_function
    with pytest.raises(AttributeError):
        print(pmc.foo)


def test_extract_config():
    pass


def test_dataservice_schema():
    pass


def test_standard_model_schema():
    pass
