import os
import pytest
from jsonschema.exceptions import ValidationError

from conftest import TEST_DATA_DIR
from etl.configuration.base_config import (
    AbstractConfig,
    YamlConfig
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

    with pytest.raises(ValidationError):
        YamlConfig(config_path, schema_path=schema_path)

    config_path = os.path.join(TEST_DATA_DIR, 'valid_yaml_config.yml')
    YamlConfig(config_path, schema_path=schema_path)


def test_extract_config():
    pass


def test_dataservice_schema():
    pass


def test_standard_model_schema():
    pass
