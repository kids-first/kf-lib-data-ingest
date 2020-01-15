import os

import pytest

from conftest import TEST_DATA_DIR
from kf_lib_data_ingest.etl.configuration.base_config import (
    AbstractConfig,
    ConfigValidationError,
    PyModuleConfig,
    YamlConfig,
)
from kf_lib_data_ingest.etl.configuration.ingest_package_config import (
    IngestPackageConfig,
)


def test_config_abs_cls():
    # Declare a concrete ingest stage class
    class InvalidConfig(AbstractConfig):
        pass

    # Test that TypeError is raised if all abstract classes are not impl
    with pytest.raises(TypeError) as e:
        InvalidConfig()
    for m in ["_read_file", "_validate"]:
        assert m in str(e.value)

    class Config(AbstractConfig):
        def _read_file(self, filepath):
            pass

        def _validate(self):
            pass

    # Test that FileNotFoundError raised on file not exists
    with pytest.raises(FileNotFoundError):
        Config("foo")

    Config(os.path.join(TEST_DATA_DIR, "valid_yaml_config.yml"))


def test_yaml_config():
    schema_path = os.path.join(TEST_DATA_DIR, "yaml_schema.yml")
    config_path = os.path.join(TEST_DATA_DIR, "invalid_yaml_config.yml")

    with pytest.raises(ConfigValidationError) as e:
        YamlConfig(config_path, schema_path=schema_path)
    assert config_path in str(e.value)

    config_path = os.path.join(TEST_DATA_DIR, "valid_yaml_config.yml")
    YamlConfig(config_path, schema_path=schema_path)


def test_attr_forwarding():
    pmc = PyModuleConfig(
        os.path.join(TEST_DATA_DIR, "test_study", "transform_module.py")
    )
    assert pmc.contents.transform_function == pmc.transform_function
    assert pmc.foo is None

    config_path = os.path.join(TEST_DATA_DIR, "valid_yaml_config.yml")
    schema_path = os.path.join(TEST_DATA_DIR, "yaml_schema.yml")
    yc = YamlConfig(config_path, schema_path=schema_path)
    assert yc.contents.get("params") == yc.params
    assert yc.foo is None


def test_ingest_package_config(tmpdir):
    bipcf_path = os.path.join(tmpdir, "bad_ingest_package_config.py")

    with open(bipcf_path, "w") as bipcf:
        bipcf.write("HI, LOL!")
    with pytest.raises(ConfigValidationError):
        IngestPackageConfig(bipcf_path)  # not valid python (syntax)

    with open(bipcf_path, "w") as bipcf:
        bipcf.write("foo = 'HI, LOL!'")
    with pytest.raises(ConfigValidationError):
        IngestPackageConfig(bipcf_path)  # missing required members

    confdir = os.path.join(tmpdir, "extract_configs")
    os.mkdir(confdir)
    with open(bipcf_path, "w") as bipcf:
        bipcf.write(
            "\n".join(
                [
                    f'extract_config_dir = "{confdir}"',
                    'study = "SD_12345678"',
                    "target_service_entities = []",
                ]
            )
        )
    IngestPackageConfig(bipcf_path)


def test_extract_config():
    pass


def test_dataservice_schema():
    pass


def test_standard_model_schema():
    pass
