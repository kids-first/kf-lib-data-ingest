"""
We only need to test validate run parameters since the actual transformation
functionality is heavily tested in tests/test_standard_model.py
"""
import os

import pytest
import pandas as pd

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)
from conftest import TRANSFORM_MODULE_PATH


@pytest.fixture(scope='function')
def transform_module():
    """
    Reusable fixture for transform tests
    """
    return TransformModule(TRANSFORM_MODULE_PATH)


def test_invalid_run_parameters(transform_stage):

    # Bad keys
    with pytest.raises(InvalidIngestStageParameters):
        transform_stage.run({i: 'foo' for i in range(5)})

    # Bad values
    with pytest.raises(InvalidIngestStageParameters):
        transform_stage.run({'foor': ('bar', None) for i in range(5)})


def test_transform_module(transform_module):
    # Valid transform module
    assert transform_module

    # Test that transform_function must be a func
    setattr(transform_module.contents, 'transform_function', 'hello')

    with pytest.raises(TypeError):
        transform_module._validate()

    # Test that transform_function must be a func
    delattr(transform_module.contents, 'transform_function')

    with pytest.raises(ConfigValidationError):
        transform_module._validate()
