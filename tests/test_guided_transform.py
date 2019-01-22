import pytest
import pandas as pd

from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformer

from conftest import TRANSFORM_MODULE_PATH


@pytest.fixture(scope='function')
def transform_module():
    """
    Reusable fixture for transform tests
    """
    return TransformModule(TRANSFORM_MODULE_PATH)


def test_transform_module(transform_module):
    """
    Test validation of user supplied transform module
    """
    # Valid transform module
    assert transform_module

    # Test that transform_function must be a func
    setattr(transform_module.contents, 'transform_function', 'hello')

    with pytest.raises(TypeError):
        transform_module._validate()

    # Test that transform_function exist in the module
    delattr(transform_module.contents, 'transform_function')

    with pytest.raises(ConfigValidationError):
        transform_module._validate()


def test_no_transform_module(target_api_config):
    """
    Test that when the filepath to the transform function py file is not
    specified, a ConfigValidationError is raised
    """
    with pytest.raises(ConfigValidationError) as e:
        GuidedTransformer(target_api_config, None)
        assert 'Guided transformation requires a' in str(e)


@pytest.mark.parametrize('ret_val, error',
                         [(None, TypeError),
                          ({'foo': pd.DataFrame()}, KeyError),
                          ({'participant': None}, TypeError)
                          ])
def test_bad_ret_vals_transform_funct(transform_stage, ret_val, error):
    """
    Test wrong return values from transform function
    """
    tf = transform_stage.transformer

    def f(df_dict):
        return ret_val

    tf.transform_module.transform_function = f
    with pytest.raises(error):
        tf._apply_transform_funct({})
