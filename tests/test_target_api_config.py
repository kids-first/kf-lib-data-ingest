import os

import pytest

from conftest import TEST_ROOT_DIR
from etl.configuration.target_api_config import TargetAPIConfig

KIDS_FIRST_CONFIG = os.path.join(os.path.dirname(TEST_ROOT_DIR), 'src',
                                 'target_apis', 'kids_first.py')

# TODO - Come back to this later after validation code has been finalized.


def test_missing_req_attrs(tmpdir, config_file_content):
    # p = tmpdir.mkdir("configs").join("test_config.py")
    # p.write("content")
    pass


def test_wrong_attr_type():
    pass


def test_target_concepts():
    pass


def test_relationships():
    pass


def test_endpoints():
    pass


def test_kids_first_config():
    assert TargetAPIConfig(KIDS_FIRST_CONFIG)


def _make_config_file_content(target_concepts_dict=None,
                              relationships_dict=None,
                              endpoints_dict=None):
    pass
