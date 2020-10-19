import os

import pytest

from conftest import TEST_DATA_DIR
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig,
)


def test_all_targets():
    # all_targets needs to exist as a list populated by valid classes
    tac = TargetAPIConfig(os.path.join(TEST_DATA_DIR, "bad_api.py"))

    tac.all_targets = [tac.Good]
    tac._validate()

    tac.all_targets[0] = tac.D
    with pytest.raises(ConfigValidationError):
        tac._validate()

    tac.all_targets = 5
    with pytest.raises(ConfigValidationError):
        tac._validate()


def test_class_validation():
    # target classes must have the correct form
    tac = TargetAPIConfig(os.path.join(TEST_DATA_DIR, "bad_api.py"))

    tac.all_targets = [tac.F]
    with pytest.raises(ConfigValidationError) as errF:
        tac._validate()

    tac.all_targets = [tac.E]
    with pytest.raises(ConfigValidationError) as errE:
        tac._validate()

    tac.all_targets = [tac.D]
    with pytest.raises(ConfigValidationError) as errD:
        tac._validate()

    tac.all_targets = [tac.C]
    with pytest.raises(ConfigValidationError) as errC:
        tac._validate()

    tac.all_targets = [tac.B]
    with pytest.raises(ConfigValidationError) as errB:
        tac._validate()

    tac.all_targets = [tac.A]
    with pytest.raises(ConfigValidationError) as errA:
        tac._validate()

    tac.all_targets = [5]
    with pytest.raises(ConfigValidationError) as err5:
        tac._validate()

    tac.all_targets = [tac.Good]
    tac._validate()

    assert 7 == len(
        {str(e.value) for e in [errF, errE, errD, errC, errB, errA, err5]}
    )

    t0 = tac.all_targets[0]

    # class methods need the right arguments

    tmp = t0.get_key_components
    t0.get_key_components = lambda x: x
    with pytest.raises(ConfigValidationError) as err6:
        tac._validate()

    t0.get_key_components = tmp
    tac._validate()

    tmp = t0.build_entity
    t0.build_entity = lambda a, b, c: a
    with pytest.raises(ConfigValidationError) as err7:
        tac._validate()

    t0.build_entity = tmp
    tac._validate()

    assert str(err6.value) != str(err7.value)


def test_validate_function():
    # if a validate function exists, make sure it runs
    tac = TargetAPIConfig(os.path.join(TEST_DATA_DIR, "bad_api.py"))

    tac._validate()

    def v():
        assert False

    tac.validate = v
    with pytest.raises(AssertionError):
        tac._validate()
