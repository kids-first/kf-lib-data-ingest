import os

import pytest

from conftest import TEST_DATA_DIR
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig,
)


@pytest.mark.parametrize("bad_api", ["bad_v2api.py", "bad_v1api.py"])
def test_all_targets(bad_api):
    # all_targets needs to exist as a list populated by valid classes
    tac = TargetAPIConfig(os.path.join(TEST_DATA_DIR, bad_api))

    tac.all_targets = [tac.Good]
    tac._validate()

    tac.all_targets[0] = tac.D
    with pytest.raises(ConfigValidationError):
        tac._validate()

    tac.all_targets = 5
    with pytest.raises(ConfigValidationError):
        tac._validate()


@pytest.mark.parametrize("bad_api", ["bad_v2api.py", "bad_v1api.py"])
def test_class_validation(bad_api):
    # target classes must have the correct form
    tac = TargetAPIConfig(os.path.join(TEST_DATA_DIR, bad_api))

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

    # class methods need the right arguments
    tac.all_targets = [tac.AlmostGood]
    with pytest.raises(ConfigValidationError) as err6:
        tac._validate()

    tac.all_targets = [tac.AlmostAlmostGood]
    with pytest.raises(ConfigValidationError) as err7:
        tac._validate()

    assert str(err6.value) != str(err7.value)


@pytest.mark.parametrize("bad_api", ["bad_v2api.py", "bad_v1api.py"])
def test_validate_function(bad_api):
    # if a validate function exists, make sure it runs
    tac = TargetAPIConfig(os.path.join(TEST_DATA_DIR, bad_api))

    tac._validate()

    def v():
        assert False

    tac.validate = v
    with pytest.raises(AssertionError):
        tac._validate()
