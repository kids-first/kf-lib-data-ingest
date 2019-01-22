import pytest
import os

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.stage import IngestStage

from conftest import TEST_INGEST_OUTPUT_DIR


@pytest.fixture(scope='function')
def ValidIngestStage():
    class ValidIngestStage(IngestStage):
        def __init__(self, ingest_output_dir=None):
            super().__init__(ingest_output_dir)

        def _run(self, foo):
            pass

        def _validate_run_parameters(self, foo):
            if not foo:
                raise InvalidIngestStageParameters

        def _write_output(self, output):
            pass

        def _read_output(self):
            pass

    return ValidIngestStage


def test_ingest_stage_abs_cls():
    # Declare a concrete ingest stage class
    class InvalidIngestStage(IngestStage):
        pass

    # Test that TypeError is raised if all abstract classes are not impl
    with pytest.raises(TypeError) as e:
        InvalidIngestStage()

    abstract_methods = ['_run',
                        '_write_output',
                        '_read_output',
                        '_validate_run_parameters']

    for m in abstract_methods:
        assert m in str(e)


def test_invalid_run_parameters(ValidIngestStage):
    stage = ValidIngestStage()

    # Test that InvalidIngestStageParameters excp raised on invalid run params
    with pytest.raises(InvalidIngestStageParameters):
        stage.run(False)

    # No exception should be raised on valid params
    stage.run(True)


def test_stage_dir_creation(ValidIngestStage):
    """
    Test that a stage's output dir gets created properly
    """
    stage = ValidIngestStage(TEST_INGEST_OUTPUT_DIR)

    assert os.path.isdir(stage.ingest_output_dir)
    assert os.path.isdir(stage.stage_cache_dir)
    assert os.path.basename(stage.stage_cache_dir) == type(stage).__name__


def test_missing_run_parameters():
    pass


def test_serialize_output():
    pass


def test_deserialize_output():
    pass
