import pytest

from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage


@pytest.fixture(scope='function')
def valid_stage_cls():
    class ValidIngestStage(IngestStage):

        def _run(self, foo):
            pass

        def _validate_run_parameters(self, foo):
            if not foo:
                raise InvalidIngestStageParameters

        def write_output(self, output):
            pass

        def read_output(self, filepath):
            pass

    return ValidIngestStage


def test_ingest_stage_abs_cls():
    # Declare a concrete ingest stage class
    class InvalidIngestStage(IngestStage):
        pass

    # Test that TypeError is raised if all abstract classes are not impl
    with pytest.raises(TypeError) as e:
        InvalidIngestStage()

    abstract_methods = ['read_output',
                        '_run',
                        'write_output',
                        '_validate_run_parameters']

    for m in abstract_methods:
        assert m in str(e)


def test_invalid_run_parameters(valid_stage_cls):
    # Test that InvalidIngestStageParameters excp raised on invalid run params
    stage = valid_stage_cls()
    with pytest.raises(InvalidIngestStageParameters):
        stage.run(False)

    # No exception should be raised on valid params
    stage.run(True)


def test_missing_run_parameters():
    pass


def test_write_output():
    pass


def test_read_output():
    pass
