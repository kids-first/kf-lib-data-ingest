import pytest

from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage


@pytest.fixture(scope='function')
def valid_stage_cls():
    class ValidIngestStage(IngestStage):

        def _operation(self):
            return 'operation'

        def _run(self, foo):
            pass

        def _validate_run_parameters(self, foo):
            if not foo:
                raise InvalidIngestStageParameters

        def _serialize_output(self, output):
            pass

        def _deserialize_output(self, filepath):
            pass

    return ValidIngestStage


def test_ingest_stage_abs_cls():
    # Declare a concrete ingest stage class
    class InvalidIngestStage(IngestStage):
        pass

    # Test that TypeError is raised if all abstract classes are not impl
    with pytest.raises(TypeError) as e:
        InvalidIngestStage()

    abstract_methods = ['_deserialize_output',
                        '_run',
                        '_serialize_output',
                        '_validate_run_parameters',
                        '_operation']

    for m in abstract_methods:
        assert m in str(e)


def test_invalid_run_parameters(valid_stage_cls):
    # Test that InvalidIngestStageParameters excp raised on invalid run params
    stage = valid_stage_cls()
    with pytest.raises(InvalidIngestStageParameters):
        stage.run(False)

    # No exception should be raised on valid params
    stage.run(True)


def test_invalid_operation(valid_stage_cls):
    """
    Test TypeError raised when IngestStage.operation is not a str
    """
    class StageInvalidOperation(valid_stage_cls):
        def _operation(self):
            return None

    with pytest.raises(TypeError):
        StageInvalidOperation()


def test_missing_run_parameters():
    pass


def test_serialize_output():
    pass


def test_deserialize_output():
    pass
