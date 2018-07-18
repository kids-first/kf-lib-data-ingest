import pytest

from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage


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
                        '_validate_run_parameters']

    for m in abstract_methods:
        assert m in str(e)


def test_invalid_run_parameters():
    # Test that InvalidIngestStageParameters excp raised on invalid run params
    class ValidIngestStage(IngestStage):
        def _run(self, foo):
            pass

        def _validate_run_parameters(self, foo):
            if not foo:
                raise InvalidIngestStageParameters

        def _serialize_output(self, output):
            pass

        def _deserialize_output(self, filepath):
            pass

    stage = ValidIngestStage()
    with pytest.raises(InvalidIngestStageParameters) as e:
        stage.run(False)

    # No exception should be raised on valid params
    stage.run(True)


def test_missing_run_parameters():
    pass


def test_serialize_output():
    pass


def test_deserialize_output():
    pass
