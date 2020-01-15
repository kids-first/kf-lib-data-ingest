import os

import pytest

from conftest import TEST_INGEST_OUTPUT_DIR
from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.stage import IngestStage


@pytest.fixture(scope="function")
def ValidIngestStage():
    class ValidIngestStage(IngestStage):
        def __init__(self, ingest_output_dir=None):
            super().__init__(ingest_output_dir=ingest_output_dir)

        def _run(self, foo):
            return foo

        def _postrun_concept_discovery(self, run_output):
            pass

        def _validate_run_parameters(self, foo):
            if not foo:
                raise InvalidIngestStageParameters

        def _write_output(self, output):
            fp = os.path.join(self.stage_cache_dir, "test.txt")
            with open(fp, "w") as text_file:
                text_file.write(output)

        def _read_output(self):
            fp = os.path.join(self.stage_cache_dir, "test.txt")
            with open(fp, "r") as text_file:
                return text_file.read()

    return ValidIngestStage


def test_ingest_stage_abs_cls():
    # Declare a concrete ingest stage class
    class InvalidIngestStage(IngestStage):
        pass

    # Test that TypeError is raised if all abstract classes are not impl
    with pytest.raises(TypeError) as e:
        InvalidIngestStage()
    for m in [
        "_run",
        "_write_output",
        "_read_output",
        "_validate_run_parameters",
    ]:
        assert m in str(e.value)


def test_invalid_run_parameters():
    # Test that InvalidIngestStageParameters excp raised on invalid run params
    class ValidIngestStage(IngestStage):
        def _run(self, foo):
            pass

        def _postrun_concept_discovery(self, run_output):
            pass

        def _validate_run_parameters(self, foo):
            if not foo:
                raise InvalidIngestStageParameters

        def _write_output(self, output):
            pass

        def _read_output(self):
            pass

    stage = ValidIngestStage()
    with pytest.raises(InvalidIngestStageParameters):
        stage.run(False)

    # No exception should be raised on valid params
    stage.run(True)


def test_stage_dir_creation(ValidIngestStage):
    """
    Test that a stage's output dir gets created properly
    """
    stage = ValidIngestStage(ingest_output_dir=TEST_INGEST_OUTPUT_DIR)
    stage.run("foo")

    assert os.path.isdir(stage.ingest_output_dir)
    assert os.path.isdir(stage.stage_cache_dir)
    assert os.path.basename(stage.stage_cache_dir) == type(stage).__name__


def test_stage_read_write(ValidIngestStage):
    """
    Test ingest stage read/write
    """
    # Test output is not written if no stage_cache_dir defined
    # and upon read_output, a FileNotFoundError error is raised
    stage = ValidIngestStage()
    assert stage.stage_cache_dir is None
    stage.run("hello world")
    with pytest.raises(FileNotFoundError) as e:
        stage.read_output()
    assert 'The output directory: "None" does not exist' in str(e.value)

    # Test output is written and read when stage_cache_dir is defined
    stage = ValidIngestStage(ingest_output_dir=TEST_INGEST_OUTPUT_DIR)
    run_input = "hello world"
    run_output = stage.run(run_input)
    assert stage.read_output() == run_output


def test_missing_run_parameters():
    pass


def test_serialize_output():
    pass


def test_deserialize_output():
    pass
