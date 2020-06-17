import os

import pytest

from conftest import delete_dir_contents, make_ingest_pipeline
from kf_lib_data_ingest.factory.generate import new_ingest_pkg


@pytest.fixture(scope="function")
def ingest_pkg_dir(tmpdir):
    ingest_pkg_dir = new_ingest_pkg(os.path.join(tmpdir, "study1"))
    delete_dir_contents(os.path.join(ingest_pkg_dir, "tests"))
    return ingest_pkg_dir


def create_test_stub(ingest_pkg_dir, true_or_false):
    """
    Create passing or failing test file
    """
    filepath = os.path.join(
        ingest_pkg_dir, "tests", f"test_passing_{true_or_false}.py"
    )
    with open(filepath, "w") as test_file:
        content = f"def test():\n    assert {true_or_false}"
        test_file.write(content)
    assert os.path.isfile(filepath)


def test_user_def_test_pass(info_caplog, ingest_pkg_dir):
    """
    Test successful user defined tests
    """
    create_test_stub(ingest_pkg_dir, True)
    ingest_pipeline = make_ingest_pipeline(
        config_filepath=ingest_pkg_dir,
        log_dir=os.path.join(ingest_pkg_dir, "logs"),
    )
    assert ingest_pipeline.run()
    assert "✅ User defined data validation tests passed" in info_caplog.text


def test_user_def_test_not_collected(info_caplog, ingest_pkg_dir):
    """
    Test successful pass of user defined tests even when no tests collected
    """
    # Run pipeline and check log
    ingest_pipeline = make_ingest_pipeline(
        config_filepath=ingest_pkg_dir,
        log_dir=os.path.join(ingest_pkg_dir, "logs"),
    )
    assert ingest_pipeline.run()
    assert (
        "⚠️ pytest did not collect any user defined tests" in info_caplog.text
    )


def test_user_def_test_fail(info_caplog, ingest_pkg_dir):
    """
    Test failure of user defined tests
    """
    create_test_stub(ingest_pkg_dir, False)

    # Run pipeline and check log
    ingest_pipeline = make_ingest_pipeline(
        config_filepath=ingest_pkg_dir,
        log_dir=os.path.join(ingest_pkg_dir, "logs"),
    )
    assert not ingest_pipeline.run()
    assert "❌ User defined data validation tests failed" in info_caplog.text
