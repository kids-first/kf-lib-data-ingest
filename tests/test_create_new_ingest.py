import filecmp
import logging
import os

from click.testing import CliRunner

from kf_lib_data_ingest.app import cli
from kf_lib_data_ingest.config import INGEST_PKG_TEMPLATE_NAME, TEMPLATES_DIR
from kf_lib_data_ingest.factory.generate import new_ingest_pkg


def test_new_ingest_pkg(tmpdir):
    """
    Test create new ingest package
    """
    expected_dir = os.path.join(tmpdir, "study1")
    ret_dest_dir = new_ingest_pkg(expected_dir)

    # Created dest dir = expected path
    assert ret_dest_dir == expected_dir

    # Created dest dir exists
    assert os.path.isdir(ret_dest_dir)

    # Compare template dir with created dir
    template_dir = os.path.join(TEMPLATES_DIR, INGEST_PKG_TEMPLATE_NAME)
    cmp_out = filecmp.dircmp(template_dir, ret_dest_dir)
    diff = set(cmp_out.left_list).symmetric_difference(set(cmp_out.right_list))
    assert len(diff) == 0


def test_new_ingest_already_exists(tmpdir):
    """
    Test attempt to create a new ingest pkg when one with same path already
    exists
    """
    dest_dir = os.path.join(tmpdir, "new_study")
    ret_dest_dir = new_ingest_pkg(dest_dir)

    dest_dir = new_ingest_pkg(ret_dest_dir)
    assert dest_dir is None


def test_ingest_template_study(caplog, tmpdir):
    """
    Test ingest runs successfully for the template package
    """
    caplog.set_level(logging.DEBUG)
    # > kidsfirst new
    study_dir = os.path.join(tmpdir, "my_study")
    runner = CliRunner()
    result = runner.invoke(cli.create_new_ingest, ["--dest_dir", study_dir])

    # Dir created
    assert os.path.isdir(study_dir)
    # No exceptions raised
    assert result.exit_code == 0

    # Run ingest on the generated template package
    result = runner.invoke(
        cli.ingest, [study_dir, "--dry_run", "--log_level", "debug"]
    )
    assert result.exit_code == 0
    assert "EXPECTED COUNT CHECKS" in caplog.text
    assert "END data ingestion"
    assert os.path.isdir(os.path.join(study_dir, "output"))
