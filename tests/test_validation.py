from click.testing import CliRunner
from kf_lib_data_ingest.app import cli


def test_validate_cmd(tmpdir):
    """
    Test the validate CLI command.
    """
    path = tmpdir.mkdir("empty")
    runner = CliRunner()
    result = runner.invoke(cli.validate, [str(path)])
    assert result.exit_code == 0
