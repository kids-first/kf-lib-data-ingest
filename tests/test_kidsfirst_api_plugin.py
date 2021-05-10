import json
from os.path import join

from click.testing import CliRunner
from kf_lib_data_ingest.app import cli
from kf_lib_data_ingest.target_api_plugins.kids_first_dataservice import (
    DELIMITER,
)

from conftest import TEST_DATA_DIR, delete_dir


def test_external_id_whole_ingest():
    # Make sure that the external_id function combines available key components
    # and doesn't error if referencing target service IDs directly. This test
    # actually runs an entire ingest and looks at what would get sent.
    runner = CliRunner()
    study_dir = join(TEST_DATA_DIR, "kfid_study")
    output_dir = join(study_dir, "output")
    delete_dir(output_dir)
    runner.invoke(cli.test, study_dir)
    with open(
        join(output_dir, "LoadStage", "SentMessages_localhost_5000.json")
    ) as sm:
        data = json.load(sm)
    gfs = [e for e in data if e["type"] == "genomic_file"]
    bsgfs = [e for e in data if e["type"] == "biospecimen_genomic_file"]
    assert gfs[0]["body"]["external_id"] == "blah1"
    assert gfs[1]["body"]["external_id"] == "blah2"
    assert bsgfs[0]["body"]["external_id"] is None
    assert bsgfs[1]["body"]["external_id"] == f"BS1{DELIMITER}blah2"
    delete_dir(output_dir)
