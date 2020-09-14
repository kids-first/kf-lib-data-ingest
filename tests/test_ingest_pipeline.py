import os
import shutil

import pytest
from click.testing import CliRunner

from conftest import COMMAND_LINE_ERROR_CODE, TEST_DATA_DIR, delete_dir
from kf_lib_data_ingest.app import cli
from kf_lib_data_ingest.config import VERSION
from kf_lib_data_ingest.etl.ingest_pipeline import (
    CODE_TO_STAGE_MAP,
    DEFAULT_STAGES_TO_RUN_STR,
    VALID_STAGES_TO_RUN_STRS,
)

TEST_STUDY_CONFIG = os.path.join(
    TEST_DATA_DIR, "test_study", "ingest_package_config.py"
)
SIMPLE_STUDY_CONFIG = os.path.join(
    TEST_DATA_DIR, "simple_study", "ingest_package_config.py"
)


@pytest.fixture(scope="session")
def simple_study_cfg():
    output_dir = os.path.join(os.path.split(SIMPLE_STUDY_CONFIG)[0], "output")
    delete_dir(output_dir)

    yield SIMPLE_STUDY_CONFIG

    delete_dir(output_dir)


@pytest.mark.parametrize("cli_cmd", [cli.test])
@pytest.mark.parametrize(
    "stages_to_run_str",
    [DEFAULT_STAGES_TO_RUN_STR]
    + VALID_STAGES_TO_RUN_STRS,  # must run all stages first
)
def test_valid_ingest_subset_stages(
    simple_study_cfg, cli_cmd, stages_to_run_str
):
    runner = CliRunner()
    result = runner.invoke(
        cli_cmd, [simple_study_cfg, "--stages", stages_to_run_str]
    )
    assert "BEGIN data ingestion" in result.output
    assert "END data ingestion" in result.output
    assert result.exit_code == 0
    pipeline_stage_names = {
        c: CODE_TO_STAGE_MAP[c].__name__ for c in CODE_TO_STAGE_MAP
    }
    pipeline_stage_names["t"] = "GuidedTransformStage"

    # Check logs
    def check_logs(char_seq, should_exist):
        for c in char_seq:
            stg_cls_name = pipeline_stage_names[c]
            assert (f"BEGIN {stg_cls_name}" in result.output) == should_exist
            assert (f"END {stg_cls_name}" in result.output) == should_exist
            # If stage before current not in stages to run, load cached output
            idx = DEFAULT_STAGES_TO_RUN_STR.index(c) - 1
            if (
                idx > 0
                and DEFAULT_STAGES_TO_RUN_STR[idx] not in stages_to_run_str
            ):
                prev_name = pipeline_stage_names.get(
                    DEFAULT_STAGES_TO_RUN_STR[idx]
                )
                assert (
                    f"Loading previously cached output "
                    f"from {prev_name}" in result.output
                ) == should_exist

    check_logs(stages_to_run_str, True)
    check_logs(CODE_TO_STAGE_MAP.keys() - list(stages_to_run_str), False)


@pytest.mark.parametrize("cli_cmd", [cli.test])
@pytest.mark.parametrize(
    "stages_to_run_str",
    ["foo"]
    + [  # invalid stage codes
        s[0] + s[-1]
        for s in VALID_STAGES_TO_RUN_STRS  # gaps in stage sequence
        if len(s) >= 3
    ]
    + [
        s[::-1] for s in VALID_STAGES_TO_RUN_STRS if len(s) > 1
    ],  # out of order stages
)
def test_invalid_ingest_subset_stages(
    simple_study_cfg, cli_cmd, stages_to_run_str
):
    runner = CliRunner()
    result = runner.invoke(
        cli_cmd, [simple_study_cfg, "--stages", stages_to_run_str]
    )
    assert result.exit_code == COMMAND_LINE_ERROR_CODE
    assert "invalid choice" in result.output


def test_ingest_cmd_missing_required_args():
    """
    Test ingest CLI when required arguments are missing

    Req args:
        Path to ingest_package_config.py or dir containing the file
    """
    runner = CliRunner()
    result = runner.invoke(cli.ingest, [])

    assert "ingest_package_config_path".upper() in result.output
    assert result.exit_code == COMMAND_LINE_ERROR_CODE


@pytest.mark.parametrize(
    "cli_cmd, arg_list",
    [
        (cli.test, [SIMPLE_STUDY_CONFIG, "--log_level", "debug"]),
        (
            cli.ingest,
            [SIMPLE_STUDY_CONFIG, "--dry_run", "--log_level", "debug"],
        ),
    ],
)
def test_ingest_cmds(cli_cmd, arg_list):
    """
    Test ingest and test CLI commands - guided transform
    """
    runner = CliRunner()
    result = runner.invoke(cli_cmd, arg_list)
    assert result.exit_code == 0

    assert "BEGIN data ingestion" in result.output
    assert "END data ingestion" in result.output
    assert "version" in result.output
    assert f"{VERSION}" in result.output

    # Make sure that post-extract validation runs
    # TODO

    assert "DRY RUN" in result.output


def test_ingest_no_transform_module(tmpdir):
    """
    Test ingest with no transform module defined

    Note - This test is temporary. When auto-transform implementation is
    complete, the absence of a transform module will result in the execution
    of auto-transform. This test will be modified to check that auto-transform
    executes.
    """
    # Copy test_study dir and remove the transform module
    study_dir = os.path.join(TEST_DATA_DIR, "test_study")
    new_study_dir = os.path.join(tmpdir, "study")
    shutil.copytree(study_dir, new_study_dir)
    tm = os.path.join(new_study_dir, "transform_module.py")
    if os.path.isfile(tm):
        os.remove(tm)

    # Remove transform module from ingest config
    ingest_config_path = os.path.join(new_study_dir, "ingest_package_config.py")
    with open(ingest_config_path) as txtfile:
        lines = [
            line
            for line in txtfile.readlines()
            if not line.strip().startswith("transform_function_path")
        ]
    with open(ingest_config_path, "w") as txtfile:
        txtfile.write("".join(lines))

    # Run ingest
    runner = CliRunner()
    result = runner.invoke(cli.ingest, f"{ingest_config_path} --dry_run")
    assert result.exit_code > 0
    assert "Transform module file has not been created yet" in result.output
