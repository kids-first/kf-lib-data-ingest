import glob
import os

import pytest
import pandas
from deepdiff import DeepDiff
from click.testing import CliRunner

from kf_lib_data_ingest.app import cli
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.io import read_df
from kf_lib_data_ingest.common.misc import import_module_from_file
from kf_lib_data_ingest.validation.data_validator import (
    NA,
    Validator as DataValidator,
)
from kf_lib_data_ingest.validation.reporting.markdown import (
    MarkdownReportBuilder,
    REL_TEST,
    GAP_TEST,
    ATTR_TEST,
    COUNT_TEST,
)

from kf_lib_data_ingest.validation.default_hierarchy import DH
from kf_lib_data_ingest.validation.validation import Validator
from kf_lib_data_ingest.validation.reporting import sample_validation_results
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type,
)
from conftest import TEST_DATA_DIR


@pytest.fixture(scope="session")
def valid_df():
    """
    A pandas.DataFrame which passes data validation
    """
    # Identifiers
    df = pandas.DataFrame(
        {
            col: [f"{col[0:2]}{i}" for i in range(5)]
            for col in DH.nodes()
            if col.endswith("ID")
        }
    )
    # Other required attributes
    attr = {
        CONCEPT.BIOSPECIMEN.ANALYTE: lambda row: "DNA",
        CONCEPT.GENOMIC_FILE.HARMONIZED: lambda row: True,
        CONCEPT.GENOMIC_FILE.REFERENCE_GENOME: (
            lambda row: constants.SEQUENCING.REFERENCE_GENOME.GRCH38
        ),
        CONCEPT.GENOMIC_FILE.URL_LIST: (
            lambda row: row[CONCEPT.GENOMIC_FILE.ID]
        ),
        CONCEPT.SEQUENCING.STRATEGY: (
            lambda row: constants.SEQUENCING.STRATEGY.WGS
        ),
        CONCEPT.SEQUENCING.PAIRED_END: (
            lambda row: constants.COMMON.NOT_REPORTED
        ),
    }
    for col, func in attr.items():
        df[col] = df.apply(func, axis=1)

    return df


def test_validate_cmd(tmpdir, valid_df):
    """
    Test validate CLI command
    """
    # Create test data
    temp_dir = tmpdir.mkdir("empty")
    valid_df.to_csv(os.path.join(temp_dir, "data.tsv"), sep="\t", index=False)

    runner = CliRunner()
    result = runner.invoke(cli.validate, [str(temp_dir)])
    assert result.exit_code == 0


def diff(desc, a, b):
    if isinstance(a, list):
        a = sorted(a, key=lambda x: x["from"])
    if isinstance(b, list):
        b = sorted(b, key=lambda x: x["from"])
    diff = DeepDiff(a, b, ignore_order=True, report_repetition=True)
    if diff:
        print("Desc:", desc)
        print("Field:", a)
        assert False, diff.pretty()


def verify_counts(base, reference):
    base = {k: v for k, v in base.items() if v != 0}
    reference = {k: v for k, v in reference.items() if v != 0}
    for k, v in base.items():
        diff(k, v, reference.get(k))
    for k, v in reference.items():
        diff(k, base.get(k), v)


def verify_test_results(base, reference):
    base = {r["description"]: list(r["errors"]) for r in base if r["errors"]}
    try:
        for k, v in base.items():
            diff(k, v, reference.get(k))
        for k, v in reference.items():
            diff(k, base.get(k), v)
    except AssertionError:
        print()
        print("Base:", base)
        raise


def verify_df_dict(df_dict, reference, test_hierarchy):
    results = DataValidator(test_hierarchy).validate(df_dict)
    assert sorted(results["files_validated"]) == sorted(df_dict.keys())
    verify_counts(results["counts"], reference.counts)
    verify_test_results(results["validation"], reference.validation)


@pytest.mark.parametrize(
    "dir", sorted(glob.glob(f"{TEST_DATA_DIR}/validation/DATASET*"))
)
def test_datasets(dir):
    df_dict = {}
    for f in glob.glob(f"{dir}/*.csv"):
        try:
            fname = os.path.basename(f)
            df_dict[fname] = read_df(f).replace("NA", NA)
        except Exception:
            continue

    reference = import_module_from_file(f"{dir}/reference.py")
    try:
        test_hierarchy = import_module_from_file(
            f"{dir}/hierarchy_override.py"
        ).HIERARCHY
    except FileNotFoundError:
        test_hierarchy = None
    assert df_dict and reference
    verify_df_dict(df_dict, reference, test_hierarchy)


def test_build_reports(tmpdir, info_caplog):
    """
    Test validation report building in kf_lib_data_ingest.validation.reporting
    """
    results = sample_validation_results.results

    # Test format of validation results
    _validate_result_schema(sample_validation_results.results)

    # Test normal case with sample validation results
    path = tmpdir.mkdir("validation_results")
    report_paths = Validator(output_dir=path)._build_report(
        results, formats={"tsv", "md", "foobar"}
    )
    # Unsupported report format
    assert "`foobar` not supported" in info_caplog.text

    # Check report(s) content
    for rp in report_paths:
        assert os.path.isfile(rp)
        fn, ext = os.path.splitext(rp)
        if ext == ".md":
            with open(rp, "r") as md_file:
                assert md_file.read()
        elif ext == ".tsv":
            df = None
            try:
                df = read_df(rp)
            except Exception:
                assert False
            assert (df is not None) and (not df.empty)

    # Test exclude_tests kwarg
    # Default - all tests
    b = MarkdownReportBuilder()
    report = b._build(results)
    tests = [REL_TEST, GAP_TEST, ATTR_TEST, COUNT_TEST]
    for t in tests:
        assert t.title() + " Tests" in report

    # Exclude some tests
    report = b._build(results, exclude_tests=tests[1:])
    for t in tests[1:]:
        assert t.title() + " Tests" not in report


def _validate_result_schema(results):
    """
    Validate structure and format of validation test result dicts
    """
    req_keys = {"counts", "files_validated", "validation"}
    req_result_keys = {
        "type",
        "description",
        "is_applicable",
        "errors",
        "inputs",
    }

    assert_safe_type(results["counts"], dict)
    assert_safe_type(results["files_validated"], list)
    assert_safe_type(results["validation"], list)

    # Check for top level required keys by attempting to access all
    # expected keys
    for k in req_keys:
        results[k]

    # Check for validation result dict required keys by attempting to
    # access all expected keyss
    for r in results["validation"]:
        for k in req_result_keys:
            r[k]

        # Check types
        assert_safe_type(r["type"], str)
        assert_safe_type(r["description"], str)
        assert_safe_type(r["is_applicable"], bool)
        assert_safe_type(r["errors"], list, dict)

        # -- Check error dicts --
        # Ensure error dicts follow expected structure by attempting to
        # access all keys and values. Catch any exception, log that there
        # was a validation error in the validation result's error dict,
        # re-raise the original exception
        errs = r["errors"]
        if r["type"] == "relationship":
            for e in errs:
                t, v = e["from"]
                if e["to"]:
                    for to_node in e["to"]:
                        t1, v1 = to_node
                for (t2, v2), files in e["locations"].items():
                    pass
        elif r["type"] == "attribute":
            assert_all_safe_type(errs.keys(), str)
            assert_all_safe_type(errs.values(), set)

        elif r["type"] == "count":
            assert_all_safe_type(errs.keys(), str)
            assert_all_safe_type(errs.values(), int)
