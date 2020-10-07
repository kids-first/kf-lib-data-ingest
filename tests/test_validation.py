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
    Validator as DataValidator
)
from kf_lib_data_ingest.validation.default_hierarchy import DH

from conftest import TEST_DATA_DIR


@pytest.fixture(scope='session')
def valid_df():
    """
    A pandas.DataFrame which passes data validation
    """
    # Identifiers
    df = pandas.DataFrame(
        {
            col: [f'{col[0:2]}{i}' for i in range(5)]
            for col in DH.nodes()
            if col.endswith('ID')
        }
    )
    # Other required attributes
    attr = {
        CONCEPT.BIOSPECIMEN.ANALYTE: lambda row: 'DNA',
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
        )
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
    valid_df.to_csv(os.path.join(temp_dir, 'data.tsv'), sep='\t', index=False)

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
