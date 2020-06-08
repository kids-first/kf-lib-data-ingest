import logging
import os

import pytest
import requests_mock
from pandas import DataFrame

from conftest import KIDSFIRST_DATASERVICE_PROD_URL, TEST_DATA_DIR
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT,
    DELIMITER,
    UNIQUE_ID_ATTR,
    concept_from,
)
from kf_lib_data_ingest.common.io import read_json
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.config import DEFAULT_KEY
from kf_lib_data_ingest.etl.load.message_packer import (
    DEFAULT_KEY_COMP,
    VALUE_DELIMITER,
)
from kf_lib_data_ingest.network.utils import get_open_api_v2_schema

schema_url = f"{KIDSFIRST_DATASERVICE_PROD_URL}/swagger"
mock_dataservice_schema = read_json(
    os.path.join(TEST_DATA_DIR, "mock_dataservice_schema.json")
)


def check_compound_uk(row, ukey_col, col1, col2):
    return (
        row[ukey_col].split(VALUE_DELIMITER)[0] == row[col1]
        and row[ukey_col].split(VALUE_DELIMITER)[1] == row[col2]
    )


@pytest.fixture(scope="function")
@requests_mock.Mocker(kw="mock")
def schema(tmpdir, target_api_config, **kwargs):
    # Setup mock responses
    mock = kwargs["mock"]
    mock.get(schema_url, json=mock_dataservice_schema)

    cached_schema_file = os.path.join(tmpdir, "cached_schema.json")
    output = get_open_api_v2_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        target_api_config.target_concepts.keys(),
        cached_schema_filepath=cached_schema_file,
    )

    return output


@pytest.fixture(scope="function")
def target_instances(message_packer):
    dfs = {
        "participant": DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.PARTICIPANT.GENDER: ["Female", "Male", "Female"],
            }
        ),
        "biospecimen": DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN.ANALYTE: ["dna", "rna", "dna"],
            }
        ),
    }
    all_data_df = dfs["participant"].merge(
        dfs["biospecimen"], on=CONCEPT.PARTICIPANT.UNIQUE_KEY
    )
    return message_packer._standard_to_target({DEFAULT_KEY: all_data_df})


@pytest.fixture(scope="function")
def df_dict():
    """
    Mock input to MessagePacker._standard_to_target
    """
    dfs = {
        "family": DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.FAMILY.UNIQUE_KEY: ["f1", "f2", "f3"],
            }
        ),
        "participant": DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.DIAGNOSIS.NAME: ["cold", "cold", None],
                CONCEPT.PARTICIPANT.GENDER: ["Female", "Male", "Female"],
            }
        ),
        "diagnosis": DataFrame(
            {CONCEPT.DIAGNOSIS.UNIQUE_KEY: ["p1-cold", "p2-cold", None]}
        ),
        "biospecimen": DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN_GROUP.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN.ANALYTE: ["dna", "rna", "dna"],
            }
        ),
        "sequencing_experiment": DataFrame(
            {
                CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN_GROUP.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.SEQUENCING.LIBRARY_NAME: ["lib1", "lib2", "lib3"],
            }
        ),
    }

    df = outer_merge(
        dfs["family"],
        dfs["participant"],
        on=CONCEPT.PARTICIPANT.UNIQUE_KEY,
        with_merge_detail_dfs=False,
    )
    df = outer_merge(
        df,
        dfs["biospecimen"],
        on=CONCEPT.PARTICIPANT.UNIQUE_KEY,
        with_merge_detail_dfs=False,
    )
    df = outer_merge(
        df,
        dfs["sequencing_experiment"],
        on=CONCEPT.BIOSPECIMEN.UNIQUE_KEY,
        with_merge_detail_dfs=False,
    )
    return {"participant": df, DEFAULT_KEY: df}


@pytest.fixture(scope="function")
def df():
    """
    Reusable test dataframe
    """
    return DataFrame(
        [
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.BIOSPECIMEN_GROUP.ID: "G1",
                CONCEPT.BIOSPECIMEN.ID: "B1",
                CONCEPT.PARTICIPANT.RACE: constants.RACE.WHITE,
            },
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.BIOSPECIMEN_GROUP.ID: "G1",
                CONCEPT.BIOSPECIMEN.ID: "B2",
                CONCEPT.PARTICIPANT.RACE: constants.RACE.WHITE,
            },
            {
                CONCEPT.PARTICIPANT.ID: "P2",
                CONCEPT.BIOSPECIMEN_GROUP.ID: "G1",
                CONCEPT.BIOSPECIMEN.ID: "B3",
                CONCEPT.PARTICIPANT.RACE: constants.RACE.ASIAN,
            },
        ]
    )


def test_standard_to_target(caplog, df_dict, target_api_config, message_packer):
    """
    Test MessagePacker._standard_to_target transformation
    """
    # Pytest caplog fixture is set to WARNING by default. Set to INFO so
    # we can capture log messages in GuidedTransformStage._standard_to_target
    caplog.set_level(logging.INFO)

    # Transform
    target_instances = message_packer._standard_to_target(df_dict)

    # Check that output only contains concepts that had data and unique key
    output_concepts = set(target_instances.keys())
    expected_concepts = output_concepts - {"sequencing_experiment"}
    assert output_concepts == expected_concepts

    # Check instances counts and values
    for target_concept, instances in target_instances.items():
        # Only 2 unique participants
        if target_concept == "participant" or target_concept == "diagnosis":
            assert len(instances) == 2
        else:
            assert 3 == len(instances)

        for instance in instances:
            assert instance.get("id")
            assert instance.get("properties")
            assert "links" in instance
            for link_dict in instance["links"]:
                for k, v in link_dict.items():
                    if k in {"study_id", "sequencing_center_id"}:
                        continue
                    assert v

    # Check log output
    no_data_concepts = set(
        target_api_config.target_concepts.keys()
    ).symmetric_difference(set(expected_concepts))
    no_unique_key_msg = "No unique key found in table for target concept:"
    for c in no_data_concepts:
        assert f"{no_unique_key_msg} {c}" in caplog.text


def test_handle_nulls(caplog, message_packer, target_instances, schema):
    """
    Test MessagePacker._handle_nulls

    Normal operation
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test normal operation
    message_packer._handle_nulls(target_instances, schema)
    expected = {
        # a boolean
        "is_proband": None,
        # a string
        "race": constants.COMMON.NOT_REPORTED,
        # an int/float
        "age_at_event_days": None,
        # a datetime
        "shipement_date": None,
    }

    for target_concept, instances in target_instances.items():
        for instance in instances:
            for attr, value in instance.get("properties", {}).items():
                if attr in expected:
                    assert value == expected[attr]


def test_handle_nulls_no_schema(
    caplog, message_packer, target_instances, schema
):
    """
    Test MessagePacker._handle_nulls

    When no schema exists for a target_concept, 'participant'
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Handle nulls
    schema["definitions"].pop("participant")
    message_packer._handle_nulls(target_instances, schema)
    assert (
        "Skip handle nulls for participant. No schema was found." in caplog.text
    )


def test_handle_nulls_no_prop_def(
    caplog, message_packer, target_instances, schema
):
    """
    Test MessagePacker._handle_nulls

    When no property def exists in schema for a property, participant.gender
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test setup
    schema["definitions"]["participant"]["properties"].pop("gender")
    target_instances["participant"][0]["properties"]["gender"] = None

    # Handle nulls
    message_packer._handle_nulls(target_instances, schema)
    assert (
        "No property definition found for "
        "participant.gender in target schema " in caplog.text
    )


def test_unique_keys(message_packer, df):
    """
    Test that MessagePacker correctly iterates over each mapped df and
    inserts a unique key column for each concept.

    Most concept's unique keys are composed of just the ID attribute. These
    are standard unique keys
    """

    # 4 Columns before
    assert 4 == len(df.columns)
    df = message_packer._add_unique_key_cols(df, DEFAULT_KEY_COMP)

    # 4 original + 3 unique key cols for the concepts
    assert 7 == len(df.columns)

    # Num of distinct concepts shouldn't change
    concepts = {concept_from(col) for col in df.columns}
    assert 3 == len(concepts)

    # Check values
    for concept_name in concepts:
        ukey_col = f"{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}"
        id_col = f"{concept_name}{DELIMITER}ID"
        assert ukey_col in df.columns
        if concept_name == CONCEPT.BIOSPECIMEN._CONCEPT_NAME:
            col1 = CONCEPT.BIOSPECIMEN_GROUP.ID
            col2 = CONCEPT.BIOSPECIMEN.ID
            ukey_col = CONCEPT.BIOSPECIMEN.UNIQUE_KEY
            assert df.apply(
                lambda row: check_compound_uk(row, ukey_col, col1, col2), axis=1
            ).all()
        else:
            assert df[id_col].equals(df[ukey_col])


def test_compound_unique_keys(message_packer):
    """
    Test that compound unique keys are autogenerated.

    These are non-standard unique keys for concepts that do not
    have an ID attribute to use as a unique key. These concepts have unique
    keys which are composed of several other concept attributes.
    """
    df = DataFrame(
        [
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.READ_GROUP.ID: "R1",
                CONCEPT.GENOMIC_FILE.ID: "G1",
            },
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.READ_GROUP.ID: "R2",
                CONCEPT.GENOMIC_FILE.ID: "G1",
            },
            {
                CONCEPT.PARTICIPANT.ID: "P2",
                CONCEPT.READ_GROUP.ID: "R3",
                CONCEPT.GENOMIC_FILE.ID: "G1",
            },
        ]
    )

    # 3 columns before
    assert 3 == len(df.columns)
    df = message_packer._add_unique_key_cols(df, DEFAULT_KEY_COMP)
    # 3 original + 3 unique key columns + 1 for the compound concept
    assert 7 == len(df.columns)
    # 4 distinct concepts should exist now (1 new one is a compound concept)
    concepts = {concept_from(col) for col in df.columns}
    assert 4 == len(concepts)

    # Check values
    for concept_name in concepts:
        ukey_col = f"{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}"
        id_col = f"{concept_name}{DELIMITER}ID"
        assert ukey_col in df.columns

        if concept_name == CONCEPT.READ_GROUP_GENOMIC_FILE._CONCEPT_NAME:
            col1 = CONCEPT.READ_GROUP.UNIQUE_KEY
            col2 = CONCEPT.GENOMIC_FILE.UNIQUE_KEY
            ukey_col = CONCEPT.READ_GROUP_GENOMIC_FILE.UNIQUE_KEY
            assert df.apply(
                lambda row: check_compound_uk(row, ukey_col, col1, col2), axis=1
            ).all()
        else:
            assert df[id_col].equals(df[ukey_col])


def test_unique_key_w_optional(message_packer):
    """
    Test unique key construction for concept whose unique key has both
    required components and optional components
    """
    df = DataFrame(
        {
            CONCEPT.PARTICIPANT.ID: ["p1", "p2", "p3"],
            CONCEPT.DIAGNOSIS.NAME: ["cold", "flu", "something"],
            CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS: [20, 30, 40],
        }
    )
    df = message_packer._add_unique_key_cols(df, DEFAULT_KEY_COMP)

    # Check for unique key column names and values
    for ukey_col in [
        CONCEPT.PARTICIPANT.UNIQUE_KEY,
        CONCEPT.DIAGNOSIS.UNIQUE_KEY,
    ]:
        assert ukey_col in df.columns

    def func(row):
        ukey = VALUE_DELIMITER.join(
            [
                str(row[CONCEPT.PARTICIPANT.ID]),
                str(row[CONCEPT.DIAGNOSIS.NAME]),
                str(row[CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS]),
            ]
        )
        return row[ukey_col] == ukey

    assert df.apply(lambda row: func(row), axis=1).all()


def test_no_key_comp_defined(message_packer):
    """
    Test concept in concept schema does not have a unique key comp defined
    """
    df = DataFrame(
        [
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.BIOSPECIMEN.ID: "B1",
                CONCEPT.GENOMIC_FILE.ID: "G1",
            },
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.BIOSPECIMEN.ID: "B2",
                CONCEPT.GENOMIC_FILE.ID: "G1",
            },
            {
                CONCEPT.PARTICIPANT.ID: "P2",
                CONCEPT.BIOSPECIMEN.ID: "B3",
                CONCEPT.GENOMIC_FILE.ID: "G1",
            },
        ]
    )

    # No key composition defined for concept
    save = DEFAULT_KEY_COMP[CONCEPT.PARTICIPANT._CONCEPT_NAME]
    del DEFAULT_KEY_COMP[CONCEPT.PARTICIPANT._CONCEPT_NAME]
    with pytest.raises(AssertionError) as e:
        message_packer._add_unique_key_cols(df, DEFAULT_KEY_COMP)
    assert "key composition not defined" in str(e.value)
    DEFAULT_KEY_COMP[CONCEPT.PARTICIPANT._CONCEPT_NAME] = save


def test_nulls_in_unique_keys(message_packer):
    """
    If any required subvalue of the unique key string is null,
    then the resulting value of the unique key should be None

    If any optional subvalue of the unique key string is null,
    then it should have been replaced with constants.COMMON.NOT_REPORTED
    """

    dfs = {
        "family": DataFrame(
            {
                CONCEPT.PARTICIPANT.ID: ["p1", "p2", "p4"],
                CONCEPT.FAMILY.ID: ["f1", "f2", "f2"],
            }
        ),
        "participant": DataFrame(
            {
                CONCEPT.PARTICIPANT.ID: ["p1", "p2", "p3"],
                CONCEPT.PARTICIPANT.GENDER: ["Female", "Male", "Female"],
                CONCEPT.DIAGNOSIS.NAME: ["cold", "flu", "strep"],
                CONCEPT.PHENOTYPE.NAME: [
                    "extra ear",
                    "enlarged ear",
                    "extra lip",
                ],
                CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS: [300, 400, None],
            }
        ),
        "biospecimen": DataFrame(
            {
                CONCEPT.PARTICIPANT.ID: ["p1", "p2", "p2"],
                CONCEPT.BIOSPECIMEN.ID: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN_GROUP.ID: ["bg1", "bg1", "bg1"],
                CONCEPT.BIOSPECIMEN.ANALYTE: ["dna", "rna", "dna"],
            }
        ),
        "genomic_file": DataFrame(
            {
                CONCEPT.GENOMIC_FILE.ID: ["g1", "g2", "g3"],
                CONCEPT.BIOSPECIMEN.ID: ["b1", "b2", "b4"],
                CONCEPT.BIOSPECIMEN_GROUP.ID: ["bg1", "bg1", "bg1"],
            }
        ),
    }

    # Test a simple unique key
    df = outer_merge(
        dfs["family"],
        dfs["participant"],
        on=CONCEPT.PARTICIPANT.ID,
        with_merge_detail_dfs=False,
    )
    df = message_packer._add_unique_key_cols(df, DEFAULT_KEY_COMP)
    assert df[CONCEPT.FAMILY.UNIQUE_KEY].values.tolist() == [
        "f1",
        "f2",
        "f2",
        None,
    ]

    # Test a compound unique key
    df = outer_merge(
        dfs["biospecimen"],
        dfs["genomic_file"],
        on=CONCEPT.BIOSPECIMEN.ID,
        with_merge_detail_dfs=False,
    )
    df = message_packer._add_unique_key_cols(df, DEFAULT_KEY_COMP)
    assert df[CONCEPT.BIOSPECIMEN_GENOMIC_FILE.UNIQUE_KEY].values.tolist() == [
        "bg1-b1-g1",
        "bg1-b2-g2",
        None,
        "bg1-b4-g3",
    ]

    # Test compound unique key with optional components
    df = message_packer._add_unique_key_cols(
        dfs["participant"], DEFAULT_KEY_COMP
    )
    assert df[CONCEPT.DIAGNOSIS.UNIQUE_KEY].values.tolist() == [
        "p1-cold-300",
        "p2-flu-400",
        "p3-strep-Not Reported",
    ]

    # Test compound unique key with missing optional components
    print(df[CONCEPT.PHENOTYPE.UNIQUE_KEY].values.tolist())
    assert df[CONCEPT.PHENOTYPE.UNIQUE_KEY].values.tolist() == [
        "p1-extra ear-Not Reported-Not Reported",
        "p2-enlarged ear-Not Reported-Not Reported",
        "p3-extra lip-Not Reported-Not Reported",
    ]
