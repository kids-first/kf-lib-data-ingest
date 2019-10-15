import os
import pytest
import pandas as pd

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT,
    DELIMITER,
    UNIQUE_ID_ATTR,
    concept_from,
    unique_key_composition,
)
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.etl.transform.transform import VALUE_DELIMITER

from kf_lib_data_ingest.common.constants import RACE


def check_compound_uk(row, ukey_col, col1, col2):
    return (
        row[ukey_col].split(VALUE_DELIMITER)[0] == row[col1]
        and row[ukey_col].split(VALUE_DELIMITER)[1] == row[col2]
    )


@pytest.fixture(scope="function")
def df():
    """
    Reusable test dataframe
    """
    return pd.DataFrame(
        [
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.BIOSPECIMEN_GROUP.ID: "G1",
                CONCEPT.BIOSPECIMEN.ID: "B1",
                CONCEPT.PARTICIPANT.RACE: RACE.WHITE,
            },
            {
                CONCEPT.PARTICIPANT.ID: "P1",
                CONCEPT.BIOSPECIMEN_GROUP.ID: "G1",
                CONCEPT.BIOSPECIMEN.ID: "B2",
                CONCEPT.PARTICIPANT.RACE: RACE.WHITE,
            },
            {
                CONCEPT.PARTICIPANT.ID: "P2",
                CONCEPT.BIOSPECIMEN_GROUP.ID: "G1",
                CONCEPT.BIOSPECIMEN.ID: "B3",
                CONCEPT.PARTICIPANT.RACE: RACE.ASIAN,
            },
        ]
    )


def test_invalid_run_parameters(guided_transform_stage, **kwargs):
    """
    Test running transform with invalid run params
    """

    # Bad keys
    with pytest.raises(InvalidIngestStageParameters):
        guided_transform_stage.run({i: "foo" for i in range(5)})

    # Bad values
    with pytest.raises(InvalidIngestStageParameters):
        guided_transform_stage.run({"foor": ("bar", None) for i in range(5)})


def test_read_write(guided_transform_stage, df):
    """
    Test TransformStage.read_output/write_output
    """
    extract_output = {"extract_config_url": ("source_url", df)}

    # Transform outputs json
    output = guided_transform_stage.run(extract_output)
    recycled_output = guided_transform_stage.read_output()

    for target_entity, data in output.items():
        assert target_entity in recycled_output
        other_data = recycled_output[target_entity]
        # Compare using DataFrames
        assert pd.DataFrame(other_data).equals(pd.DataFrame(data))
        assert os.path.isfile(
            os.path.join(
                guided_transform_stage.stage_cache_dir, "tsv", target_entity
            )
            + ".tsv"
        )
    for key, df in guided_transform_stage.transform_func_output.items():
        assert os.path.isfile(
            os.path.join(
                guided_transform_stage.transform_func_dir, f"{key}.tsv"
            )
        )


def test_unique_keys(guided_transform_stage, df):
    """
    Test that transform stage correctly iterates over each mapped df and
    inserts a unique key column for each concept.

    Most concept's unique keys are composed of just the ID attribute. These
    are standard unique keys
    """

    # 4 Columns before
    assert 4 == len(df.columns)
    df = guided_transform_stage._add_unique_key_cols(
        df, unique_key_composition
    )

    # 4 original + 3 unique key cols for the concepts
    assert 7 == len(df.columns)

    # Num of distinct concepts shouldn't change
    concepts = set([concept_from(col) for col in df.columns])
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
                lambda row: check_compound_uk(row, ukey_col, col1, col2),
                axis=1,
            ).all()
        else:
            assert df[id_col].equals(df[ukey_col])


def test_compound_unique_keys(guided_transform_stage):
    """
    Test that compound unique keys are autogenerated.

    These are non-standard unique keys for concepts that do not
    have an ID attribute to use as a unique key. These concepts have unique
    keys which are composed of several other concept attributes.
    """
    df = pd.DataFrame(
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
    df = guided_transform_stage._add_unique_key_cols(
        df, unique_key_composition
    )
    # 3 original + 3 unique key columns + 1 for the compound concept
    assert 7 == len(df.columns)
    # 4 distinct concepts should exist now (1 new one is a compound concept)
    concepts = set([concept_from(col) for col in df.columns])
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
                lambda row: check_compound_uk(row, ukey_col, col1, col2),
                axis=1,
            ).all()
        else:
            assert df[id_col].equals(df[ukey_col])


def test_unique_key_w_optional(guided_transform_stage):
    """
    Test unique key construction for concept whose unique key has both
    required components and optional components
    """
    df = pd.DataFrame(
        {
            CONCEPT.PARTICIPANT.ID: ["p1", "p2", "p3"],
            CONCEPT.DIAGNOSIS.NAME: ["cold", "flu", "something"],
            CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS: [20, 30, 40],
        }
    )
    df = guided_transform_stage._add_unique_key_cols(
        df, unique_key_composition
    )

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


def test_no_key_comp_defined(guided_transform_stage):
    """
    Test concept in concept schema does not have a unique key comp defined
    """
    df = pd.DataFrame(
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
    save = unique_key_composition[CONCEPT.PARTICIPANT._CONCEPT_NAME]
    del unique_key_composition[CONCEPT.PARTICIPANT._CONCEPT_NAME]
    with pytest.raises(AssertionError) as e:
        guided_transform_stage._add_unique_key_cols(df, unique_key_composition)
    assert "key composition not defined" in str(e.value)
    unique_key_composition[CONCEPT.PARTICIPANT._CONCEPT_NAME] = save


def test_nulls_in_unique_keys(guided_transform_stage):
    """
    If any required subvalue of the unique key string is null,
    then the resulting value of the unique key should be None

    If any optional subvalue of the unique key string is null,
    then it should have been replaced with constants.COMMON.NOT_REPORTED
    """

    dfs = {
        "family": pd.DataFrame(
            {
                CONCEPT.PARTICIPANT.ID: ["p1", "p2", "p4"],
                CONCEPT.FAMILY.ID: ["f1", "f2", "f2"],
            }
        ),
        "participant": pd.DataFrame(
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
        "biospecimen": pd.DataFrame(
            {
                CONCEPT.PARTICIPANT.ID: ["p1", "p2", "p2"],
                CONCEPT.BIOSPECIMEN.ID: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN_GROUP.ID: ["bg1", "bg1", "bg1"],
                CONCEPT.BIOSPECIMEN.ANALYTE: ["dna", "rna", "dna"],
            }
        ),
        "genomic_file": pd.DataFrame(
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
    df = guided_transform_stage._add_unique_key_cols(
        df, unique_key_composition
    )
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
    df = guided_transform_stage._add_unique_key_cols(
        df, unique_key_composition
    )
    assert df[CONCEPT.BIOSPECIMEN_GENOMIC_FILE.UNIQUE_KEY].values.tolist() == [
        "bg1-b1-g1",
        "bg1-b2-g2",
        None,
        "bg1-b4-g3",
    ]

    # Test compound unique key with optional components
    df = guided_transform_stage._add_unique_key_cols(
        dfs["participant"], unique_key_composition
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
