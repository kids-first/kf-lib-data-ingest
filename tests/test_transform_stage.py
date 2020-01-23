import os

import pandas as pd
import pytest

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.constants import RACE
from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters


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
        assert (
            pd.DataFrame(other_data)
            .sort_index(axis=1)
            .equals(pd.DataFrame(data).sort_index(axis=1))
        )
        assert os.path.isfile(
            os.path.join(guided_transform_stage.stage_cache_dir, target_entity)
            + ".tsv"
        )
