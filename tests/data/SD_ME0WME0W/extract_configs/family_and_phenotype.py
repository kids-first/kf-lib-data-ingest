from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.extract.operations import *
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT
)
import re

# source_data_url = 'file://../data/family_and_phenotype.tsv'
host = 'https://kf-study-creator.kidsfirstdrc.org'
kfid = 'SF_HH5PMCJX'
source_data_url = f'{host}/download/study/SD_ME0WME0W/file/{kfid}'

source_data_read_params = {
    "header": 1,
    "usecols": lambda x: x != "[ignore]"
}


def observed_yes_no(x):
    if isinstance(x, str):
        x = x.lower()
    if x in {"true", "yes", 1}:
        return constants.PHENOTYPE.OBSERVED.YES
    elif x in {"false", "no", 0}:
        return constants.PHENOTYPE.OBSERVED.NO
    elif x in {"", None}:
        return None


operations = [
    value_map(
        in_col="participant",
        m={
            r"PID(\d+)": lambda x: int(x),  # strip PID and 0-padding
        },
        out_col=CONCEPT.PARTICIPANT.ID
    ),
    keep_map(
        in_col="mother",
        out_col=CONCEPT.PARTICIPANT.MOTHER_ID
    ),
    keep_map(
        in_col="father",
        out_col=CONCEPT.PARTICIPANT.FATHER_ID
    ),
    value_map(
        in_col="gender",
        # Don't worry about mother/father gender here.
        # We can create them in a later phase.
        m={
            "F": constants.GENDER.FEMALE,
            "M": constants.GENDER.MALE
        },
        out_col=CONCEPT.PARTICIPANT.GENDER
    ),
    value_map(
        in_col="specimens",
        m=lambda x: Split(re.split('[,;]', x)),
        out_col=CONCEPT.BIOSPECIMEN.ID
    ),
    [
        value_map(
            in_col=6,  # age (hrs) (first)
            m=lambda x: int(x) / 24,
            out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
        ),
        melt_map(
            var_name=CONCEPT.PHENOTYPE.NAME,
            map_for_vars={
                "CLEFT_EGO": "Cleft ego",
                "CLEFT_ID": "Cleft id"
            },
            value_name=CONCEPT.PHENOTYPE.OBSERVED,
            map_for_values=observed_yes_no
        )
    ],
    [
        value_map(
            in_col=9,  # age (hrs) (second)
            m=lambda x: int(x) / 24,
            out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
        ),
        melt_map(
            var_name=CONCEPT.PHENOTYPE.NAME,
            map_for_vars={
                "EXTRA_EARDRUM": "Extra eardrum"
            },
            value_name=CONCEPT.PHENOTYPE.OBSERVED,
            map_for_values=observed_yes_no
        )
    ]
]
