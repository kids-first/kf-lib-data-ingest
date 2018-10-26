from common import constants
from etl.extract.operations import *
from etl.transform.standard_model.concept_schema import CONCEPT

source_data_url = 'file://../data/simple_headered_tsv_1.tsv'

source_data_loading_parameters = {}


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
    value_map(
        in_col="mother",
        m=lambda x: x,
        out_col=CONCEPT.PARTICIPANT.MOTHER_ID
    ),
    value_map(
        in_col="father",
        m=lambda x: x,
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
        in_col="consent",
        m={
            "1": constants.CONSENT_TYPE.GRU,
            "2": constants.CONSENT_TYPE.HMB_IRB,
            "3": constants.CONSENT_TYPE.DS_OC_PUB_MDS
        },
        out_col=CONCEPT.PARTICIPANT.CONSENT_TYPE
    ),
    [
        value_map(
            in_col=6,  # age in hours (first)
            m=lambda x: int(x)/24,
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
            in_col=9,  # age in hours (second)
            m=lambda x: int(x)/24,
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
