from dateutil import parser

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT


MIN_AGE_DAYS = 0
MAX_AGE_DAYS = 32872
NULL_VALUES = {
    constants.COMMON.NOT_REPORTED,
    constants.COMMON.CANNOT_COLLECT,
    constants.COMMON.NOT_APPLICABLE,
    constants.COMMON.UNKNOWN,
    constants.COMMON.NOT_AVAILABLE
}

check_bool = lambda x: x in {
        constants.COMMON.TRUE,
        constants.COMMON.FALSE
    }

check_age = lambda x: (int(x) >= MIN_AGE_DAYS) and (int(x) < MAX_AGE_DAYS)

def check_date(x):
    try:
        value = parser.parse(str(value)) if value else None
    except ValueError as e:
        return False
    return True


check_positive = lambda x: int(x) > 0


INPUT_VALIDATION = {
    # PARTICIPANT
    CONCEPT.PARTICIPANT.RACE: lambda x: x in {
          constants.RACE.WHITE,
          constants.RACE.NATIVE_AMERICAN,
          constants.RACE.BLACK,
          constants.RACE.ASIAN,
          constants.RACE.PACIFIC,
          constants.COMMON.OTHER,
          constants.RACE.MULTIPLE
    },
    CONCEPT.PARTICIPANT.ENROLLMENT_AGE_DAYS: check_age,
    CONCEPT.PARTICIPANT.IS_PROBAND: check_bool,
    CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY: check_bool,
    CONCEPT.PARTICIPANT.ETHNICITY: lambda x: x in {
        constants.ETHNICITY.HISPANIC,
        constants.ETHNICITY.NON_HISPANIC
    },
    CONCEPT.PARTICIPANT.GENDER: lambda x: x in {
        constants.GENDER.MALE,
        constants.GENDER.FEMALE,
        constants.COMMON.OTHER
    },
    CONCEPT.PARTICIPANT.SPECIES: lambda x: x in {
        constants.SPECIES.DOG,
        constants.SPECIES.HUMAN
    },
    CONCEPT.PARTICIPANT.CONSENT_TYPE: lambda x: x in {
        constants.CONSENT_TYPE.DS_OBDR_MDS,
        constants.CONSENT_TYPE.DS_OBD_MDS,
        constants.CONSENT_TYPE.DS_OC_PUB_MDS,
        constants.CONSENT_TYPE.HMB_MDS,
        constants.CONSENT_TYPE.HMB_IRB,
        constants.CONSENT_TYPE.DS_CHD,
        constants.CONSENT_TYPE.DS_CHD_IRB,
        constants.CONSENT_TYPE.GRU
    },

    # BIOSPECIMEN
    CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS: check_age,
    CONCEPT.BIOSPECIMEN.ANALYTE: lambda x: x in {
        constants.SEQUENCING.ANALYTE.DNA,
        constants.SEQUENCING.ANALYTE.RNA,
        constants.SEQUENCING.ANALYTE.VIRTUAL,
        constants.COMMON.OTHER
    },
    CONCEPT.BIOSPECIMEN.CONCENTRATION_MG_PER_ML: check_positive,
    CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT: lambda x: x in {
        constants.SPECIMEN.SAMPLE_PROCUREMENT.AUTOPSY,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.BIOPSY,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.SUBTOTAL_RESECTION,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.TOTAL_RESECTION,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.BLOOD_DRAW,
        constants.COMMON.OTHER
    },
    CONCEPT.BIOSPECIMEN.SHIPMENT_DATE: check_date,
    CONCEPT.BIOSPECIMEN.VISIBLE: check_bool,
    CONCEPT.BIOSPECIMEN.VOLUME_UL: check_positive,

    # BIOSPECIMEN_DIAGNOSIS
    CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE: check_bool,

    # BIOSPECIMEN_GENOMIC_FILE
    CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE: check_bool,

    # DIAGNOSIS
    CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS: check_positive,
    CONCEPT.DIAGNOSIS.CATEGORY: lambda x: x in {
        constants.STUDY.CANCER,
        constants.STUDY.STRUCTURAL_DEFECT,
        constants.COMMON.OTHER
    },
    CONCEPT.DIAGNOSIS.VISIBLE: check_bool,

    # FAMILY
    CONCEPT.FAMILY.VISIBLE: check_bool,

    # FAMILY_RELATIONSHIP
    CONCEPT.FAMILY_RELATIONSHIP.VISIBLE: check_bool,

    # GENOMIC_FILE
    CONCEPT.GENOMIC_FILE.AVAILABILITY: lambda x:
        (x in {
            constants.GENOMIC_FILE.AVAILABILITY.IMMEDIATE,
            constants.GENOMIC_FILE.AVAILABILITY.COLD_STORAGE
        }) or
        (x in NULL_VALUES),
    CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS: check_bool,


}



