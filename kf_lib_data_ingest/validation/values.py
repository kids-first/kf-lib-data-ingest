from dateutil.parser import parse as parsedate
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT

NA = ""

MIN_AGE_DAYS = 0
# Max value chosen due to HIPAA de-identification standard
# Using safe harbor guidelines
# Equates to 90 years
MAX_AGE_DAYS = 32872
NULL_VALUES = {
    NA,
    constants.COMMON.NOT_ABLE_TO_PROVIDE,
    constants.COMMON.NOT_REPORTED,
    constants.COMMON.CANNOT_COLLECT,
    constants.COMMON.NOT_APPLICABLE,
    constants.COMMON.UNKNOWN,
    constants.COMMON.NOT_AVAILABLE,
}

AGE_UNITS = {
    constants.AGE.UNITS.DAYS,
    constants.AGE.UNITS.MONTHS,
    constants.AGE.UNITS.YEARS,
}


def try_wrap(func):
    """
    A decorator which catches exceptions and only runs func if input isn't None.
    """

    def inner(x):
        ret = True
        if x != NA:
            try:
                ret = func(x)
            except Exception:
                ret = False
        return ret

    return inner


check_age_days = (
    f"must be a number x such that {MIN_AGE_DAYS} <= x <= {MAX_AGE_DAYS}.",
    try_wrap(lambda x: (int(x) >= MIN_AGE_DAYS) and (int(x) <= MAX_AGE_DAYS)),
)


check_date = (
    "must be a valid representation of a Date or Datetime object.",
    try_wrap(lambda x: bool(parsedate(str(x)))),
)

check_positive = (
    "must be a number > 0.",
    try_wrap(lambda x: int(x) > 0),
)


check_non_negative = (
    "must be a number >= 0.",
    try_wrap(lambda x: int(x) >= 0),
)


INPUT_VALIDATION = {
    # PARTICIPANT
    CONCEPT.PARTICIPANT.RACE: {
        constants.RACE.WHITE,
        constants.RACE.NATIVE_AMERICAN,
        constants.RACE.BLACK,
        constants.RACE.ASIAN,
        constants.RACE.PACIFIC,
        constants.COMMON.OTHER,
        constants.RACE.MULTIPLE,
    },
    CONCEPT.PARTICIPANT.ENROLLMENT_AGE_DAYS: check_age_days,
    CONCEPT.PARTICIPANT.ENROLLMENT_AGE.VALUE: check_positive,
    CONCEPT.PARTICIPANT.ENROLLMENT_AGE.UNITS: AGE_UNITS,
    CONCEPT.PARTICIPANT.ETHNICITY: {
        constants.ETHNICITY.HISPANIC,
        constants.ETHNICITY.NON_HISPANIC,
    },
    CONCEPT.PARTICIPANT.GENDER: {
        constants.GENDER.MALE,
        constants.GENDER.FEMALE,
        constants.COMMON.OTHER,
    },
    CONCEPT.PARTICIPANT.SPECIES: {
        constants.SPECIES.DOG,
        constants.SPECIES.HUMAN,
    },
    CONCEPT.PARTICIPANT.CONSENT_TYPE: {
        constants.CONSENT_TYPE.DS_OBDR_MDS,
        constants.CONSENT_TYPE.DS_OBD_MDS,
        constants.CONSENT_TYPE.DS_OC_PUB_MDS,
        constants.CONSENT_TYPE.HMB_MDS,
        constants.CONSENT_TYPE.HMB_IRB,
        constants.CONSENT_TYPE.DS_CHD,
        constants.CONSENT_TYPE.DS_CHD_IRB,
        constants.CONSENT_TYPE.GRU,
    },
    CONCEPT.PARTICIPANT.LAST_CONTACT_AGE_DAYS: check_age_days,
    CONCEPT.PARTICIPANT.LAST_CONTACT_AGE.VALUE: check_positive,
    CONCEPT.PARTICIPANT.LAST_CONTACT_AGE.UNITS: AGE_UNITS,
    # BIOSPECIMEN
    CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS: check_age_days,
    CONCEPT.BIOSPECIMEN.EVENT_AGE.VALUE: check_positive,
    CONCEPT.BIOSPECIMEN.EVENT_AGE.UNITS: AGE_UNITS,
    CONCEPT.BIOSPECIMEN.ANALYTE: {
        constants.SEQUENCING.ANALYTE.DNA,
        constants.SEQUENCING.ANALYTE.RNA,
        constants.SEQUENCING.ANALYTE.VIRTUAL,
        constants.COMMON.OTHER,
    },
    CONCEPT.BIOSPECIMEN.CONCENTRATION_MG_PER_ML: check_positive,
    CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT: {
        constants.SPECIMEN.SAMPLE_PROCUREMENT.AUTOPSY,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.BIOPSY,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.SUBTOTAL_RESECTION,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.TOTAL_RESECTION,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.BLOOD_DRAW,
        constants.SPECIMEN.SAMPLE_PROCUREMENT.BONE_MARROW_ASPIRATION,
        constants.COMMON.OTHER,
    },
    CONCEPT.BIOSPECIMEN.SHIPMENT_DATE: check_date,
    CONCEPT.BIOSPECIMEN.VOLUME_UL: check_positive,
    # BIOSPECIMEN_DIAGNOSIS
    # BIOSPECIMEN_GENOMIC_FILE
    # DIAGNOSIS
    CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS: check_positive,
    CONCEPT.DIAGNOSIS.CATEGORY: {
        constants.STUDY.CANCER,
        constants.STUDY.STRUCTURAL_DEFECT,
        constants.COMMON.OTHER,
    },
    # FAMILY
    # There's an enum for FAMILY_TYPE in the dataservice, but nothing for this
    # exists in the ingest library.
    # FAMILY_RELATIONSHIP
    # GENOMIC_FILE
    CONCEPT.GENOMIC_FILE.AVAILABILITY: {
        constants.GENOMIC_FILE.AVAILABILITY.IMMEDIATE,
        constants.GENOMIC_FILE.AVAILABILITY.COLD_STORAGE,
    },
    CONCEPT.GENOMIC_FILE.DATA_TYPE: {
        constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS,
        constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX,
        constants.GENOMIC_FILE.DATA_TYPE.EXPRESSION,
        constants.GENOMIC_FILE.DATA_TYPE.GENE_EXPRESSION,
        constants.GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS,
        constants.GENOMIC_FILE.DATA_TYPE.GVCF,
        constants.GENOMIC_FILE.DATA_TYPE.GVCF_INDEX,
        constants.GENOMIC_FILE.DATA_TYPE.HISTOLOGY_IMAGES,
        constants.GENOMIC_FILE.DATA_TYPE.ISOFORM_EXPRESSION,
        constants.GENOMIC_FILE.DATA_TYPE.OPERATION_REPORTS,
        constants.COMMON.OTHER,
        constants.GENOMIC_FILE.DATA_TYPE.PATHOLOGY_REPORTS,
        constants.GENOMIC_FILE.DATA_TYPE.RADIOLOGY_IMAGES,
        constants.GENOMIC_FILE.DATA_TYPE.RADIOLOGY_REPORTS,
        constants.GENOMIC_FILE.DATA_TYPE.NUCLEOTIDE_VARIATION,
        constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_COPY_NUMBER_VARIATIONS,
        constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_STRUCTURAL_VARIATIONS,
        constants.GENOMIC_FILE.DATA_TYPE.UNALIGNED_READS,
        constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS,
        constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX,
        constants.GENOMIC_FILE.DATA_TYPE.QC_METRICS,
        constants.GENOMIC_FILE.DATA_TYPE.SPLICE_JUNCTION,
        constants.GENOMIC_FILE.DATA_TYPE.EXPRESSION_COUNTS,
        constants.GENOMIC_FILE.DATA_TYPE.STAR_SINGLE_CELL_COUNTS,
        constants.GENOMIC_FILE.DATA_TYPE.STAR_CELL_RANGER_COUNTS,
        constants.GENOMIC_FILE.DATA_TYPE.SINGLE_CELL_QC_METRICS,
    },
    CONCEPT.GENOMIC_FILE.SIZE: check_non_negative,
    # INVESTIGATOR
    # OUTCOME
    CONCEPT.OUTCOME.EVENT_AGE_DAYS: check_age_days,
    CONCEPT.OUTCOME.EVENT_AGE.VALUE: check_positive,
    CONCEPT.OUTCOME.EVENT_AGE.UNITS: AGE_UNITS,
    CONCEPT.OUTCOME.DISEASE_RELATED: {
        constants.OUTCOME.DISEASE_RELATED.YES,
        constants.OUTCOME.DISEASE_RELATED.NO,
    },
    CONCEPT.OUTCOME.VITAL_STATUS: {
        constants.OUTCOME.VITAL_STATUS.ALIVE,
        constants.OUTCOME.VITAL_STATUS.DEAD,
    },
    # PHENOTYPE
    CONCEPT.PHENOTYPE.EVENT_AGE_DAYS: check_age_days,
    CONCEPT.PHENOTYPE.EVENT_AGE.VALUE: check_positive,
    CONCEPT.PHENOTYPE.EVENT_AGE.UNITS: AGE_UNITS,
    CONCEPT.PHENOTYPE.OBSERVED: {
        constants.PHENOTYPE.OBSERVED.YES,
        constants.PHENOTYPE.OBSERVED.NO,
    },
    # READ_GROUP
    CONCEPT.READ_GROUP.PAIRED_END: {"1", "2"},
    CONCEPT.READ_GROUP.LANE_NUMBER: check_positive,
    CONCEPT.READ_GROUP.QUALITY_SCALE: {
        constants.READ_GROUP.QUALITY_SCALE.ILLUMINA13,
        constants.READ_GROUP.QUALITY_SCALE.ILLUMINA15,
        constants.READ_GROUP.QUALITY_SCALE.ILLUMINA18,
        constants.READ_GROUP.QUALITY_SCALE.SANGER,
        constants.READ_GROUP.QUALITY_SCALE.SOLEXA,
    },
    # READ_GROUP_GENOMIC_FILE
    # SEQUENCING_CENTER
    # SEQUENCING_EXPERIMENT
    CONCEPT.SEQUENCING.DATE: check_date,
    CONCEPT.SEQUENCING.STRATEGY: {
        constants.SEQUENCING.STRATEGY.TARGETED,
        constants.SEQUENCING.STRATEGY.WXS,
        constants.SEQUENCING.STRATEGY.WGS,
        constants.SEQUENCING.STRATEGY.RNA,
        constants.SEQUENCING.STRATEGY.MRNA,
        constants.SEQUENCING.STRATEGY.LINKED_WGS,
        constants.SEQUENCING.STRATEGY.CCS_RNA,
        constants.SEQUENCING.STRATEGY.CCS_WGS,
        constants.SEQUENCING.STRATEGY.CLR_RNA,
        constants.SEQUENCING.STRATEGY.CLR_WGS,
        constants.SEQUENCING.STRATEGY.ONT_WGS,
        constants.COMMON.OTHER,
    },
    # CONCEPT.SEQUENCING.LIBRARY_PREP - there is no concept for this
    # CONCEPT.SEQUENCING.LIBRARY_SELECTION - no concept for this either
    CONCEPT.SEQUENCING.LIBRARY_STRAND: {
        constants.SEQUENCING.STRAND.UNSTRANDED,
        constants.SEQUENCING.STRAND.FIRST,
        constants.SEQUENCING.STRAND.SECOND,
        constants.COMMON.OTHER,
    },
    CONCEPT.SEQUENCING.MAX_INSERT_SIZE: check_positive,
    CONCEPT.SEQUENCING.MEAN_DEPTH: check_positive,
    CONCEPT.SEQUENCING.MEAN_INSERT_SIZE: check_positive,
    CONCEPT.SEQUENCING.MEAN_READ_LENGTH: check_positive,
    CONCEPT.SEQUENCING.PAIRED_END: {"True", "False"},
    CONCEPT.SEQUENCING.PLATFORM: {
        constants.SEQUENCING.PLATFORM.ION_TORRENT,
        constants.SEQUENCING.PLATFORM.SOLID,
        constants.SEQUENCING.PLATFORM.ILLUMINA,
        constants.SEQUENCING.PLATFORM.LS454,
        constants.SEQUENCING.PLATFORM.GENOMICS,
        constants.SEQUENCING.PLATFORM.PACBIO,
        constants.COMMON.OTHER,
    },
    CONCEPT.SEQUENCING.TOTAL_READS: check_positive,
    # SEQUENCING_EXPERIMENT_GENOMIC_FILE
    # STUDY
    # STUDY_FILE
    CONCEPT.STUDY_FILE.AVAILABILITY: {
        constants.GENOMIC_FILE.AVAILABILITY.IMMEDIATE,
        constants.GENOMIC_FILE.AVAILABILITY.COLD_STORAGE,
    },
    # TASK
    # TASK_GENOMIC_FILE
}


def describe_set(x):
    all_x = x | NULL_VALUES
    return (
        f"must be one of {sorted(x)} or {sorted(NULL_VALUES)}",
        lambda x: x in all_x,
    )


for k, v in INPUT_VALIDATION.items():
    if isinstance(v, set):
        INPUT_VALIDATION[k] = describe_set(v)
