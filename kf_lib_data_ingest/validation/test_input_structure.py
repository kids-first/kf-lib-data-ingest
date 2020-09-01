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

def null_wrapper(values):
    return lambda x: (x in values) or (x in NULL_VALUES)

check_age = lambda x: (int(x) >= MIN_AGE_DAYS) and (int(x) < MAX_AGE_DAYS)

def check_date(x):
    try:
        value = parser.parse(str(value)) if value else None
    except ValueError as e:
        return False
    return True


check_positive = lambda x: int(x) > 0
check_non_negative = lambda x: int(x) >= 0


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
    CONCEPT.BIOSPECIMEN.VOLUME_UL: check_positive,

    # BIOSPECIMEN_DIAGNOSIS

    # BIOSPECIMEN_GENOMIC_FILE

    # DIAGNOSIS
    CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS: check_positive,
    CONCEPT.DIAGNOSIS.CATEGORY: lambda x: x in {
        constants.STUDY.CANCER,
        constants.STUDY.STRUCTURAL_DEFECT,
        constants.COMMON.OTHER
    },

    # FAMILY

    # FAMILY_RELATIONSHIP

    # GENOMIC_FILE
    CONCEPT.GENOMIC_FILE.AVAILABILITY: lambda x:
        (x in {
            constants.GENOMIC_FILE.AVAILABILITY.IMMEDIATE,
            constants.GENOMIC_FILE.AVAILABILITY.COLD_STORAGE
        }) or
        (x in NULL_VALUES),
    CONCEPT.GENOMIC_FILE.DATA_TYPE: lambda x: (x in {
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
        constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX
    }) or (x in NULL_VALUES),
    CONCEPT.SEQUENCING.PAIRED_END: lambda x: (x in {1, 2}) or (x in NULL_VALUES),
    CONCEPT.GENOMIC_FILE.SIZE: check_non_negative,

    # INVESTIGATOR

    # OUTCOME
    CONCEPT.OUTCOME.EVENT_AGE_DAYS: check_age,
    CONCEPT.OUTCOME.DISEASE_RELATED: lambda x: (x in {
        constants.OUTCOME.DISEASE_RELATED.YES,
        constants.OUTCOME.DISEASE_RELATED.NO
    }) or (x in NULL_VALUES),
    CONCEPT.OUTCOME.VITAL_STATUS: lambda x: (x in {
        constants.OUTCOME.VITAL_STATUS.ALIVE,
        constants.OUTCOME.VITAL_STATUS.DEAD
    }) or (x in NULL_VALUES),

    # PHENOTYPE
    CONCEPT.PHENOTYPE.EVENT_AGE_DAYS: check_age,
    CONCEPT.PHENOTYPE.OBSERVED: lambda x: (x in {
        constants.PHENOTYPE.OBSERVED.YES,
        constants.PHENOTYPE.OBSERVED.NO
    }) or (x in NULL_VALUES),

    # READ_GROUP
    CONCEPT.READ_GROUP.LANE_NUMBER: check_positive,
    CONCEPT.READ_GROUP.QUALITY_SCALE: lambda x: (x in {
        constants.READ_GROUP.QUALITY_SCALE.ILLUMINA13,
        constants.READ_GROUP.QUALITY_SCALE.ILLUMINA15,
        constants.READ_GROUP.QUALITY_SCALE.ILLUMINA18,
        constants.READ_GROUP.QUALITY_SCALE.SANGER,
        constants.READ_GROUP.QUALITY_SCALE.SOLEXA
    }) or (x in NULL_VALUES),

    # READ_GROUP_GENOMIC_FILE

    # SEQUENCING_CENTER

    # SEQUENCING_EXPERIMENT
    CONCEPT.SEQUENCING.DATE: check_date,
    CONCEPT.SEQUENCING.STRATEGY: lambda x: (x in {
        constants.SEQUENCING.STRATEGY.TARGETED,
        constants.SEQUENCING.STRATEGY.WXS,
        constants.SEQUENCING.STRATEGY.WGS,
        constants.SEQUENCING.STRATEGY.RNA,
        constants.SEQUENCING.STRATEGY.MRNA,
        constants.SEQUENCING.STRATEGY.LINKED_WGS
    }) or (x in NULL_VALUES),
    #TODO: CONCEPT.SEQUENCING.LIBRARY_PREP
    #TODO: CONCEPT.SEQUENCING.LIBRARY_SELECTION
    CONCEPT.SEQUENCING.LIBRARY_STRAND: lambda x: (x in {
        constants.SEQUENCING.STRAND.UNSTRANDED,
        constants.SEQUENCING.STRAND.FIRST,
        constants.SEQUENCING.STRAND.SECOND,
        constants.COMMON.OTHER
    }) or (x in NULL_VALUES),
    CONCEPT.SEQUENCING.MAX_INSERT_SIZE: check_positive,
    CONCEPT.SEQUENCING.MEAN_DEPTH: check_positive,
    CONCEPT.SEQUENCING.MEAN_INSERT_SIZE: check_positive,
    CONCEPT.SEQUENCING.MEAN_READ_LENGTH: check_positive,
    CONCEPT.SEQUENCING.PLATFORM: lambda x: (x in {
        constants.SEQUENCING.PLATFORM.ION_TORRENT,
        constants.SEQUENCING.PLATFORM.SOLID,
        constants.SEQUENCING.PLATFORM.ILLUMINA,
        constants.SEQUENCING.PLATFORM.LS454,
        constants.SEQUENCING.PLATFORM.GENOMICS,
        constants.SEQUENCING.PLATFORM.PACBIO,
        constants.COMMON.OTHER
    }) or (x in NULL_VALUES),
    CONCEPT.SEQUENCING.TOTAL_READS: check_positive,

    # SEQUENCING_EXPERIMENT_GENOMIC_FILE

    # STUDY
    CONCEPT.STUDY.RELEASE_STATUS: lambda x: (x in {
        constants.STUDY.STATUS.FAILED,
        constants.STUDY.STATUS.PENDING,
        constants.STUDY.STATUS.PUBLISHING,
        constants.STUDY.STATUS.CANCELED,
        constants.STUDY.STATUS.STAGED,
        constants.STUDY.STATUS.RUNNING,
        constants.STUDY.STATUS.PUBLISHED,
        constants.STUDY.STATUS.WAITING
    }) or (x in NULL_VALUES),

    # STUDY_FILE
    CONCEPT.STUDY_FILE.AVAILABILITY: lambda x: (x in {
        constants.GENOMIC_FILE.AVAILABILITY.IMMEDIATE,
        constants.GENOMIC_FILE.AVAILABILITY.COLD_STORAGE
    }) or (x in NULL_VALUES),
    
    # TASK
    
    # TASK_GENOMIC_FILE

}



