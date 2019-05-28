"""
When you want to use free text for values, don't.
Use preset constants instead.
"""


class COMMON:
    NOT_APPLICABLE = 'Not Applicable'
    NOT_AVAILABLE = 'Not Available'
    NOT_REPORTED = 'Not Reported'
    UNKNOWN = 'Reported Unknown'
    CANNOT_COLLECT = 'Not Allowed To Collect'
    NO_MATCH = 'No Match'
    OTHER = 'Other'
    MULTIPLE = "Multiple"


class FILE:
    class HASH:
        MD5 = 'MD5'
        SHA1 = 'SHA-1'
        SHA256 = 'SHA-256'
        SHA512 = 'SHA-512'
        S3_ETAG = 'S3 ETag'


class SPECIMEN:
    class COMPOSITION:
        XENOGRAFT = 'Xenograft Tissue'
        EBVI = 'Epstein-Barr Virus Immortalized Cells'
        TISSUE = 'Solid Tissue'
        BLOOD = 'Blood'
        PERIPHERAL_BLOOD = 'Peripheral Blood'
        BUCCAL_SWAB = 'Buccal Cells'
        BONE_MARROW = 'Bone Marrow'
        SALIVA = 'Saliva'
        LINES = 'Cell Lines'
        BONE = "Bone"

    class TISSUE_TYPE:
        TUMOR = 'Tumor'
        GERMLINE = 'Normal'

    class ANATOMY_SITE:
        SKULL = 'Skull'


class GENOMIC_FILE:
    class AVAILABILITY:
        IMMEDIATE = 'Immediate Download'
        COLD_STORAGE = 'In cold storage'

    class FORMAT:
        FASTQ = 'fastq'
        BAM = 'bam'
        CRAM = 'cram'
        BAI = 'bai'
        CRAI = 'crai'
        GVCF = 'gvcf'
        TBI = 'tbi'
        VCF = 'vcf'

    class DATA_TYPE:
        UNALIGNED_READS = 'Unaligned Reads'
        ALIGNED_READS = 'Aligned Reads'
        ALIGNED_READS_INDEX = 'Aligned Reads Index'
        GVCF = 'gVCF'
        GVCF_INDEX = 'gVCF Index'
        VARIANT_CALLS = 'Variant Calls'
        VARIANT_CALLS_INDEX = 'Variant Calls Index'


class SEQUENCING:
    class REFERENCE_GENOME:
        GRCH38 = 'GRCh38'
        GRCH37 = 'GRCh37'
        HS37D5 = 'hs37d5'
        HG19 = 'hg19'

    class PLATFORM:
        ILLUMINA = 'Illumina'

    class INSTRUMENT:
        HISEQ_X_v2_5 = 'HiSeq X v2.5'
        HISEQ_X_10 = 'HiSeq X Ten'

    class STRAND:
        UNSTRANDED = 'Unstranded'
        FIRST = 'First Stranded'
        SECOND = 'Second Stranded'
        OTHER = 'Other'

    class CENTER:
        class WASHU:
            NAME = 'Washington University'
            KF_ID = 'SC_K52V7463'

        class BROAD:
            NAME = 'Broad Institute'
            KF_ID = 'SC_DGDDQVYR'

        class HUDSON_ALPHA:
            NAME = 'HudsonAlpha Institute for Biotechnology'
            KF_ID = 'SC_X1N69WJM'

        class BAYLOR:
            NAME = 'Baylor College of Medicine'
            KF_ID = 'SC_A1JNZAZH'

        class ST_JUDE:
            NAME = 'St Jude'
            KF_ID = 'SC_1K3QGW4V'

    class STRATEGY:
        WGS = 'WGS'

    class ANALYTE:
        DNA = 'DNA'
        RNA = 'RNA'


class STUDY:
    CANCER = 'Cancer'
    STRUCTURAL_DEFECT = 'Structural Birth Defect'

    class STATUS:
        PENDING = 'Pending'


class CONSENT_TYPE:
    DS_OBDR_MDS = 'DS-OBDR-MDS'
    # Disease-Specific (Orofacial birth defects and related phenotypes,
    # MDS) (DS-OBDR-MDS) - Use of the data must be related to Orofacial
    # birth defects and related phenotypes. Use of the data includes
    # methods development research (e.g., development of software or
    # algorithms). Includes related diseases and phenotypes such as medical
    # and dental data, speech characteristics, images and derived data
    # (e.g. 3D facial measures, ultrasounds of the upper lip), and all
    # other physical assessments taken on the study subjects.
    DS_OBD_MDS = 'DS-OBD-MDS'
    # Disease-Specific (Orofacial birth defects, MDS) (DS-OBD-MDS) - Use of
    # the data must be related to Orofacial birth defects. Use of the data
    # includes methods development research (e.g., development of software
    # or algorithms).
    DS_OC_PUB_MDS = 'DS-OC-PUB-MDS'
    # Disease-Specific (Oral Clefts, PUB, MDS) (DS-OC-PUB-MDS) - Use of the
    # data must be related to Oral Clefts. Requestor agrees to make results
    # of studies using the data available to the larger scientific
    # community. Use of the data includes methods development research
    # (e.g., development of software or algorithms).
    HMB_MDS = 'HMB-MDS'
    # Health/Medical/Biomedical (MDS) (HMB-MDS) - Use of this data is
    # limited to health/medical/biomedical purposes, does not include the
    # study of population origins or ancestry. Use of the data includes
    # methods development research (e.g., development of software or
    # algorithms).
    HMB_IRB = 'HMB-IRB'
    # Health/Medical/Biomedical general research
    DS_CHD_IRB = 'DS-CHD-IRB'
    # Disease specific Cardiac Heart Defect
    DS_CHD = "DS-CHD"
    # Disease specific Cardiac Heart Defect
    GRU = 'GRU'
    # General Research Use


class AUTHORITY:
    DBGAP = 'dbGaP'


class VITAL_STATUS:
    DEAD = 'Deceased'
    ALIVE = 'Alive'


class RELATIONSHIP:
    PROBAND = "Proband"
    HUSBAND = "Husband"
    WIFE = "Wife"
    SPOUSE = "Spouse"
    MOTHER = "Mother"
    FATHER = "Father"
    PARENT = "Parent"
    BROTHER = "Brother"
    SISTER = "Sister"
    TWIN_BROTHER = "Twin Brother"
    TWIN_SISTER = "Twin Sister"
    TWIN = "Twin"
    SIBLING = "Sibling"
    DAUGHTER = "Daughter"
    SON = "Son"
    CHILD = "Child"
    MATERNAL_GRANDMOTHER = "Maternal Grandmother"
    MATERNAL_GRANDFATHER = "Maternal Grandfather"
    MATERNAL_GRANDPARENT = "Maternal Grandparent"
    PATERNAL_GRANDMOTHER = "Paternal Grandmother"
    PATERNAL_GRANDFATHER = "Paternal Grandfather"
    PATERNAL_GRANDPARENT = "Paternal Grandparent"
    GRANDMOTHER = "Grandmother"
    GRANDFATHER = "Grandfather"
    GRANDPARENT = "Grandparent"
    MATERNAL_GRANDDAUGHTER = "Maternal Granddaughter"
    MATERNAL_GRANDSON = "Maternal Grandson"
    MATERNAL_GRANDCHILD = "Maternal Grandchild"
    PATERNAL_GRANDDAUGHTER = "Paternal Granddaughter"
    PATERNAL_GRANDSON = "Paternal Grandson"
    PATERNAL_GRANDCHILD = "Paternal Grandchild"
    GRANDDAUGHTER = "Granddaughter"
    GRANDSON = "Grandson"
    GRANDCHILD = "Grandchild"


REVERSE_RELATIONSHIPS = {
    RELATIONSHIP.HUSBAND: RELATIONSHIP.SPOUSE,
    RELATIONSHIP.WIFE: RELATIONSHIP.SPOUSE,
    RELATIONSHIP.SPOUSE: RELATIONSHIP.SPOUSE,
    RELATIONSHIP.MOTHER: RELATIONSHIP.CHILD,
    RELATIONSHIP.FATHER: RELATIONSHIP.CHILD,
    RELATIONSHIP.PARENT: RELATIONSHIP.CHILD,
    RELATIONSHIP.BROTHER: RELATIONSHIP.SIBLING,
    RELATIONSHIP.SISTER: RELATIONSHIP.SIBLING,
    RELATIONSHIP.SIBLING: RELATIONSHIP.SIBLING,
    RELATIONSHIP.TWIN_BROTHER: RELATIONSHIP.TWIN,
    RELATIONSHIP.TWIN_SISTER: RELATIONSHIP.TWIN,
    RELATIONSHIP.TWIN: RELATIONSHIP.TWIN,
    RELATIONSHIP.DAUGHTER: RELATIONSHIP.PARENT,
    RELATIONSHIP.SON: RELATIONSHIP.PARENT,
    RELATIONSHIP.CHILD: RELATIONSHIP.PARENT,
    RELATIONSHIP.MATERNAL_GRANDMOTHER: RELATIONSHIP.MATERNAL_GRANDCHILD,
    RELATIONSHIP.MATERNAL_GRANDFATHER: RELATIONSHIP.MATERNAL_GRANDCHILD,
    RELATIONSHIP.MATERNAL_GRANDPARENT: RELATIONSHIP.MATERNAL_GRANDCHILD,
    RELATIONSHIP.PATERNAL_GRANDMOTHER: RELATIONSHIP.PATERNAL_GRANDCHILD,
    RELATIONSHIP.PATERNAL_GRANDFATHER: RELATIONSHIP.PATERNAL_GRANDCHILD,
    RELATIONSHIP.PATERNAL_GRANDPARENT: RELATIONSHIP.PATERNAL_GRANDCHILD,
    RELATIONSHIP.GRANDMOTHER: RELATIONSHIP.GRANDCHILD,
    RELATIONSHIP.GRANDFATHER: RELATIONSHIP.GRANDCHILD,
    RELATIONSHIP.GRANDPARENT: RELATIONSHIP.GRANDCHILD,
    RELATIONSHIP.MATERNAL_GRANDDAUGHTER: RELATIONSHIP.MATERNAL_GRANDPARENT,
    RELATIONSHIP.MATERNAL_GRANDSON: RELATIONSHIP.MATERNAL_GRANDPARENT,
    RELATIONSHIP.MATERNAL_GRANDCHILD: RELATIONSHIP.MATERNAL_GRANDPARENT,
    RELATIONSHIP.PATERNAL_GRANDDAUGHTER: RELATIONSHIP.PATERNAL_GRANDPARENT,
    RELATIONSHIP.PATERNAL_GRANDSON: RELATIONSHIP.PATERNAL_GRANDPARENT,
    RELATIONSHIP.PATERNAL_GRANDCHILD: RELATIONSHIP.PATERNAL_GRANDPARENT,
    RELATIONSHIP.GRANDDAUGHTER: RELATIONSHIP.GRANDPARENT,
    RELATIONSHIP.GRANDSON: RELATIONSHIP.GRANDPARENT,
    RELATIONSHIP.GRANDCHILD: RELATIONSHIP.GRANDPARENT
}


class GENDER:
    MALE = 'Male'
    FEMALE = 'Female'


RELATIONSHIP_GENDERS = {
    RELATIONSHIP.SPOUSE: {
        GENDER.MALE: RELATIONSHIP.HUSBAND,
        GENDER.FEMALE: RELATIONSHIP.WIFE
    },
    RELATIONSHIP.CHILD: {
        GENDER.MALE: RELATIONSHIP.SON,
        GENDER.FEMALE: RELATIONSHIP.DAUGHTER
    },
    RELATIONSHIP.SIBLING: {
        GENDER.MALE: RELATIONSHIP.BROTHER,
        GENDER.FEMALE: RELATIONSHIP.SISTER
    },
    RELATIONSHIP.TWIN: {
        GENDER.MALE: RELATIONSHIP.TWIN_BROTHER,
        GENDER.FEMALE: RELATIONSHIP.TWIN_SISTER
    },
    RELATIONSHIP.PARENT: {
        GENDER.MALE: RELATIONSHIP.FATHER,
        GENDER.FEMALE: RELATIONSHIP.MOTHER
    },
    RELATIONSHIP.MATERNAL_GRANDCHILD: {
        GENDER.MALE: RELATIONSHIP.MATERNAL_GRANDSON,
        GENDER.FEMALE: RELATIONSHIP.MATERNAL_GRANDDAUGHTER
    },
    RELATIONSHIP.PATERNAL_GRANDCHILD: {
        GENDER.MALE: RELATIONSHIP.PATERNAL_GRANDSON,
        GENDER.FEMALE: RELATIONSHIP.PATERNAL_GRANDDAUGHTER
    },
    RELATIONSHIP.GRANDCHILD: {
        GENDER.MALE: RELATIONSHIP.GRANDSON,
        GENDER.FEMALE: RELATIONSHIP.GRANDDAUGHTER
    },
    RELATIONSHIP.MATERNAL_GRANDPARENT: {
        GENDER.MALE: RELATIONSHIP.MATERNAL_GRANDFATHER,
        GENDER.FEMALE: RELATIONSHIP.MATERNAL_GRANDMOTHER
    },
    RELATIONSHIP.PATERNAL_GRANDPARENT: {
        GENDER.MALE: RELATIONSHIP.PATERNAL_GRANDFATHER,
        GENDER.FEMALE: RELATIONSHIP.PATERNAL_GRANDMOTHER
    },
    RELATIONSHIP.GRANDPARENT: {
        GENDER.MALE: RELATIONSHIP.GRANDFATHER,
        GENDER.FEMALE: RELATIONSHIP. GRANDMOTHER
    }
}


class RACE:
    WHITE = 'White'
    NATIVE_AMERICAN = 'American Indian or Alaska Native'
    BLACK = 'Black or African American'
    ASIAN = 'Asian'
    PACIFIC = 'Native Hawaiian or Other Pacific Islander'
    MULTIPLE = 'More Than One Race'


class ETHNICITY:
    NON_HISPANIC = 'Not Hispanic or Latino'
    HISPANIC = 'Hispanic or Latino'


class PHENOTYPE:
    class OBSERVED:
        YES = 'Positive'
        NO = 'Negative'
