"""
When you want to use free text for values, don't.
Use preset constants instead.
"""


class COMMON:
    NOT_APPLICABLE = "Not Applicable"
    NOT_AVAILABLE = "Not Available"
    NOT_REPORTED = "Not Reported"
    UNKNOWN = "Reported Unknown"
    CANNOT_COLLECT = "Not Allowed To Collect"
    NO_MATCH = "No Match"
    OTHER = "Other"


class SPECIMEN:
    class COMPOSITION:
        TISSUE = "Solid Tissue"
        BLOOD = "Blood"
        BUCCAL_SWAB = "Buccal Swab"
        BONE_MARROW = "Bone Marrow"

    class TISSUE_TYPE:
        TUMOR = "Tumor Tissue"
        GERMLINE = "Germline DNA"


class GENOMIC_FILE:
    class AVAILABILITY:
        IMMEDIATE = "Immediate Download"
        COLD_STORAGE = "In cold storage"


class SEQUENCING:
    class REFERENCE_GENOME:
        GRCH38 = "GRCh38"

    class PLATFORM:
        ILLUMINA = "Illumina"

    class INSTRUMENT:
        HISEQ_X_v2_5 = "HiSeq X v2.5"

    class STRAND:
        UNSTRANDED = 'Unstranded'
        FIRST = 'First Stranded'
        SECOND = 'Second Stranded'
        OTHER = 'Other'

    class CENTER:
        class WASHU:
            NAME = "Washington University"
            KF_ID = "SC_K52V7463"

        class BROAD:
            NAME = "Broad Institute"
            KF_ID = "SC_DGDDQVYR"

        class HUDSON_ALPHA:
            NAME = "HudsonAlpha Institute for Biotechnology"
            KF_ID = "SC_X1N69WJM"

        class BAYLOR:
            NAME = "Baylor College of Medicine"
            KF_ID = "SC_A1JNZAZH"

        class ST_JUDE:
            NAME = "St Jude"
            KF_ID = "SC_1K3QGW4V"

    class STRATEGY:
        WGS = "WGS"

    class ANALYTE:
        DNA = "DNA"
        RNA = "RNA"


class STUDY:
    CANCER = "Cancer"
    STRUCTURAL_DEFECT = "Structural Birth Defect"

    class STATUS:
        PENDING = "Pending"


class CONSENT_TYPE:
    DS_OBDR_MDS = "DS-OBDR-MDS"
    # Disease-Specific (Orofacial birth defects and related phenotypes,
    # MDS) (DS-OBDR-MDS) - Use of the data must be related to Orofacial
    # birth defects and related phenotypes. Use of the data includes
    # methods development research (e.g., development of software or
    # algorithms). Includes related diseases and phenotypes such as medical
    # and dental data, speech characteristics, images and derived data
    # (e.g. 3D facial measures, ultrasounds of the upper lip), and all
    # other physical assessments taken on the study subjects.
    DS_OBD_MDS = "DS-OBD-MDS"
    # Disease-Specific (Orofacial birth defects, MDS) (DS-OBD-MDS) - Use of
    # the data must be related to Orofacial birth defects. Use of the data
    # includes methods development research (e.g., development of software
    # or algorithms).
    DS_OC_PUB_MDS = "DS-OC-PUB-MDS"
    # Disease-Specific (Oral Clefts, PUB, MDS) (DS-OC-PUB-MDS) - Use of the
    # data must be related to Oral Clefts. Requestor agrees to make results
    # of studies using the data available to the larger scientific
    # community. Use of the data includes methods development research
    # (e.g., development of software or algorithms).
    HMB_MDS = "HMB-MDS"
    # Health/Medical/Biomedical (MDS) (HMB-MDS) - Use of this data is
    # limited to health/medical/biomedical purposes, does not include the
    # study of population origins or ancestry. Use of the data includes
    # methods development research (e.g., development of software or
    # algorithms).
    HMB_IRB = "HMB-IRB"
    # Health/Medical/Biomedical general research
    DS_CHD_IRB = "DS-CHD-IRB"
    # Disease specific Cardiac Heart Defect
    GRU = "GRU"
    # General Research Use


class AUTHORITY:
    DBGAP = "dbGaP"


class VITAL_STATUS:
    DEAD = "Dead"
    ALIVE = "Alive"


class RELATIONSHIP:
    MOTHER = "Mother"
    FATHER = "Father"
    SIBLING = "Sibling"
    CHILD = "Child"


class GENDER:
    MALE = "Male"
    FEMALE = "Female"


class RACE:
    WHITE = "White"
    NATIVE_AMERICAN = "American Indian or Alaska Native"
    BLACK = "Black or African American"
    ASIAN = "Asian"
    PACIFIC = "Native Hawaiian or Other Pacific Islander"
    MULTIPLE = "More Than One Race"


class ETHNICITY:
    NON_HISPANIC = "Not Hispanic or Latino"
    HISPANIC = "Hispanic or Latino"


class PHENOTYPE:
    class OBSERVED:
        YES = "Positive"
        NO = "Negative"
