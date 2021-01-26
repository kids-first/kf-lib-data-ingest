"""
When you want to use free text for values, don't. Use preset constants instead.

All constants should be strings. The Extract stage converts all data to
strings, and we want to maintain the ability to compare with constants during
Transform.
"""


class COMMON:
    CANNOT_COLLECT = "Not Allowed To Collect"
    MULTIPLE = "Multiple"
    NOT_APPLICABLE = "Not Applicable"
    NOT_AVAILABLE = "Not Available"
    NOT_REPORTED = "Not Reported"
    NO_MATCH = "No Match"
    OTHER = "Other"
    UNKNOWN = "Reported Unknown"
    TRUE = "True"
    FALSE = "False"


class AGE:
    class UNITS:
        DAYS = "Days"
        MONTHS = "Months"
        YEARS = "Years"


class FILE:
    class HASH:
        MD5 = "MD5"
        SHA1 = "SHA-1"
        SHA256 = "SHA-256"
        SHA512 = "SHA-512"
        S3_ETAG = "ETag"


class SPECIMEN:
    class COMPOSITION:
        BLOOD = "Peripheral Whole Blood"
        BONE = "Bone"
        BONE_MARROW = "Bone Marrow"
        BUCCAL_SWAB = "Buccal Cells"
        EBVI = "Epstein-Barr Virus Immortalized Cells"
        FIBROBLASTS = "Fibroblasts"
        LINE = "Derived Cell Line"
        LYMPHOCYTES = "Lymphocytes"
        MNC = "Mononuclear Cells"
        PLASMA = "Plasma"
        SALIVA = "Saliva"
        TISSUE = "Solid Tissue"
        XENOGRAFT = "Xenograft Tissue"

    class SAMPLE_PROCUREMENT:
        AUTOPSY = "Autopsy"
        BIOPSY = "Biopsy"
        BLOOD_DRAW = "Blood Draw"
        SUBTOTAL_RESECTION = "Subtotal Resection"
        TOTAL_RESECTION = "Gross Total Resection"
        BONE_MARROW_ASPIRATION = "Bone Marrow Aspiration"

    class ANATOMY_SITE:
        BONE_MARROW = "Bone Marrow"
        HAIR = "Hair"
        SKULL = "Skull"
        CNS = "Central Nervous System"

    class TISSUE_TYPE:
        GERMLINE = "Normal"
        NORMAL = "Normal"
        TUMOR = "Tumor"


class GENOMIC_FILE:
    class AVAILABILITY:
        IMMEDIATE = "Immediate Download"
        COLD_STORAGE = "Cold Storage"

    class DATA_TYPE:
        ALIGNED_READS = "Aligned Reads"
        ALIGNED_READS_INDEX = "Aligned Reads Index"
        EXPRESSION = "Expression"
        GVCF = "gVCF"
        GVCF_INDEX = "gVCF Index"
        HISTOLOGY_IMAGES = "Histology Images"
        NUCLEOTIDE_VARIATION = "Simple Nucleotide Variations"
        OPERATION_REPORTS = "Operation Reports"
        PATHOLOGY_REPORTS = "Pathology Reports"
        RADIOLOGY_IMAGES = "Radiology Images"
        RADIOLOGY_REPORTS = "Radiology Reports"
        UNALIGNED_READS = "Unaligned Reads"
        VARIANT_CALLS = "Variant Calls"
        VARIANT_CALLS_INDEX = "Variant Calls Index"
        ANNOTATED_SOMATIC_MUTATIONS = "Annotated Somatic Mutations"
        GENE_EXPRESSION = "Gene Expression"
        GENE_FUSIONS = "Gene Fusions"
        ISOFORM_EXPRESSION = "Isoform Expression"
        SOMATIC_COPY_NUMBER_VARIATIONS = "Somatic Copy Number Variations"
        SOMATIC_STRUCTURAL_VARIATIONS = "Somatic Structural Variations"

    class FORMAT:
        BAI = "bai"
        BAM = "bam"
        CRAI = "crai"
        CRAM = "cram"
        DCM = "dcm"
        FASTQ = "fastq"
        GPR = "gpr"
        GVCF = "gvcf"
        IDAT = "idat"
        PDF = "pdf"
        SVS = "svs"
        TBI = "tbi"
        VCF = "vcf"
        HTML = "html"
        MAF = "maf"


class READ_GROUP:
    class QUALITY_SCALE:
        ILLUMINA13 = "Illumina13"
        ILLUMINA15 = "Illumina15"
        ILLUMINA18 = "Illumina18"
        SANGER = "Sanger"
        SOLEXA = "Solexa"


class SEQUENCING:
    class REFERENCE_GENOME:
        GRCH38 = "GRCh38"
        GRCH37 = "GRCh37"
        HS37D5 = "hs37d5"
        HG19 = "hg19"

    class PLATFORM:
        GENOMICS = "Complete Genomics"
        ILLUMINA = "Illumina"
        ION_TORRENT = "Ion Torrent"
        LS454 = "LS454"
        PACBIO = "PacBio"
        SOLID = "SOLiD"

    class INSTRUMENT:
        HISEQ_X_v2_5 = "HiSeq X v2.5"
        HISEQ_X_10 = "HiSeq X Ten"

    class STRAND:
        FIRST = "First Stranded"
        SECOND = "Second Stranded"
        UNSTRANDED = "Unstranded"

    class CENTER:
        class ASHION:
            NAME = "Ashion"
            KF_ID = "SC_0CNMF82N"

        class BAYLOR:
            NAME = "Baylor College of Medicine"
            KF_ID = "SC_A1JNZAZH"

        class BC_CANCER_AGENCY:
            NAME = "British Columbia Cancer Agency Genome Sciences Centre"
            KF_ID = "SC_FN7NH453"

        class BGI:
            NAME = "BGI@CHOP Genome Center"
            KF_ID = "SC_WWEQ9HFY"

        class BGI_CHINA:
            NAME = "BGI"
            KF_ID = "SC_FAD4KCQG"

        class BROAD:
            NAME = "Broad Institute"
            KF_ID = "SC_DGDDQVYR"

        class CBTN_UNSEQUENCED:
            NAME = "CBTN Unsequenced"
            KF_ID = "SC_31369RXZ"

        class CHOP:
            NAME = "CHOP"
            KF_ID = "SC_9NSC532X"

        class CHOP_DGD:
            NAME = "CHOP DGD"
            KF_ID = "SC_ZZPPF973"

        class COMPLETE_GENOMICS:
            NAME = "Complete Genomics"
            KF_ID = "SC_D30SEWS4"

        class FELINE_DIAGNOSTICS:
            NAME = "Feline Diagnostics LLC"
            KF_ID = "SC_CATTVETT"

        class FG:
            NAME = "fg"
            KF_ID = "SC_XXXXXXX2"

        class HUDSON_ALPHA:
            NAME = "HudsonAlpha Institute for Biotechnology"
            KF_ID = "SC_X1N69WJM"

        class NANT:
            NAME = "NantOmics"
            KF_ID = "SC_N1EVHSME"

        class NOVOGENE:
            NAME = "Novogene"
            KF_ID = "SC_2ZBAMKK0"

        class NCI:
            NAME = "National Cancer Institute, Khan Lab"
            KF_ID = "SC_F6RZ51K9"

        class SICKKIDS:
            NAME = "SickKids"
            KF_ID = "SC_9WMJKQ1X"

        class SIDRA:
            NAME = "Genomic Clinical Core at Sidra Medical and Research Center"
            KF_ID = "SC_KE2ASNJM"

        class ST_JUDE:
            NAME = "St Jude"
            KF_ID = "SC_1K3QGW4V"

        class TEMP:
            NAME = "TEMP"
            KF_ID = "SC_SJJ0B9GN"

        class TGEN:
            NAME = "The Translational Genomics Research Institute"
            KF_ID = "SC_KQ9JZG3P"

        class UNKNOWN_CHRIS_JONES:
            NAME = "UNKNOWN:CHRIS_JONES"
            KF_ID = "SC_5A2B1T4K"

        class VIRTUAL:
            NAME = "Virtual"
            KF_ID = "SC_BATJDPHB"

        class WASHU:
            NAME = "Washington University"
            KF_ID = "SC_K52V7463"

        class YALE:
            NAME = "Yale"
            KF_ID = "SC_31W52VNX"

        class FREDHUTCH:
            NAME = "Fred Hutchinson Cancer Research Center"
            KF_ID = "SC_8JXH42X1"

    class STRATEGY:
        LINKED_WGS = "Linked-Read WGS (10x Chromium)"
        MRNA = "miRNA-Seq"
        RNA = "RNA-Seq"
        WGS = "WGS"
        WXS = "WXS"
        TARGETED = "Targeted Sequencing"

    class ANALYTE:
        DNA = "DNA"
        RNA = "RNA"
        VIRTUAL = "Virtual"


class STUDY:
    CANCER = "Cancer"
    STRUCTURAL_DEFECT = "Structural Birth Defect"
    STRUCTURAL_DEFECT_AND_CANCER = "Structural Birth Defect and Cancer"

    class STATUS:
        CANCELED = "Canceled"
        FAILED = "Failed"
        PENDING = "Pending"
        PUBLISHED = "Published"
        PUBLISHING = "Publishing"
        RUNNING = "Running"
        STAGED = "Staged"
        WAITING = "Waiting"


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
    DS_CHD = "DS-CHD"
    # Disease specific Cardiac Heart Defect
    HMB_NPU = "HMB-NPU"
    # Health/Medical/Biomedical (NPU) (HMB-NPU) - Use of this data is
    # limited to health/medical/biomedical purposes, does not include the
    # study of population origins or ancestry. Use of the data is limited to
    # not-for-profit organizations.
    GRU = "GRU"
    # General Research Use


class AUTHORITY:
    DBGAP = "dbGaP"


class OUTCOME:
    class DISEASE_RELATED:
        YES = "Yes"
        NO = "No"

    class VITAL_STATUS:
        ALIVE = "Alive"
        DEAD = "Deceased"


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
    RELATIONSHIP.GRANDCHILD: RELATIONSHIP.GRANDPARENT,
}


class GENDER:
    MALE = "Male"
    FEMALE = "Female"


RELATIONSHIP_GENDERS = {
    RELATIONSHIP.SPOUSE: {
        GENDER.MALE: RELATIONSHIP.HUSBAND,
        GENDER.FEMALE: RELATIONSHIP.WIFE,
    },
    RELATIONSHIP.CHILD: {
        GENDER.MALE: RELATIONSHIP.SON,
        GENDER.FEMALE: RELATIONSHIP.DAUGHTER,
    },
    RELATIONSHIP.SIBLING: {
        GENDER.MALE: RELATIONSHIP.BROTHER,
        GENDER.FEMALE: RELATIONSHIP.SISTER,
    },
    RELATIONSHIP.TWIN: {
        GENDER.MALE: RELATIONSHIP.TWIN_BROTHER,
        GENDER.FEMALE: RELATIONSHIP.TWIN_SISTER,
    },
    RELATIONSHIP.PARENT: {
        GENDER.MALE: RELATIONSHIP.FATHER,
        GENDER.FEMALE: RELATIONSHIP.MOTHER,
    },
    RELATIONSHIP.MATERNAL_GRANDCHILD: {
        GENDER.MALE: RELATIONSHIP.MATERNAL_GRANDSON,
        GENDER.FEMALE: RELATIONSHIP.MATERNAL_GRANDDAUGHTER,
    },
    RELATIONSHIP.PATERNAL_GRANDCHILD: {
        GENDER.MALE: RELATIONSHIP.PATERNAL_GRANDSON,
        GENDER.FEMALE: RELATIONSHIP.PATERNAL_GRANDDAUGHTER,
    },
    RELATIONSHIP.GRANDCHILD: {
        GENDER.MALE: RELATIONSHIP.GRANDSON,
        GENDER.FEMALE: RELATIONSHIP.GRANDDAUGHTER,
    },
    RELATIONSHIP.MATERNAL_GRANDPARENT: {
        GENDER.MALE: RELATIONSHIP.MATERNAL_GRANDFATHER,
        GENDER.FEMALE: RELATIONSHIP.MATERNAL_GRANDMOTHER,
    },
    RELATIONSHIP.PATERNAL_GRANDPARENT: {
        GENDER.MALE: RELATIONSHIP.PATERNAL_GRANDFATHER,
        GENDER.FEMALE: RELATIONSHIP.PATERNAL_GRANDMOTHER,
    },
    RELATIONSHIP.GRANDPARENT: {
        GENDER.MALE: RELATIONSHIP.GRANDFATHER,
        GENDER.FEMALE: RELATIONSHIP.GRANDMOTHER,
    },
}


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


class SPECIES:
    HUMAN = "Homo Sapiens"
    DOG = "Canis lupus familiaris"


class PHENOTYPE:
    class OBSERVED:
        YES = "Positive"
        NO = "Negative"
