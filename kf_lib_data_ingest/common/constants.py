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
    NOT_ABLE_TO_PROVIDE = "Not Able to Provide"
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
        MD5 = "md5"
        SHA1 = "sha1"
        SHA256 = "sha256"
        SHA512 = "sha512"
        S3_ETAG = "etag"


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
        SALIVA_KIT = "Saliva Kit"
        SUBTOTAL_RESECTION = "Subtotal Resection"
        TOTAL_RESECTION = "Gross Total Resection"
        BONE_MARROW_ASPIRATION = "Bone Marrow Aspiration"

    class ANATOMY_SITE:
        ARM = "Arm"
        BONE_MARROW = "Bone Marrow"
        HAIR = "Hair"
        MOUTH = "Mouth"
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
        ONT = "ONT"

    class INSTRUMENT:
        HISEQ_X_v2_5 = "HiSeq X v2.5"
        HISEQ_X_10 = "HiSeq X Ten"

    class STRAND:
        FIRST = "First Stranded"
        SECOND = "Second Stranded"
        STRANDED = "Stranded"
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

        class CSIR:
            NAME = "CSIR - Institute of Genomics and Integrative Biology, Delhi, India"
            KF_ID = "SC_MDY0AZMZ"

        class FELINE_DIAGNOSTICS:
            NAME = "Feline Diagnostics LLC"
            KF_ID = "SC_CATTVETT"

        class FG:
            NAME = "fg"
            KF_ID = "SC_XXXXXXX2"

        class HGT:
            NAME = "Humangenetik Tübingen"
            KF_ID = "SC_75KENA7A"

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

        class NIH:
            NAME = "National Institutes of Health"
            KF_ID = "SC_HEXD2E5R"

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

        class TEMPUS:
            NAME = "Tempus"
            KF_ID = "SC_TQ8HJWGE"

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
        METHYL = "Methylation"
        MRNA = "miRNA-Seq"
        RNA = "RNA-Seq"
        WGS = "WGS"
        WXS = "WXS"
        TARGETED = "Targeted Sequencing"
        PANEL = "Panel"

    class ANALYTE:
        DNA = "DNA"
        RNA = "RNA"
        VIRTUAL = "Virtual"

    class LIBRARY:
        class SELECTION:
            HYBRID = "Hybrid Selection"
            PCR = "PCR"
            AFFINITY_ENRICHMENT = "Affinity Enrichment"
            POLYT_ENRICHMENT = "Poly-T Enrichment"
            RANDOM = "Random"
            RNA_DEPLETION = "rRNA Depletion"
            MRNA_SIZE_FRACTIONATION = "miRNA Size Fractionation"

        class PREP:
            POLYA = "polyA"
            TOTALRNASEQ = "totalRNAseq"


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
    GRU_NPU = "GRU-NPU"
    # General Research Use (NPU) (GRU-NPU) - Use of this data is
    # limited to general research use. Use of the data is limited to
    # not-for-profit organizations.


class AUTHORITY:
    DBGAP = "dbGaP"


class OUTCOME:
    class DISEASE_RELATED:
        YES = "Yes"
        NO = "No"

    class VITAL_STATUS:
        ALIVE = "Alive"
        DEAD = "Deceased"


# ######################## NOTE ###############################################
# "Twin" relationships here mean identical twins.
# For non-identical twins please just use the normal sibling relationships.
# #############################################################################


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
    HALF_BROTHER = "Half-Brother"
    HALF_SISTER = "Half-Sister"
    HALF_SIBLING = "Half-Sibling"
    SIBLING = "Sibling"
    DAUGHTER = "Daughter"
    SON = "Son"
    CHILD = "Child"
    MATERNAL_AUNT = "Maternal Aunt"
    MATERNAL_GRANDMOTHER = "Maternal Grandmother"
    MATERNAL_GRANDFATHER = "Maternal Grandfather"
    MATERNAL_GRANDPARENT = "Maternal Grandparent"
    MATERNAL_GREAT_GRANDMOTHER = "Maternal Great-Grandmother"
    MATERNAL_GREAT_GRANDFATHER = "Maternal Great-Grandfather"
    MATERNAL_GREAT_GRANDPARENT = "Maternal Great-Grandparent"
    MATERNAL_UNCLE = "Maternal Uncle"
    PATERNAL_AUNT = "Paternal Aunt"
    PATERNAL_GRANDMOTHER = "Paternal Grandmother"
    PATERNAL_GRANDFATHER = "Paternal Grandfather"
    PATERNAL_GRANDPARENT = "Paternal Grandparent"
    PATERNAL_GREAT_GRANDMOTHER = "Paternal Great-Grandmother"
    PATERNAL_GREAT_GRANDFATHER = "Paternal Great-Grandfather"
    PATERNAL_GREAT_GRANDPARENT = "Paternal Great-Grandparent"
    PATERNAL_UNCLE = "Paternal Uncle"
    GRANDMOTHER = "Grandmother"
    GRANDFATHER = "Grandfather"
    GRANDPARENT = "Grandparent"
    GREAT_GRANDMOTHER = "Great-Grandmother"
    GREAT_GRANDFATHER = "Great-Grandfather"
    GREAT_GRANDPARENT = "Great-Grandparent"
    MATERNAL_GRANDDAUGHTER = "Maternal Granddaughter"
    MATERNAL_GRANDSON = "Maternal Grandson"
    MATERNAL_GRANDCHILD = "Maternal Grandchild"
    MATERNAL_GREAT_GRANDDAUGHTER = "Maternal Great-Granddaughter"
    MATERNAL_GREAT_GRANDSON = "Maternal Great-Grandson"
    MATERNAL_GREAT_GRANDCHILD = "Maternal Great-Grandchild"
    PATERNAL_GRANDDAUGHTER = "Paternal Granddaughter"
    PATERNAL_GRANDSON = "Paternal Grandson"
    PATERNAL_GRANDCHILD = "Paternal Grandchild"
    PATERNAL_GREAT_GRANDDAUGHTER = "Paternal Great-Granddaughter"
    PATERNAL_GREAT_GRANDSON = "Paternal Great-Grandson"
    PATERNAL_GREAT_GRANDCHILD = "Paternal Great-Grandchild"
    GRANDDAUGHTER = "Granddaughter"
    GRANDSON = "Grandson"
    GRANDCHILD = "Grandchild"
    GREAT_GRANDDAUGHTER = "Great-Granddaughter"
    GREAT_GRANDSON = "Great-Grandson"
    GREAT_GRANDCHILD = "Great-Grandchild"
    MATERNAL_NEPHEW = "Maternal Nephew"
    MATERNAL_NIECE = "Maternal Niece"
    PATERNAL_NEPHEW = "Paternal Nephew"
    PATERNAL_NIECE = "Paternal Niece"
    NEPHEW = "Nephew"
    NIECE = "Niece"


R = RELATIONSHIP


REVERSE_RELATIONSHIPS = {
    R.HUSBAND: R.SPOUSE,
    R.WIFE: R.SPOUSE,
    R.SPOUSE: R.SPOUSE,
    R.MOTHER: R.CHILD,
    R.FATHER: R.CHILD,
    R.PARENT: R.CHILD,
    R.BROTHER: R.SIBLING,
    R.SISTER: R.SIBLING,
    R.SIBLING: R.SIBLING,
    R.TWIN_BROTHER: R.TWIN,
    R.TWIN_SISTER: R.TWIN,
    R.TWIN: R.TWIN,
    R.DAUGHTER: R.PARENT,
    R.SON: R.PARENT,
    R.CHILD: R.PARENT,
    R.MATERNAL_GRANDMOTHER: R.MATERNAL_GRANDCHILD,
    R.MATERNAL_GRANDFATHER: R.MATERNAL_GRANDCHILD,
    R.MATERNAL_GRANDPARENT: R.MATERNAL_GRANDCHILD,
    R.PATERNAL_GRANDMOTHER: R.PATERNAL_GRANDCHILD,
    R.PATERNAL_GRANDFATHER: R.PATERNAL_GRANDCHILD,
    R.PATERNAL_GRANDPARENT: R.PATERNAL_GRANDCHILD,
    R.GRANDMOTHER: R.GRANDCHILD,
    R.GRANDFATHER: R.GRANDCHILD,
    R.GRANDPARENT: R.GRANDCHILD,
    R.MATERNAL_GRANDDAUGHTER: R.MATERNAL_GRANDPARENT,
    R.MATERNAL_GRANDSON: R.MATERNAL_GRANDPARENT,
    R.MATERNAL_GRANDCHILD: R.MATERNAL_GRANDPARENT,
    R.PATERNAL_GRANDDAUGHTER: R.PATERNAL_GRANDPARENT,
    R.PATERNAL_GRANDSON: R.PATERNAL_GRANDPARENT,
    R.PATERNAL_GRANDCHILD: R.PATERNAL_GRANDPARENT,
    R.GRANDDAUGHTER: R.GRANDPARENT,
    R.GRANDSON: R.GRANDPARENT,
    R.GRANDCHILD: R.GRANDPARENT,
}


class GENDER:
    MALE = "Male"
    FEMALE = "Female"
    OTHER = "Other"


# relates gendered relationship terms to their genders.
GENDER_FROM_RELATION = {
    r: g
    for g, rs in {
        GENDER.FEMALE: {
            R.WIFE,
            R.MOTHER,
            R.SISTER,
            R.TWIN_SISTER,
            R.DAUGHTER,
            R.MATERNAL_GRANDMOTHER,
            R.PATERNAL_GRANDMOTHER,
            R.GRANDMOTHER,
            R.MATERNAL_GRANDDAUGHTER,
            R.PATERNAL_GRANDDAUGHTER,
            R.GRANDDAUGHTER,
        },
        GENDER.MALE: {
            R.HUSBAND,
            R.FATHER,
            R.BROTHER,
            R.TWIN_BROTHER,
            R.SON,
            R.MATERNAL_GRANDFATHER,
            R.PATERNAL_GRANDFATHER,
            R.GRANDFATHER,
            R.MATERNAL_GRANDSON,
            R.PATERNAL_GRANDSON,
            R.GRANDSON,
        },
    }.items()
    for r in rs
}

# relates generic relationship terms to gendered ones
GENDERED_RELATIONSHIPS = {
    R.SPOUSE: {
        GENDER.MALE: R.HUSBAND,
        GENDER.FEMALE: R.WIFE,
    },
    R.CHILD: {
        GENDER.MALE: R.SON,
        GENDER.FEMALE: R.DAUGHTER,
    },
    R.SIBLING: {
        GENDER.MALE: R.BROTHER,
        GENDER.FEMALE: R.SISTER,
    },
    R.TWIN: {
        GENDER.MALE: R.TWIN_BROTHER,
        GENDER.FEMALE: R.TWIN_SISTER,
    },
    R.PARENT: {
        GENDER.MALE: R.FATHER,
        GENDER.FEMALE: R.MOTHER,
    },
    R.MATERNAL_GRANDCHILD: {
        GENDER.MALE: R.MATERNAL_GRANDSON,
        GENDER.FEMALE: R.MATERNAL_GRANDDAUGHTER,
    },
    R.PATERNAL_GRANDCHILD: {
        GENDER.MALE: R.PATERNAL_GRANDSON,
        GENDER.FEMALE: R.PATERNAL_GRANDDAUGHTER,
    },
    R.GRANDCHILD: {
        GENDER.MALE: R.GRANDSON,
        GENDER.FEMALE: R.GRANDDAUGHTER,
    },
    R.MATERNAL_GRANDPARENT: {
        GENDER.MALE: R.MATERNAL_GRANDFATHER,
        GENDER.FEMALE: R.MATERNAL_GRANDMOTHER,
    },
    R.PATERNAL_GRANDPARENT: {
        GENDER.MALE: R.PATERNAL_GRANDFATHER,
        GENDER.FEMALE: R.PATERNAL_GRANDMOTHER,
    },
    R.GRANDPARENT: {
        GENDER.MALE: R.GRANDFATHER,
        GENDER.FEMALE: R.GRANDMOTHER,
    },
}

# Relates sibling groups to their known parents.
# Don't include proband in siblings or parents.
RELATIONSHIP_PARENTS = [
    {
        "siblings": {
            R.BROTHER,
            R.SISTER,
            R.SIBLING,
            R.TWIN_BROTHER,
            R.TWIN_SISTER,
            R.TWIN,
        },
        "mother": R.MOTHER,
        "father": R.FATHER,
        "generic": R.PARENT,
    },
    {
        "siblings": {R.MOTHER},
        "mother": R.MATERNAL_GRANDMOTHER,
        "father": R.MATERNAL_GRANDFATHER,
        "generic": R.MATERNAL_GRANDPARENT,
    },
    {
        "siblings": {R.FATHER},
        "mother": R.PATERNAL_GRANDMOTHER,
        "father": R.PATERNAL_GRANDFATHER,
        "generic": R.PATERNAL_GRANDPARENT,
    },
]


def genderize_relationship(relationship, gender):
    r = GENDERED_RELATIONSHIPS.get(relationship)
    if r:
        return r.get(gender, relationship)
    else:
        return relationship


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
    DOG = "Canis lupus familiaris"
    FLY = "Drosophila melanogaster"
    HUMAN = "Homo sapiens"
    MOUSE = "Mus musculus"


class PHENOTYPE:
    class OBSERVED:
        YES = "Positive"
        NO = "Negative"
