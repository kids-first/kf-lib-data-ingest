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

    class VISIBILITY_REASON:
        READY = "Ready For Release"
        PRE_RELEASE = "Pre-Release"
        SAMPLE_ISSUE = "Sample Issue"
        CONSENT_HOLD = "Consent Hold"
        SEQUENCING_QUALITY = "Sequencing Quality Issue"


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
        LYMPHOBLASTOID_CELL_LINES = "Lymphoblastoid Cell Lines"
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
        UMBILICAL_CORD = "Umbilical Cord"
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

        class NYGC:
            NAME = "New York Genome Center"
            KF_ID = "SC_BJW95TMY"

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
        SCRNA = "scRNA-Seq"
        SNRNA = "snRNA-Seq"
        CCS_WGS = "Circular Consensus Sequencing WGS"
        CCS_RNA = "Circular Consensus Sequencing RNA-Seq"
        CLR_WGS = "Continuous Long Reads WGS"
        CLR_RNA = "Continuous Long Reads RNA-Seq"
        ONT_WGS = "ONT WGS"

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
    HMB_GSO = "HMB-GSO"
    # Health/Medical/Biomedical (GSO) (HMB-GSO) Genetic studies only.
    # Use of the data is limited to genetic studies only
    # (i.e., no “phenotype-only” research).


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
    # PARTNERS:
    WIFE = "Wife"
    HUSBAND = "Husband"
    SPOUSE = "Spouse"
    # PARENTS:
    MOTHER = "Mother"
    FATHER = "Father"
    PARENT = "Parent"
    # CHILDREN:
    DAUGHTER = "Daughter"
    SON = "Son"
    CHILD = "Child"
    # SIBLINGS:
    SISTER = "Sister"
    BROTHER = "Brother"
    SIBLING = "Sibling"
    TWIN_SISTER = "Twin Sister"
    TWIN_BROTHER = "Twin Brother"
    TWIN = "Twin"
    MATERNAL_HALF_SISTER = "Maternal Half-Sister"
    MATERNAL_HALF_BROTHER = "Maternal Half-Brother"
    MATERNAL_HALF_SIBLING = "Maternal Half-Sibling"
    PATERNAL_HALF_SISTER = "Paternal Half-Sister"
    PATERNAL_HALF_BROTHER = "Paternal Half-Brother"
    PATERNAL_HALF_SIBLING = "Paternal Half-Sibling"
    HALF_SISTER = "Half-Sister"
    HALF_BROTHER = "Half-Brother"
    HALF_SIBLING = "Half-Sibling"
    # GRANDPARENTS:
    MATERNAL_GRANDMOTHER = "Maternal Grandmother"
    MATERNAL_GRANDFATHER = "Maternal Grandfather"
    MATERNAL_GRANDPARENT = "Maternal Grandparent"
    PATERNAL_GRANDMOTHER = "Paternal Grandmother"
    PATERNAL_GRANDFATHER = "Paternal Grandfather"
    PATERNAL_GRANDPARENT = "Paternal Grandparent"
    GRANDMOTHER = "Grandmother"
    GRANDFATHER = "Grandfather"
    GRANDPARENT = "Grandparent"
    # GRANDCHILDREN:
    MATERNAL_GRANDDAUGHTER = "Maternal Granddaughter"
    MATERNAL_GRANDSON = "Maternal Grandson"
    MATERNAL_GRANDCHILD = "Maternal Grandchild"
    PATERNAL_GRANDDAUGHTER = "Paternal Granddaughter"
    PATERNAL_GRANDSON = "Paternal Grandson"
    PATERNAL_GRANDCHILD = "Paternal Grandchild"
    GRANDDAUGHTER = "Granddaughter"
    GRANDSON = "Grandson"
    GRANDCHILD = "Grandchild"
    # GREAT GRANDPARENTS:
    MATERNAL_GREAT_GRANDMOTHER = "Maternal Great-Grandmother"
    MATERNAL_GREAT_GRANDFATHER = "Maternal Great-Grandfather"
    MATERNAL_GREAT_GRANDPARENT = "Maternal Great-Grandparent"
    PATERNAL_GREAT_GRANDMOTHER = "Paternal Great-Grandmother"
    PATERNAL_GREAT_GRANDFATHER = "Paternal Great-Grandfather"
    PATERNAL_GREAT_GRANDPARENT = "Paternal Great-Grandparent"
    GREAT_GRANDMOTHER = "Great-Grandmother"
    GREAT_GRANDFATHER = "Great-Grandfather"
    GREAT_GRANDPARENT = "Great-Grandparent"
    # GREAT GRANDCHILDREN:
    MATERNAL_GREAT_GRANDDAUGHTER = "Maternal Great-Granddaughter"
    MATERNAL_GREAT_GRANDSON = "Maternal Great-Grandson"
    MATERNAL_GREAT_GRANDCHILD = "Maternal Great-Grandchild"
    PATERNAL_GREAT_GRANDDAUGHTER = "Paternal Great-Granddaughter"
    PATERNAL_GREAT_GRANDSON = "Paternal Great-Grandson"
    PATERNAL_GREAT_GRANDCHILD = "Paternal Great-Grandchild"
    GREAT_GRANDDAUGHTER = "Great-Granddaughter"
    GREAT_GRANDSON = "Great-Grandson"
    GREAT_GRANDCHILD = "Great-Grandchild"
    # AUNTS/UNCLES:
    MATERNAL_AUNT = "Maternal Aunt"
    MATERNAL_UNCLE = "Maternal Uncle"
    MATERNAL_PIBLING = "Maternal Aunt or Uncle"
    PATERNAL_AUNT = "Paternal Aunt"
    PATERNAL_UNCLE = "Paternal Uncle"
    PATERNAL_PIBLING = "Paternal Aunt or Uncle"
    AUNT = "Aunt"
    UNCLE = "Uncle"
    PIBLING = "Aunt or Uncle"
    # NIECES/NEPHEWS:
    MATERNAL_NIECE = "Maternal Niece"
    MATERNAL_NEPHEW = "Maternal Nephew"
    MATERNAL_NIBLING = "Maternal Niece or Nephew"
    PATERNAL_NIECE = "Paternal Niece"
    PATERNAL_NEPHEW = "Paternal Nephew"
    PATERNAL_NIBLING = "Paternal Niece or Nephew"
    NIECE = "Niece"
    NEPHEW = "Nephew"
    NIBLING = "Niece or Nephew"
    # GREAT AUNTS/UNCLES:
    MATERNAL_GREAT_AUNT = "Maternal Great Aunt"
    MATERNAL_GREAT_UNCLE = "Maternal Great Uncle"
    MATERNAL_GREAT_PIBLING = "Maternal Great Aunt or Uncle"
    PATERNAL_GREAT_AUNT = "Paternal Great Aunt"
    PATERNAL_GREAT_UNCLE = "Paternal Great Uncle"
    PATERNAL_GREAT_PIBLING = "Paternal Great Aunt or Uncle"
    GREAT_AUNT = "Great Aunt"
    GREAT_UNCLE = "Great Uncle"
    GREAT_PIBLING = "Great Aunt or Uncle"
    # GREAT NIECES/NEPHEWS:
    MATERNAL_GREAT_NIECE = "Maternal Great Niece"
    MATERNAL_GREAT_NEPHEW = "Maternal Great Nephew"
    MATERNAL_GREAT_NIBLING = "Maternal Great Niece or Nephew"
    PATERNAL_GREAT_NIECE = "Paternal Great Niece"
    PATERNAL_GREAT_NEPHEW = "Paternal Great Nephew"
    PATERNAL_GREAT_NIBLING = "Paternal Great Niece or Nephew"
    GREAT_NIECE = "Great Niece"
    GREAT_NEPHEW = "Great Nephew"
    GREAT_NIBLING = "Great Niece or Nephew"
    # COUSINS:
    MATERNAL_FIRST_COUSIN = "Maternal First Cousin"
    PATERNAL_FIRST_COUSIN = "Paternal First Cousin"
    FIRST_COUSIN = "First Cousin"
    MATERNAL_SECOND_COUSIN = "Maternal Second Cousin"
    PATERNAL_SECOND_COUSIN = "Paternal Second Cousin"
    SECOND_COUSIN = "Second Cousin"
    MATERNAL_COUSIN = "Maternal Cousin"
    PATERNAL_COUSIN = "Paternal Cousin"
    COUSIN = "Cousin"


R = RELATIONSHIP


REVERSE_RELATIONSHIPS = {
    # PARTNERS:
    R.WIFE: R.SPOUSE,
    R.HUSBAND: R.SPOUSE,
    R.SPOUSE: R.SPOUSE,
    # PARENTS:
    R.MOTHER: R.CHILD,
    R.FATHER: R.CHILD,
    R.PARENT: R.CHILD,
    # CHILDREN:
    R.DAUGHTER: R.PARENT,
    R.SON: R.PARENT,
    R.CHILD: R.PARENT,
    # SIBLINGS:
    R.BROTHER: R.SIBLING,
    R.SISTER: R.SIBLING,
    R.SIBLING: R.SIBLING,
    R.TWIN_BROTHER: R.TWIN,
    R.TWIN_SISTER: R.TWIN,
    R.TWIN: R.TWIN,
    R.MATERNAL_HALF_SISTER: R.MATERNAL_HALF_SIBLING,
    R.MATERNAL_HALF_BROTHER: R.MATERNAL_HALF_SIBLING,
    R.MATERNAL_HALF_SIBLING: R.MATERNAL_HALF_SIBLING,
    R.PATERNAL_HALF_SISTER: R.PATERNAL_HALF_SIBLING,
    R.PATERNAL_HALF_BROTHER: R.PATERNAL_HALF_SIBLING,
    R.PATERNAL_HALF_SIBLING: R.PATERNAL_HALF_SIBLING,
    R.HALF_SISTER: R.HALF_SIBLING,
    R.HALF_BROTHER: R.HALF_SIBLING,
    R.HALF_SIBLING: R.HALF_SIBLING,
    # GRANDPARENTS:
    R.MATERNAL_GRANDMOTHER: R.MATERNAL_GRANDCHILD,
    R.MATERNAL_GRANDFATHER: R.MATERNAL_GRANDCHILD,
    R.MATERNAL_GRANDPARENT: R.MATERNAL_GRANDCHILD,
    R.PATERNAL_GRANDMOTHER: R.PATERNAL_GRANDCHILD,
    R.PATERNAL_GRANDFATHER: R.PATERNAL_GRANDCHILD,
    R.PATERNAL_GRANDPARENT: R.PATERNAL_GRANDCHILD,
    R.GRANDMOTHER: R.GRANDCHILD,
    R.GRANDFATHER: R.GRANDCHILD,
    R.GRANDPARENT: R.GRANDCHILD,
    # GRANDCHILDREN:
    R.MATERNAL_GRANDDAUGHTER: R.MATERNAL_GRANDPARENT,
    R.MATERNAL_GRANDSON: R.MATERNAL_GRANDPARENT,
    R.MATERNAL_GRANDCHILD: R.MATERNAL_GRANDPARENT,
    R.PATERNAL_GRANDDAUGHTER: R.PATERNAL_GRANDPARENT,
    R.PATERNAL_GRANDSON: R.PATERNAL_GRANDPARENT,
    R.PATERNAL_GRANDCHILD: R.PATERNAL_GRANDPARENT,
    R.GRANDDAUGHTER: R.GRANDPARENT,
    R.GRANDSON: R.GRANDPARENT,
    R.GRANDCHILD: R.GRANDPARENT,
    # GREAT GRANDPARENTS:
    R.MATERNAL_GREAT_GRANDMOTHER: R.MATERNAL_GREAT_GRANDCHILD,
    R.MATERNAL_GREAT_GRANDFATHER: R.MATERNAL_GREAT_GRANDCHILD,
    R.MATERNAL_GREAT_GRANDPARENT: R.MATERNAL_GREAT_GRANDCHILD,
    R.PATERNAL_GREAT_GRANDMOTHER: R.PATERNAL_GREAT_GRANDCHILD,
    R.PATERNAL_GREAT_GRANDFATHER: R.PATERNAL_GREAT_GRANDCHILD,
    R.PATERNAL_GREAT_GRANDPARENT: R.PATERNAL_GREAT_GRANDCHILD,
    R.GREAT_GRANDMOTHER: R.GREAT_GRANDCHILD,
    R.GREAT_GRANDFATHER: R.GREAT_GRANDCHILD,
    R.GREAT_GRANDPARENT: R.GREAT_GRANDCHILD,
    # GREAT GRANDCHILDREN:
    R.MATERNAL_GREAT_GRANDDAUGHTER: R.MATERNAL_GREAT_GRANDPARENT,
    R.MATERNAL_GREAT_GRANDSON: R.MATERNAL_GREAT_GRANDPARENT,
    R.MATERNAL_GREAT_GRANDCHILD: R.MATERNAL_GREAT_GRANDPARENT,
    R.PATERNAL_GREAT_GRANDDAUGHTER: R.PATERNAL_GREAT_GRANDPARENT,
    R.PATERNAL_GREAT_GRANDSON: R.PATERNAL_GREAT_GRANDPARENT,
    R.PATERNAL_GREAT_GRANDCHILD: R.PATERNAL_GREAT_GRANDPARENT,
    R.GREAT_GRANDDAUGHTER: R.GREAT_GRANDPARENT,
    R.GREAT_GRANDSON: R.GREAT_GRANDPARENT,
    R.GREAT_GRANDCHILD: R.GREAT_GRANDPARENT,
    # AUNTS/UNCLES:
    R.MATERNAL_AUNT: R.MATERNAL_NIBLING,
    R.MATERNAL_UNCLE: R.MATERNAL_NIBLING,
    R.MATERNAL_PIBLING: R.MATERNAL_NIBLING,
    R.PATERNAL_AUNT: R.PATERNAL_NIBLING,
    R.PATERNAL_UNCLE: R.PATERNAL_NIBLING,
    R.PATERNAL_PIBLING: R.PATERNAL_NIBLING,
    R.AUNT: R.NIBLING,
    R.UNCLE: R.NIBLING,
    R.PIBLING: R.NIBLING,
    # NIECES/NEPHEWS:
    R.MATERNAL_NIECE: R.MATERNAL_PIBLING,
    R.MATERNAL_NEPHEW: R.MATERNAL_PIBLING,
    R.MATERNAL_NIBLING: R.MATERNAL_PIBLING,
    R.PATERNAL_NIECE: R.PATERNAL_PIBLING,
    R.PATERNAL_NEPHEW: R.PATERNAL_PIBLING,
    R.PATERNAL_NIBLING: R.PATERNAL_PIBLING,
    R.NIECE: R.PIBLING,
    R.NEPHEW: R.PIBLING,
    R.NIBLING: R.PIBLING,
    # GREAT AUNTS/UNCLES:
    R.MATERNAL_GREAT_AUNT: R.MATERNAL_GREAT_NIBLING,
    R.MATERNAL_GREAT_UNCLE: R.MATERNAL_GREAT_NIBLING,
    R.MATERNAL_GREAT_PIBLING: R.MATERNAL_GREAT_NIBLING,
    R.PATERNAL_GREAT_AUNT: R.PATERNAL_GREAT_NIBLING,
    R.PATERNAL_GREAT_UNCLE: R.PATERNAL_GREAT_NIBLING,
    R.PATERNAL_GREAT_PIBLING: R.PATERNAL_GREAT_NIBLING,
    R.GREAT_AUNT: R.GREAT_NIBLING,
    R.GREAT_UNCLE: R.GREAT_NIBLING,
    R.GREAT_PIBLING: R.GREAT_NIBLING,
    # GREAT NIECES/NEPHEWS:
    R.MATERNAL_GREAT_NIECE: R.MATERNAL_GREAT_PIBLING,
    R.MATERNAL_GREAT_NEPHEW: R.MATERNAL_GREAT_PIBLING,
    R.MATERNAL_GREAT_NIBLING: R.MATERNAL_GREAT_PIBLING,
    R.PATERNAL_GREAT_NIECE: R.PATERNAL_GREAT_PIBLING,
    R.PATERNAL_GREAT_NEPHEW: R.PATERNAL_GREAT_PIBLING,
    R.PATERNAL_GREAT_NIBLING: R.PATERNAL_GREAT_PIBLING,
    R.GREAT_NIECE: R.GREAT_PIBLING,
    R.GREAT_NEPHEW: R.GREAT_PIBLING,
    R.GREAT_NIBLING: R.GREAT_PIBLING,
    # COUSINS:
    R.MATERNAL_FIRST_COUSIN: R.MATERNAL_FIRST_COUSIN,
    R.PATERNAL_FIRST_COUSIN: R.PATERNAL_FIRST_COUSIN,
    R.FIRST_COUSIN: R.FIRST_COUSIN,
    R.MATERNAL_SECOND_COUSIN: R.MATERNAL_SECOND_COUSIN,
    R.PATERNAL_SECOND_COUSIN: R.PATERNAL_SECOND_COUSIN,
    R.SECOND_COUSIN: R.SECOND_COUSIN,
    R.MATERNAL_COUSIN: R.MATERNAL_COUSIN,
    R.PATERNAL_COUSIN: R.PATERNAL_COUSIN,
    R.COUSIN: R.COUSIN,
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
            # PARTNERS (FEMALE):
            R.WIFE,
            # PARENTS (FEMALE):
            R.MOTHER,
            # CHILDREN (FEMALE):
            R.DAUGHTER,
            # SIBLINGS (FEMALE):
            R.SISTER,
            R.TWIN_SISTER,
            R.MATERNAL_HALF_SISTER,
            R.PATERNAL_HALF_SISTER,
            R.HALF_SISTER,
            # GRANDPARENTS (FEMALE):
            R.MATERNAL_GRANDMOTHER,
            R.PATERNAL_GRANDMOTHER,
            R.GRANDMOTHER,
            # GRANDCHILDREN (FEMALE):
            R.MATERNAL_GRANDDAUGHTER,
            R.PATERNAL_GRANDDAUGHTER,
            R.GRANDDAUGHTER,
            # GREAT GRANDPARENTS (FEMALE):
            R.MATERNAL_GREAT_GRANDMOTHER,
            R.PATERNAL_GREAT_GRANDMOTHER,
            R.GREAT_GRANDMOTHER,
            # GREAT GRANDCHILDREN (FEMALE):
            R.MATERNAL_GREAT_GRANDDAUGHTER,
            R.PATERNAL_GREAT_GRANDDAUGHTER,
            R.GREAT_GRANDDAUGHTER,
            # AUNTS/UNCLES (FEMALE):
            R.MATERNAL_AUNT,
            R.PATERNAL_AUNT,
            R.AUNT,
            # NIECES/NEPHEWS (FEMALE):
            R.MATERNAL_NIECE,
            R.PATERNAL_NIECE,
            R.NIECE,
            # GREAT AUNTS/UNCLES (FEMALE):
            R.MATERNAL_GREAT_AUNT,
            R.PATERNAL_GREAT_AUNT,
            R.GREAT_AUNT,
            # GREAT NIECES/NEPHEWS (FEMALE):
            R.MATERNAL_GREAT_NIECE,
            R.PATERNAL_GREAT_NIECE,
            R.GREAT_NIECE,
            # COUSINS (FEMALE): n/a
        },
        GENDER.MALE: {
            # PARTNERS (MALE):
            R.HUSBAND,
            # PARENTS (MALE):
            R.FATHER,
            # CHILDREN (MALE):
            R.SON,
            # SIBLINGS (MALE):
            R.BROTHER,
            R.TWIN_BROTHER,
            R.MATERNAL_HALF_BROTHER,
            R.PATERNAL_HALF_BROTHER,
            R.HALF_BROTHER,
            # GRANDPARENTS (MALE):
            R.MATERNAL_GRANDFATHER,
            R.PATERNAL_GRANDFATHER,
            R.GRANDFATHER,
            # GRANDCHILDREN (MALE):
            R.MATERNAL_GRANDSON,
            R.PATERNAL_GRANDSON,
            R.GRANDSON,
            # GREAT GRANDPARENTS (MALE):
            R.MATERNAL_GREAT_GRANDFATHER,
            R.PATERNAL_GREAT_GRANDFATHER,
            R.GREAT_GRANDFATHER,
            # GREAT GRANDCHILDREN (MALE):
            R.MATERNAL_GREAT_GRANDSON,
            R.PATERNAL_GREAT_GRANDSON,
            R.GREAT_GRANDSON,
            # AUNTS/UNCLES (MALE):
            R.MATERNAL_UNCLE,
            R.PATERNAL_UNCLE,
            R.UNCLE,
            # NIECES/NEPHEWS (MALE):
            R.MATERNAL_NEPHEW,
            R.PATERNAL_NEPHEW,
            R.NEPHEW,
            # GREAT AUNTS/UNCLES (MALE):
            R.MATERNAL_GREAT_UNCLE,
            R.PATERNAL_GREAT_UNCLE,
            R.GREAT_UNCLE,
            # GREAT NIECES/NEPHEWS (MALE):
            R.MATERNAL_GREAT_NEPHEW,
            R.PATERNAL_GREAT_NEPHEW,
            R.GREAT_NEPHEW,
            # COUSINS (MALE): n/a
        },
    }.items()
    for r in rs
}

# relates generic relationship terms to gendered ones
GENDERED_RELATIONSHIPS = {
    # PARTNERS:
    R.SPOUSE: {
        GENDER.MALE: R.HUSBAND,
        GENDER.FEMALE: R.WIFE,
    },
    # PARENTS:
    R.PARENT: {
        GENDER.MALE: R.FATHER,
        GENDER.FEMALE: R.MOTHER,
    },
    # CHILDREN:
    R.CHILD: {
        GENDER.MALE: R.SON,
        GENDER.FEMALE: R.DAUGHTER,
    },
    # SIBLINGS:
    R.SIBLING: {
        GENDER.MALE: R.BROTHER,
        GENDER.FEMALE: R.SISTER,
    },
    R.TWIN: {
        GENDER.MALE: R.TWIN_BROTHER,
        GENDER.FEMALE: R.TWIN_SISTER,
    },
    R.MATERNAL_HALF_SIBLING: {
        GENDER.MALE: R.MATERNAL_HALF_BROTHER,
        GENDER.FEMALE: R.MATERNAL_HALF_SISTER,
    },
    R.PATERNAL_HALF_SIBLING: {
        GENDER.MALE: R.PATERNAL_HALF_BROTHER,
        GENDER.FEMALE: R.PATERNAL_HALF_SISTER,
    },
    R.HALF_SIBLING: {
        GENDER.MALE: R.HALF_BROTHER,
        GENDER.FEMALE: R.HALF_SISTER,
    },
    # GRANDPARENTS:
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
    # GRANDCHILDREN:
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
    # GREAT GRANDPARENTS:
    R.MATERNAL_GREAT_GRANDPARENT: {
        GENDER.MALE: R.MATERNAL_GREAT_GRANDFATHER,
        GENDER.FEMALE: R.MATERNAL_GREAT_GRANDMOTHER,
    },
    R.PATERNAL_GREAT_GRANDPARENT: {
        GENDER.MALE: R.PATERNAL_GREAT_GRANDFATHER,
        GENDER.FEMALE: R.PATERNAL_GREAT_GRANDMOTHER,
    },
    R.GREAT_GRANDPARENT: {
        GENDER.MALE: R.GREAT_GRANDFATHER,
        GENDER.FEMALE: R.GREAT_GRANDMOTHER,
    },
    # GREAT GRANDCHILDREN:
    R.MATERNAL_GREAT_GRANDCHILD: {
        GENDER.MALE: R.MATERNAL_GREAT_GRANDSON,
        GENDER.FEMALE: R.MATERNAL_GREAT_GRANDDAUGHTER,
    },
    R.PATERNAL_GREAT_GRANDCHILD: {
        GENDER.MALE: R.PATERNAL_GREAT_GRANDSON,
        GENDER.FEMALE: R.PATERNAL_GREAT_GRANDDAUGHTER,
    },
    R.GREAT_GRANDCHILD: {
        GENDER.MALE: R.GREAT_GRANDSON,
        GENDER.FEMALE: R.GREAT_GRANDDAUGHTER,
    },
    # AUNTS/UNCLES:
    R.MATERNAL_PIBLING: {
        GENDER.MALE: R.MATERNAL_UNCLE,
        GENDER.FEMALE: R.MATERNAL_AUNT,
    },
    R.PATERNAL_PIBLING: {
        GENDER.MALE: R.PATERNAL_UNCLE,
        GENDER.FEMALE: R.PATERNAL_AUNT,
    },
    R.PIBLING: {
        GENDER.MALE: R.UNCLE,
        GENDER.FEMALE: R.AUNT,
    },
    # NIECES/NEPHEWS:
    R.MATERNAL_NIBLING: {
        GENDER.MALE: R.MATERNAL_NEPHEW,
        GENDER.FEMALE: R.MATERNAL_NIECE,
    },
    R.PATERNAL_NIBLING: {
        GENDER.MALE: R.PATERNAL_NEPHEW,
        GENDER.FEMALE: R.PATERNAL_NIECE,
    },
    R.NIBLING: {
        GENDER.MALE: R.NEPHEW,
        GENDER.FEMALE: R.NIECE,
    },
    # GREAT AUNTS/UNCLES:
    R.MATERNAL_GREAT_PIBLING: {
        GENDER.MALE: R.MATERNAL_GREAT_UNCLE,
        GENDER.FEMALE: R.MATERNAL_GREAT_AUNT,
    },
    R.PATERNAL_GREAT_PIBLING: {
        GENDER.MALE: R.PATERNAL_GREAT_UNCLE,
        GENDER.FEMALE: R.PATERNAL_GREAT_AUNT,
    },
    R.GREAT_PIBLING: {
        GENDER.MALE: R.GREAT_UNCLE,
        GENDER.FEMALE: R.GREAT_AUNT,
    },
    # GREAT NIECES/NEPHEWS:
    R.MATERNAL_GREAT_NIBLING: {
        GENDER.MALE: R.MATERNAL_GREAT_NEPHEW,
        GENDER.FEMALE: R.MATERNAL_GREAT_NIECE,
    },
    R.PATERNAL_GREAT_NIBLING: {
        GENDER.MALE: R.PATERNAL_GREAT_NEPHEW,
        GENDER.FEMALE: R.PATERNAL_GREAT_NIECE,
    },
    R.GREAT_NIBLING: {
        GENDER.MALE: R.GREAT_NEPHEW,
        GENDER.FEMALE: R.GREAT_NIECE,
    },
    # COUSINS: n/a
}

# Relates sibling groups to their known parents.
# Don't include proband in siblings or parents.
RELATIONSHIP_PARENTS = [
    # SIBLINGS & PARENTS:
    {
        "siblings": {
            R.BROTHER,
            R.SISTER,
            R.SIBLING,
            R.TWIN_BROTHER,
            R.TWIN_SISTER,
            R.TWIN,
            R.MATERNAL_HALF_BROTHER,
            R.MATERNAL_HALF_SISTER,
            R.MATERNAL_HALF_SIBLING,
            R.PATERNAL_HALF_BROTHER,
            R.PATERNAL_HALF_SISTER,
            R.PATERNAL_HALF_SIBLING,
            R.HALF_BROTHER,
            R.HALF_SISTER,
            R.HALF_SIBLING,
        },
        "mother": R.MOTHER,
        "father": R.FATHER,
        "generic": R.PARENT,
    },
    # MATERNAL GRANDPARENTS:
    {
        "siblings": {R.MOTHER},
        "mother": R.MATERNAL_GRANDMOTHER,
        "father": R.MATERNAL_GRANDFATHER,
        "generic": R.MATERNAL_GRANDPARENT,
    },
    # PATERNAL GRANDPARENTS:
    {
        "siblings": {R.FATHER},
        "mother": R.PATERNAL_GRANDMOTHER,
        "father": R.PATERNAL_GRANDFATHER,
        "generic": R.PATERNAL_GRANDPARENT,
    },
    # GREAT GRANDPARENTS:
    {
        "siblings": {R.GRANDMOTHER},
        "mother": R.GREAT_GRANDMOTHER,
        "father": R.GREAT_GRANDFATHER,
        "generic": R.GREAT_GRANDPARENT,
    },
    {
        "siblings": {R.GRANDFATHER},
        "mother": R.GREAT_GRANDMOTHER,
        "father": R.GREAT_GRANDFATHER,
        "generic": R.GREAT_GRANDPARENT,
    },
    # MATERNAL GREAT GRANDPARENTS:
    {
        "siblings": {R.MATERNAL_GRANDMOTHER},
        "mother": R.MATERNAL_GREAT_GRANDMOTHER,
        "father": R.MATERNAL_GREAT_GRANDFATHER,
        "generic": R.MATERNAL_GREAT_GRANDPARENT,
    },
    {
        "siblings": {R.MATERNAL_GRANDFATHER},
        "mother": R.MATERNAL_GREAT_GRANDMOTHER,
        "father": R.MATERNAL_GREAT_GRANDFATHER,
        "generic": R.MATERNAL_GREAT_GRANDPARENT,
    },
    # PATERNAL GREAT GRANDPARENTS:
    {
        "siblings": {R.PATERNAL_GRANDMOTHER},
        "mother": R.PATERNAL_GREAT_GRANDMOTHER,
        "father": R.PATERNAL_GREAT_GRANDFATHER,
        "generic": R.PATERNAL_GREAT_GRANDPARENT,
    },
    {
        "siblings": {R.PATERNAL_GRANDFATHER},
        "mother": R.PATERNAL_GREAT_GRANDMOTHER,
        "father": R.PATERNAL_GREAT_GRANDFATHER,
        "generic": R.PATERNAL_GREAT_GRANDPARENT,
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
