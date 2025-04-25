import os
import re
import pandas as pd
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.constants import COMMON, GENOMIC_FILE
from kf_lib_data_ingest.common.pandas_utils import Split
from kf_lib_data_ingest.etl.extract.operations import (
    constant_map,
    keep_map,
    row_map,
    value_map,
)

# TODO - Replace this with a URL to your own data file
source_data_url = "file://../data/test_singlecell_manifest.csv"

FILE_EXT_FORMAT_MAP = {
    "BAI": GENOMIC_FILE.FORMAT.BAI,
    "BAM": GENOMIC_FILE.FORMAT.BAM,
    "CRAI": GENOMIC_FILE.FORMAT.CRAI,
    "CRAM": GENOMIC_FILE.FORMAT.CRAM,
    "DCM": GENOMIC_FILE.FORMAT.DCM,
    "FASTQ": GENOMIC_FILE.FORMAT.FASTQ,
    "GPR": GENOMIC_FILE.FORMAT.GPR,
    "VCF": GENOMIC_FILE.FORMAT.VCF,
    "IDAT": GENOMIC_FILE.FORMAT.IDAT,
    "PDF": GENOMIC_FILE.FORMAT.PDF,
    "SVS": GENOMIC_FILE.FORMAT.SVS,
    "TBI": GENOMIC_FILE.FORMAT.TBI,
    "HTML": GENOMIC_FILE.FORMAT.HTML,
    "MAF": GENOMIC_FILE.FORMAT.MAF,
    "CNS": "cns",
    "TXT": "txt",
    "PNG": "png",
    "CSV": "csv",
    "PED": "ped",
    "SEG": "seg",
    "TAR": "tar",
    "TSV": "tsv",
    "MZML": "mzML",
    "MZID": "mzID",
    "RAW": "RAW",
    "PSM": "PSM",
}


def file_format(x):
    """
    Get genomic file extension
    """
    if x in FILE_EXT_FORMAT_MAP:
        file_ext = FILE_EXT_FORMAT_MAP[x]
    else:
        file_ext = None

    return file_ext


def fname(key):
    """
    Return just the filename portion of the key
    """
    return key.rsplit("/", 1)[-1]


operations = [
    keep_map("sample_id", out_col=CONCEPT.BIOSPECIMEN_GROUP.ID),
    keep_map("aliquot_id", out_col=CONCEPT.BIOSPECIMEN.ID),
    ### genomic file
    keep_map(in_col="file_name", out_col=CONCEPT.GENOMIC_FILE.ID),
    # file url
    value_map(
        in_col="file_name",
        m=lambda x: [f"{x}"],
        out_col=CONCEPT.GENOMIC_FILE.URL_LIST,
    ),
    # file name
    value_map(
        in_col="file_name",
        m=fname,
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME,
    ),
    # data type
    constant_map(m="Other", out_col=CONCEPT.GENOMIC_FILE.DATA_TYPE),
    # file format
    value_map(
        in_col="file_format",
        m=file_format,
        out_col=CONCEPT.GENOMIC_FILE.FILE_FORMAT,
    ),
    # file size
    keep_map(in_col="file_size", out_col=CONCEPT.GENOMIC_FILE.SIZE),
    constant_map(
        m="unharmonized", out_col=CONCEPT.GENOMIC_FILE.FILE_VERSION_DESCRIPTOR
    ),
    # file md5
    value_map(
        in_col="file_hash_value",
        out_col=CONCEPT.GENOMIC_FILE.HASH_DICT,
        m=lambda x: {constants.FILE.HASH.MD5.lower(): x},
    ),
    # access
    constant_map(
        m=constants.COMMON.TRUE, out_col=CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS
    ),
    # harmonization status
    constant_map(
        m=constants.COMMON.FALSE, out_col=CONCEPT.GENOMIC_FILE.HARMONIZED
    ),
    # file availability
    constant_map(
        m=constants.GENOMIC_FILE.AVAILABILITY.IMMEDIATE,
        out_col=CONCEPT.GENOMIC_FILE.AVAILABILITY,
    ),
    # sequencing
    keep_map("aliquot_id", out_col=CONCEPT.SEQUENCING.ID),
    keep_map(in_col="experiment_strategy", out_col=CONCEPT.SEQUENCING.STRATEGY),
    constant_map(
        m="SC_MSQP04K7", out_col=CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID
    ),
    keep_map("platform", out_col=CONCEPT.SEQUENCING.PLATFORM),
    keep_map("instrument_model", out_col=CONCEPT.SEQUENCING.INSTRUMENT),
    keep_map("end_bias", out_col=CONCEPT.SEQUENCING.END_BIAS),
    keep_map("library_selection", out_col=CONCEPT.SEQUENCING.LIBRARY_SELECTION),
    keep_map("library_strand", out_col=CONCEPT.SEQUENCING.LIBRARY_STRAND),
    keep_map(
        "library_construction", out_col=CONCEPT.SEQUENCING.LIBRARY_CONSTRUCTION
    ),
    keep_map("UMI_barcode_read", out_col=CONCEPT.SEQUENCING.UMI_BARCODE_READ),
    keep_map(
        "UMI_barcode_offset", out_col=CONCEPT.SEQUENCING.UMI_BARCODE_OFFSET
    ),
    keep_map("UMI_barcode_size", out_col=CONCEPT.SEQUENCING.UMI_BARCODE_SIZE),
    keep_map("cell_barcode_read", out_col=CONCEPT.SEQUENCING.CELL_BARCODE_READ),
    keep_map(
        "cell_barcode_offset", out_col=CONCEPT.SEQUENCING.CELL_BARCODE_OFFSET
    ),
    keep_map("cell_barcode_size", out_col=CONCEPT.SEQUENCING.CELL_BARCODE_SIZE),
    keep_map("cDNA_read", out_col=CONCEPT.SEQUENCING.CDNA_READ),
    keep_map("cDNA_read_offset", out_col=CONCEPT.SEQUENCING.CDNA_READ_OFFSET),
    keep_map("is_paired_end", out_col=CONCEPT.SEQUENCING.PAIRED_END),
    keep_map("read_pair_number", out_col=CONCEPT.SEQUENCING.READ_PAIR_NUMBER),
    keep_map(
        "is_adapter_trimmed", out_col=CONCEPT.SEQUENCING.IS_ADAPTER_TRIMMED
    ),
    keep_map("total_reads", out_col=CONCEPT.SEQUENCING.TOTAL_READS),
    keep_map(
        "target_cell_number", out_col=CONCEPT.SEQUENCING.TARGET_CELL_NUMBER
    ),
    # visiable
    # sequencing
    constant_map(m=constants.COMMON.FALSE, out_col=CONCEPT.SEQUENCING.VISIBLE),
    constant_map(m="Other", out_col=CONCEPT.SEQUENCING.VISIBILTIY_REASON),
    constant_map(m="Other", out_col=CONCEPT.SEQUENCING.VISIBILITY_COMMENT),
    # genomic_file
    constant_map(
        m=constants.COMMON.FALSE, out_col=CONCEPT.GENOMIC_FILE.VISIBLE
    ),
    constant_map(m="Other", out_col=CONCEPT.GENOMIC_FILE.VISIBILTIY_REASON),
    constant_map(m="Other", out_col=CONCEPT.GENOMIC_FILE.VISIBILITY_COMMENT),
    # biospecimen_genomic_file
    constant_map(
        m=constants.COMMON.FALSE,
        out_col=CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE,
    ),
    constant_map(
        m="Other", out_col=CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBILTIY_REASON
    ),
    constant_map(
        m="Other", out_col=CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBILITY_COMMENT
    ),
    # sequencing_genomic_file
    constant_map(
        m=constants.COMMON.FALSE,
        out_col=CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE,
    ),
    constant_map(
        m="Other", out_col=CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBILTIY_REASON
    ),
    constant_map(
        m="Other", out_col=CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBILITY_COMMENT
    ),
]
