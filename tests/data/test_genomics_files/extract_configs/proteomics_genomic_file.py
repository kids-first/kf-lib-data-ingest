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
source_data_url = "file://../data/test_proteomics_manifest.csv"

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
    "PSM": "PSM"

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
    keep_map('sample_id', out_col=CONCEPT.BIOSPECIMEN_GROUP.ID),
    keep_map('aliquot_id', out_col=CONCEPT.BIOSPECIMEN.ID),

    ### genomic file
    keep_map(in_col="file_name", out_col=CONCEPT.GENOMIC_FILE.ID),
    
    # file url
    value_map(
        in_col = "file_name",
        m = lambda x: [f'{x}'],
        out_col = CONCEPT.GENOMIC_FILE.URL_LIST,
    ),
    # file name
    value_map(
        in_col = "file_name",
        m=fname,
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME,
    ),

    # data type
    constant_map(m="Other", out_col=CONCEPT.GENOMIC_FILE.DATA_TYPE),
   
    # file format
    value_map(
        in_col="file_format",
        m = file_format,
        out_col=CONCEPT.GENOMIC_FILE.FILE_FORMAT,
    ),

    # file size
    keep_map(in_col="file_size", out_col=CONCEPT.GENOMIC_FILE.SIZE),
    constant_map(m="unharmonized", out_col=CONCEPT.GENOMIC_FILE.FILE_VERSION_DESCRIPTOR),

    # file md5
    value_map(
        in_col="file_hash_value",
        out_col=CONCEPT.GENOMIC_FILE.HASH_DICT,
        m=lambda x: {constants.FILE.HASH.MD5.lower(): x}
    ),

    # access
    constant_map(
        m=constants.COMMON.TRUE, out_col=CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS
    ),

    # harmonization status
    constant_map(
        m=constants.COMMON.FALSE, 
        out_col=CONCEPT.GENOMIC_FILE.HARMONIZED
    ),
    
    # file availability
    constant_map(
        m=constants.GENOMIC_FILE.AVAILABILITY.IMMEDIATE,
        out_col=CONCEPT.GENOMIC_FILE.AVAILABILITY,
    ),

    # sequencing
    keep_map('aliquot_id', out_col=CONCEPT.SEQUENCING.ID),
    keep_map(in_col='experiment_strategy', out_col=CONCEPT.SEQUENCING.STRATEGY),
    keep_map(in_col='proteomics_experiment', out_col=CONCEPT.SEQUENCING.PROTEOMICS_EXPERIMENT),
    keep_map('mass_spec_rawfile_conversion', out_col=CONCEPT.SEQUENCING.MASS_SPEC_RAWFILE_CONVERSION),
    constant_map(m="SC_MSQP04K7", out_col=CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID),
    keep_map('platform', out_col=CONCEPT.SEQUENCING.PLATFORM),
    keep_map('acquisition_type', out_col=CONCEPT.SEQUENCING.ACQUISITION_TYPE),
    keep_map('ion_fragmentation', out_col=CONCEPT.SEQUENCING.ION_FRAGMENTATION),
    keep_map('enrichment_approach', out_col=CONCEPT.SEQUENCING.ENRICHMENT_APPROACH),
    keep_map('quantification_technique', out_col=CONCEPT.SEQUENCING.QUANTIFICATION_TECHNIQUE),
    keep_map('quantification_labeling_method', out_col=CONCEPT.SEQUENCING.QUANTIFICATION_LABELING_METHOD),
    keep_map('quantification_label_id', out_col=CONCEPT.SEQUENCING.QUANTIFICATION_LABEL_ID),
    keep_map('chromatography_approach', out_col=CONCEPT.SEQUENCING.CHROMATOGRAPHY_APPROACH),
    keep_map('fractionation_approach', out_col=CONCEPT.SEQUENCING.FRACTIONATION_APPROACH),
    keep_map('fraction_number', out_col=CONCEPT.SEQUENCING.FRACTION_NUMBER),

    # visiable
    # sequencing
    keep_map(in_col="visible", out_col=CONCEPT.SEQUENCING.VISIBLE),
    constant_map(m="Other", out_col=CONCEPT.SEQUENCING.VISIBILTIY_REASON),
    constant_map(m="Other", out_col=CONCEPT.SEQUENCING.VISIBILITY_COMMENT),

    # genomic_file
    keep_map(in_col="visible", out_col=CONCEPT.GENOMIC_FILE.VISIBLE),
    constant_map(m="Other", out_col=CONCEPT.GENOMIC_FILE.VISIBILTIY_REASON),
    constant_map(m="Other", out_col=CONCEPT.GENOMIC_FILE.VISIBILITY_COMMENT),

    # biospecimen_genomic_file
    keep_map(in_col="visible", out_col=CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE),
    constant_map(m="Other", out_col=CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBILTIY_REASON),
    constant_map(m="Other", out_col=CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBILITY_COMMENT),

    # sequencing_genomic_file
    keep_map(in_col="visible", out_col=CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE),
    constant_map(m="Other", out_col=CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBILTIY_REASON),
    constant_map(m="Other", out_col=CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBILITY_COMMENT),
]