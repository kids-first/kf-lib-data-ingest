from kf_lib_data_ingest.etl.extract.operations import (
    value_map,
    constant_map,
    row_map,
    keep_map,
)
from kf_lib_data_ingest.common.concept_schema import CONCEPT

source_data_url = "file://../data/simple_headered_tsv_2.tsv"


operations = [
    value_map(
        in_col="specimen",
        m={r"BSID(\d+)": lambda x: int(x)},  # strip BSID and 0-padding
        out_col=CONCEPT.BIOSPECIMEN.ID,
    ),
    value_map(
        in_col="specimen",
        m={r"BSID(\d+)": lambda x: int(x)},  # strip BSID and 0-padding
        out_col=CONCEPT.BIOSPECIMEN_GROUP.ID,
    ),
    constant_map(
        out_col=CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID, m="SC_A1JNZAZH"
    ),
    keep_map(in_col="bam_file_name", out_col=CONCEPT.GENOMIC_FILE.FILE_NAME),
    keep_map(in_col="cram_file_name", out_col=CONCEPT.GENOMIC_FILE.FILE_NAME),
    row_map(
        m=lambda row: [row["storage_dir"] + "/" + row["bam_file_name"]],
        out_col=CONCEPT.GENOMIC_FILE.URL_LIST,
    ),
    row_map(
        m=lambda row: [row["storage_dir"] + "/" + row["cram_file_name"]],
        out_col=CONCEPT.GENOMIC_FILE.URL_LIST,
    ),
]
