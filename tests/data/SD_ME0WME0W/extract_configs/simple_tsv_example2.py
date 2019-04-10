from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.extract.operations import *
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT
)

# source_data_url = 'file://../data/simple_headered_tsv_2.tsv'
host = 'https://kf-study-creator.kids-first.io'
kfid = 'SF_XK75K8A4'
source_data_url = f'{host}/download/study/SD_ME0WME0W/file/{kfid}'

source_data_loading_parameters = {}


operations = [
    value_map(
        in_col='specimen',
        m={
            r'BSID(\d+)': lambda x: int(x),  # strip BSID and 0-padding
        },
        out_col=CONCEPT.BIOSPECIMEN.ALIQUOT_ID
    ),
    value_map(
        in_col='specimen',
        m={
            r'BSID(\d+)': lambda x: int(x),  # strip BSID and 0-padding
        },
        out_col=CONCEPT.BIOSPECIMEN.ID
    ),
    keep_map(
        in_col='bam_file_name',
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
    ),
    keep_map(
        in_col='cram_file_name',
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
    ),
    row_map(
        m=lambda row: row['storage_dir'] + '/' + row['bam_file_name'],
        out_col=CONCEPT.GENOMIC_FILE.FILE_PATH
    ),
    row_map(
        m=lambda row: row['storage_dir'] + '/' + row['cram_file_name'],
        out_col=CONCEPT.GENOMIC_FILE.FILE_PATH
    )
]