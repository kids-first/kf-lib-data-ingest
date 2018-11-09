from common import constants
from etl.extract.operations import *
from etl.transform.standard_model.concept_schema import CONCEPT

source_data_url = 'file://../data/simple_headered_tsv_2.tsv'

source_data_loading_parameters = {}


operations = [
    value_map(
        in_col='specimen',
        m={
            r'BSID(\d+)': lambda x: int(x),  # strip BSID and 0-padding
        },
        out_col=CONCEPT.BIOSPECIMEN.ALIQUOT_ID
    ),
    keep_map(
        in_col='bam_file_name',
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
    ),
    row_map(
        m=lambda row: row['storage_dir'] + '/' + row['bam_file_name'],
        out_col=CONCEPT.GENOMIC_FILE.FILE_PATH
    ),
    keep_map(
        in_col='cram_file_name',
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
    ),
    row_map(
        m=lambda row: row['storage_dir'] + '/' + row['cram_file_name'],
        out_col=CONCEPT.GENOMIC_FILE.FILE_PATH
    )
]
