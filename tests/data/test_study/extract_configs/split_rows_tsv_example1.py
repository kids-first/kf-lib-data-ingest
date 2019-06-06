from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.extract.operations import *
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT
)

source_data_url = 'file://../data/split_rows_tsv_1.tsv'

source_data_loading_parameters = {}

operations = [
    keep_map(
        in_col='pid',
        out_col=CONCEPT.PARTICIPANT.ID
    ),
    value_map(
        in_col='diagnosis',
        m={
            r'.+': lambda x: Split(x.split(','))
        },
        out_col=CONCEPT.DIAGNOSIS.NAME
    ),
    value_map(
        in_col='bsid',
        m={
            r'.+': lambda x: Split(x.split(','))
        },
        out_col=CONCEPT.BIOSPECIMEN.ID
    ),
    keep_map(
        in_col=4,
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
    ),
    keep_map(
        in_col=5,
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
    )
]
