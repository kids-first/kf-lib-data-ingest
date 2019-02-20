from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.extract.operations import *
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT
)

source_data_url = 'file://../data/unsimple_xlsx_1.xlsx'

# we use pandas.read_excel internally for xlsx files
source_data_loading_parameters = {
    'sheet_name': 'Sheet2',
    'usecols': 'B:D',
    'header': None,
    'names': ['sample', 's3_path', 'DNA/RNA'],
    'skiprows': 4
}


operations = [
    value_map(
        in_col='sample',
        m={
            r'BSID(\d+)': lambda x: int(x),  # strip BSID and 0-padding
        },
        out_col=CONCEPT.BIOSPECIMEN.ALIQUOT_ID
    ),
    value_map(
        in_col='sample',
        m={
            r'BSID(\d+)': lambda x: int(x),  # strip BSID and 0-padding
        },
        out_col=CONCEPT.BIOSPECIMEN.ID
    ),
    value_map(
        in_col='s3_path',
        m=lambda x: 's3://' + x,
        out_col=CONCEPT.GENOMIC_FILE.FILE_PATH
    ),
    value_map(
        in_col='DNA/RNA',
        m={
            'DNA': constants.SEQUENCING.ANALYTE.DNA,
            'RNA': constants.SEQUENCING.ANALYTE.RNA
        },
        out_col=CONCEPT.BIOSPECIMEN.ANALYTE
    )
]
