from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.extract.operations import *
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT
)

source_data_url = (
    's3://kf-study-us-east-1-prd-sd-p445achv/study-files/modified/'
    'manifest_180917_rectified.tsv'
)

source_data_loading_parameters = {}

operations = [
    keep_map(
        in_col='Library',
        out_col=CONCEPT.SEQUENCING.ID
    ),
    keep_map(
        in_col='Library',
        out_col=CONCEPT.SEQUENCING.LIBRARY_NAME
    ),
    keep_map(
        in_col='Sample Name',
        out_col=CONCEPT.BIOSPECIMEN.ID
    ),
    keep_map(
        in_col='Sample Name',
        out_col=CONCEPT.BIOSPECIMEN_GROUP.ID
    )
]
