from common import constants
from etl.extract.operations import *
from etl.transform.standard_model.concept_schema import CONCEPT

source_data_url = (
    'file:///Users/kelmana/Documents/kids-first/kf-study-imports/Avi/'
    'boyd17/data/manifest_180917_rectified.tsv'
)

source_data_loading_parameters = {}

operations = [
    keep_map(
        in_col='Library',
        out_col=CONCEPT.SEQUENCING.LIBRARY_NAME
    ),
    keep_map(
        in_col='combined',
        out_col=CONCEPT.BIOSPECIMEN.ALIQUOT_ID
    ),
    keep_map(
        in_col='combined',
        out_col=CONCEPT.BIOSPECIMEN.ID
    )
]
