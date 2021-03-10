"""
Auto-generated extract config module.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.etl.extract.operations import keep_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT

source_data_url = "file://../data/clinical.tsv"

operations = [
    keep_map(in_col="BS_kfid", out_col=CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID),
    keep_map(in_col="BS_external_aliquot_id", out_col=CONCEPT.BIOSPECIMEN.ID),
    keep_map(in_col="GF_external_id", out_col=CONCEPT.GENOMIC_FILE.ID),
]
