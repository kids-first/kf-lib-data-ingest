"""
Auto-generated extract config module.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT

# TODO - Replace this with a URL to your own data file
source_data_url = "file://../data/sample_manifest.csv"

# TODO - Replace this with operations that make sense for your own data file
operations = [
    keep_map(in_col="participant", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="sample_id", out_col=CONCEPT.SAMPLE.ID),
    value_map(
        in_col="parent_sample",
        m=lambda x: None if x == "na" else x,
        out_col=CONCEPT.BIOSPECIMEN.ID,
    ),
]
