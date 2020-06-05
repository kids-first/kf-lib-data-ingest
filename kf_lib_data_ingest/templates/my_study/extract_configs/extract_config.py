"""
Auto-generated extract config module.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common import constants  # noqa F401
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map

# TODO - Replace this with a URL to your own data file
source_data_url = "file://../data/clinical.tsv"

# TODO (Optional) Fill in special loading parameters here
source_data_read_params = {}

# TODO (Optional) You can set a custom read function with
# source_data_read_func


# TODO - Replace this with operations that make sense for your own data file
operations = [
    keep_map(in_col="family", out_col=CONCEPT.FAMILY.ID),
    value_map(
        in_col="subject",
        m={r"PID(\d+)": lambda x: int(x)},  # strip PID and 0-padding
        out_col=CONCEPT.PARTICIPANT.ID,
    ),
    keep_map(in_col="gender", out_col=CONCEPT.PARTICIPANT.GENDER),
    keep_map(in_col="sample", out_col=CONCEPT.BIOSPECIMEN.ID),
    keep_map(in_col="sample", out_col=CONCEPT.BIOSPECIMEN_GROUP.ID),
    keep_map(in_col="analyte", out_col=CONCEPT.BIOSPECIMEN.ANALYTE),
    keep_map(in_col="diagnosis", out_col=CONCEPT.DIAGNOSIS.NAME),
]
