"""
Auto-generated extract config module.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT

# TODO - Replace this with a URL to your own data file
source_data_url = "file://../data/clinical.tsv"

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
    value_map(
        in_col="analyte",
        out_col=CONCEPT.BIOSPECIMEN.ANALYTE,
        m={
            r"dna": constants.SEQUENCING.ANALYTE.DNA,
            r"rna": constants.SEQUENCING.ANALYTE.RNA,
        },
    ),
    keep_map(in_col="diagnosis", out_col=CONCEPT.DIAGNOSIS.NAME),
]
