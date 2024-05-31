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
source_data_url = "file://../data/sample_manifest.csv"

# TODO - Replace this with operations that make sense for your own data file
operations = [
    keep_map(in_col="participant", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="sample_id", out_col=CONCEPT.SAMPLE.ID),
    value_map(
        in_col="aliquot_id",
        m=lambda x: None if x == "na" else x,
        out_col=CONCEPT.BIOSPECIMEN.ID,
    ),
    value_map(
        in_col="tissue_type",
        m={
            "tumor": constants.SPECIMEN.TISSUE_TYPE.TUMOR,
            "normal": constants.SPECIMEN.TISSUE_TYPE.NORMAL,
        },
        out_col=CONCEPT.SAMPLE.TISSUE_TYPE,
    ),
    value_map(
        in_col="composition",
        m={
            "tissue": constants.SPECIMEN.COMPOSITION.TISSUE,
            "blood": constants.SPECIMEN.COMPOSITION.BLOOD,
            "bone": constants.SPECIMEN.COMPOSITION.BONE,
        },
        out_col=CONCEPT.SAMPLE.COMPOSITION,
    ),
    keep_map(in_col="age_at_collection", out_col=CONCEPT.SAMPLE.EVENT_AGE_DAYS),
    value_map(
        in_col="analyte",
        m={
            "DNA": constants.SEQUENCING.ANALYTE.DNA,
            "RNA": constants.SEQUENCING.ANALYTE.RNA,
            "NA": None,
            "na": None,
        },
        out_col=CONCEPT.BIOSPECIMEN.ANALYTE,
    ),
]
