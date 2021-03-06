from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.extract.operations import (
    value_map,
    keep_map,
    constant_map,
)
from kf_lib_data_ingest.common.concept_schema import CONCEPT

source_data_url = (
    "s3://kf-study-us-east-1-prd-sd-p445achv/study-files/modified/"
    "s3_files_kf-seq-data-hudsonalpha_haib17SB5136.tsv"
)

operations = [
    value_map(
        in_col="ETag",
        out_col=CONCEPT.GENOMIC_FILE.HASH_DICT,
        m=lambda x: {"etag": x},
    ),
    keep_map(in_col="ContentLength", out_col=CONCEPT.GENOMIC_FILE.SIZE),
    keep_map(in_col="file_name", out_col=CONCEPT.GENOMIC_FILE.FILE_NAME),
    keep_map(in_col="file_path", out_col=CONCEPT.GENOMIC_FILE.URL_LIST),
    keep_map(in_col="file_path", out_col=CONCEPT.GENOMIC_FILE.ID),
    constant_map(
        m=constants.SEQUENCING.REFERENCE_GENOME.HG19,
        out_col=CONCEPT.SEQUENCING.REFERENCE_GENOME,
    ),
    value_map(
        m={
            r"^kf-seq-data-hudsonalpha\/haib17SB5136\/(SL\d+)\/.+$": lambda x: x  # noqa E501
        },
        in_col="file_path",
        out_col=CONCEPT.SEQUENCING.LIBRARY_NAME,
    ),
    value_map(
        m={
            r"^([^_]+)_s([^_]+)_([^_]+)_.+\.fastq\.gz$": lambda a, b, c: a,
            r".+": None,
        },
        in_col="file_name",
        out_col=CONCEPT.READ_GROUP.FLOW_CELL,
    ),
    value_map(
        m={
            r"^([^_]+)_s([^_]+)_([^_]+)_.+\.fastq\.gz$": lambda a, b, c: b,
            r".+": None,
        },
        in_col="file_name",
        out_col=CONCEPT.READ_GROUP.LANE_NUMBER,
    ),
    value_map(
        m={
            r"^([^_]+)_s([^_]+)_([^_]+)_.+\.fastq\.gz$": lambda a, b, c: c,
            r".+": None,
        },
        in_col="file_name",
        out_col=CONCEPT.READ_GROUP.PAIRED_END,
    ),
    value_map(
        m={
            r"^([^_]+_[^_]+_)[^_]+_(.+)\.fastq\.gz$": lambda a, b: a + b,
            r".+": None,
        },
        in_col="file_name",
        out_col=CONCEPT.READ_GROUP.ID,
    ),
]
