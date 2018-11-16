from common import constants
from etl.extract.operations import *
from etl.transform.standard_model.concept_schema import CONCEPT

source_data_url = (
    's3://kf-study-us-east-1-prd-sd-p445achv/study-files/modified/'
    's3_files_kf-seq-data-hudsonalpha_haib17SB5136.tsv'
)

source_data_loading_parameters = {}

operations = [
    keep_map(
        in_col='ETag',
        out_col=CONCEPT.GENOMIC_FILE.ETAG
    ),
    keep_map(
        in_col='ContentLength',
        out_col=CONCEPT.GENOMIC_FILE.SIZE
    ),
    keep_map(
        in_col='file_name',
        out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
    ),
    keep_map(
        in_col='file_path',
        out_col=CONCEPT.GENOMIC_FILE.FILE_PATH
    ),
    constant_map(
        m=constants.SEQUENCING.REFERENCE_GENOME.HG19,
        out_col=CONCEPT.SEQUENCING.REFERENCE_GENOME
    ),
    value_map(
        m={
            r'^kf-seq-data-hudsonalpha\/haib17SB5136\/(SL\d+)\/.+$': lambda x: x  # noqa E501
        },
        in_col='file_path',
        out_col=CONCEPT.SEQUENCING.LIBRARY_NAME
    ),
    value_map(
        m={
            r'^([^_]+)_s([^_]+)_([^_]+)_.+\.fastq\.gz$': lambda a, b, c: a,
            r'.+': None
        },
        in_col='file_name',
        out_col=CONCEPT.READ_GROUP.FLOW_CELL
    ),
    value_map(
        m={
            r'^([^_]+)_s([^_]+)_([^_]+)_.+\.fastq\.gz$': lambda a, b, c: b,
            r'.+': None
        },
        in_col='file_name',
        out_col=CONCEPT.READ_GROUP.LANE_NUMBER
    ),
    value_map(
        m={
            r'^([^_]+)_s([^_]+)_([^_]+)_.+\.fastq\.gz$': lambda a, b, c: c,
            r'.+': None
        },
        in_col='file_name',
        out_col=CONCEPT.READ_GROUP.PAIRED_END
    ),
    value_map(
        m={
            r'^([^_]+_[^_]+_)[^_]+_(.+)\.fastq\.gz$': lambda a, b: a+b,
            r'.+': None
        },
        in_col='file_name',
        out_col=CONCEPT.READ_GROUP.ID
    )
]
