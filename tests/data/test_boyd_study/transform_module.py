import os
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    d = {os.path.basename(k): v for k, v in mapped_df_dict.items()}

    # Family, participant, biospecimen, diagnosis, phenotype, outcome
    clinical = d["Boyd_Minimal_Data_Elements_Cuellar_9-2-2018_EDITED_v2_map.py"]
    # S3 info + read groups + sequencing library
    s3_files = d["s3_files_kf-seq-data-hudsonalpha_haib17SB5136_map.py"]
    # Specimen manifest with sequencing library
    manifest = d["manifest_180917_rectified_map.py"]

    # Genomic files and manifest with specimen IDs
    bs_gf = outer_merge(
        manifest,
        s3_files,
        with_merge_detail_dfs=False,
        on=CONCEPT.SEQUENCING.LIBRARY_NAME,
    )

    all_data_df = outer_merge(
        clinical, bs_gf, on=CONCEPT.BIOSPECIMEN.ID, with_merge_detail_dfs=False
    )

    return {DEFAULT_KEY: all_data_df}
