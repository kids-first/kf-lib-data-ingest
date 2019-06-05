import os

from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    mapped_df_dict = {os.path.basename(filepath): df
                      for filepath, df in mapped_df_dict.items()}

    # participants w participants
    participants_phenotypes = outer_merge(
        mapped_df_dict['simple_tsv_example1.py'],
        mapped_df_dict['split_rows_tsv_example1.py'],
        on=CONCEPT.PARTICIPANT.ID,
        with_merge_detail_dfs=False,
        left_name='simple_tsv_example1',
        right_name='split_rows_tsv_example1')

    # biospecimens and files
    specimens_files = outer_merge(
        mapped_df_dict['simple_tsv_example2.py'],
        mapped_df_dict['unsimple_xlsx_example1.py'],
        on=CONCEPT.BIOSPECIMEN.ID,
        with_merge_detail_dfs=False,
        left_name='simple_tsv_example2',
        right_name='unsimple_xlsx_example1')

    all_data_df = outer_merge(
        participants_phenotypes,
        specimens_files,
        on=CONCEPT.GENOMIC_FILE.FILE_NAME,
        with_merge_detail_dfs=False,
        left_name='participants_phenotypes',
        right_name='specimens_files')

    return {DEFAULT_KEY: all_data_df}
