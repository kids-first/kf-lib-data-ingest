"""
Auto-generated transform module

Replace the contents of transform_function with your own code

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing transform_function.
"""
import os

# Use these merge funcs, not pandas.merge
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    dfs = {
        os.path.basename(fp): df
        for fp, df in
        mapped_df_dict.items()
    }

    clinical_df = dfs['clinical.py']
    family_and_phenotype_df = dfs['family_and_phenotype.py']

    merged = outer_merge(clinical_df,
                         family_and_phenotype_df,
                         on=CONCEPT.BIOSPECIMEN.ID,
                         with_merge_detail_dfs=False)

    return {DEFAULT_KEY: merged}
