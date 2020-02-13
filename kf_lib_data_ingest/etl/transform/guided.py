"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the mapped
source data tables should be merged into a single table containing all of the
mapped source data
"""
import os
from collections import defaultdict

import pandas

from kf_lib_data_ingest.common.concept_schema import CONCEPT, concept_attr_from
from kf_lib_data_ingest.common.misc import clean_up_df
from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
)
from kf_lib_data_ingest.config import DEFAULT_KEY
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule,
)
from kf_lib_data_ingest.etl.transform.transform import TransformStage


class GuidedTransformStage(TransformStage):
    def __init__(self, transform_function_path, *args, **kwargs):
        self.transform_module = TransformModule(transform_function_path)
        super().__init__(*args, **kwargs)
        self.transform_func_dir = os.path.join(
            self.stage_cache_dir, "transform_function_output"
        )

    def _apply_transform_funct(self, data_dict):
        """
        Apply user supplied transform function to merge dataframes in data_dict
        into a new set of dataframes, so that each target entity has 1
        dataframe with all of its mapped data from extract stage.

        :param df_dict: a dict of mapped dataframes from the ExtractStage
        :type df_dict: dict
        :returns: a dict of dataframes
        """
        filepath = self.transform_module.config_filepath
        self.logger.info(
            "Applying user supplied transform function " f"{filepath} ..."
        )

        # Reformat the data_dict into expected form for transform_funct
        # Turn data_dict[<extract config file>] = (source data url, dataframe)
        # into df_dict[<extract config file>] = dataframe
        df_dict = {k: v[1] for k, v in data_dict.items()}

        # Apply user supplied transform function
        transform_funct = self.transform_module.transform_function
        merged_df_dict = transform_funct(df_dict)

        # Validation of transform function output
        assert_safe_type(merged_df_dict, dict)
        assert_all_safe_type(merged_df_dict.keys(), str)
        assert_all_safe_type(merged_df_dict.values(), pandas.DataFrame)

        return merged_df_dict

    def _validate_data(self, df_dict):
        """
        Do data validation on the transform function output that warrants
        stopping pipeline execution so that the user can go back and fix their
        data or configs

        BIOSPECIMEN Validation
            - Any BIOSPECIMEN data must contain both the BIOSPECIMEN.ID and
            BIOSPECIMEN_GROUP.ID concept identifiers

        :param df_dict: output of the user transform function. A dict of Pandas
        DataFrames
        :type df_dict: dict
        """
        self.logger.info("Start data validation on transform function output")

        # Biospecimen - must have both BIOSPECIMEN.ID and BIOSPECIMEN_GROUP.ID
        # columns
        biospecimen_df = df_dict.get("biospecimen", df_dict.get(DEFAULT_KEY))
        # No DataFrame found - pass
        if not isinstance(biospecimen_df, pandas.DataFrame):
            self.logger.debug(
                "Skipped validation of biospecimen data. "
                "Did not find a DataFrame for biospecimen"
            )
        # Neither column is found - pass
        elif (CONCEPT.BIOSPECIMEN_GROUP.ID not in biospecimen_df) and (
            CONCEPT.BIOSPECIMEN.ID not in biospecimen_df
        ):
            self.logger.debug(
                "Skipped validation of biospecimen data. "
                f"Did not find any {CONCEPT.BIOSPECIMEN._CONCEPT_NAME} "
                f"or {CONCEPT.BIOSPECIMEN_GROUP._CONCEPT_NAME} concepts "
                "in the data"
            )
        # Only one of the two columns found - fail
        else:
            self.logger.debug("Validating biospecimen data")
            valid = (
                CONCEPT.BIOSPECIMEN.ID in biospecimen_df
                and CONCEPT.BIOSPECIMEN_GROUP.ID in biospecimen_df
            )
            if not valid:
                raise Exception(
                    "❌ Invalid biospecimen DataFrame in transform function "
                    "output! DataFrame must have both a "
                    f"{CONCEPT.BIOSPECIMEN.ID} column and a "
                    f"{CONCEPT.BIOSPECIMEN_GROUP.ID} column. Check your "
                    "transform module logic or extract configs."
                )
            else:
                self.logger.debug("✅ Valid biospecimen DataFrame")

        self.logger.info(
            "Completed data validation on transform function output"
        )

    def _run(self, data_dict):
        """
        See TransformStage._run
        """
        # Apply user transform func
        output = self._apply_transform_funct(data_dict)

        # Validate transform func output
        self._validate_data(output)

        # Clean up dfs
        for key, df in output.items():
            output[key] = clean_up_df(df)

        return output

    def _postrun_concept_discovery(self, run_output):
        """
        See the docstring for IngestStage._postrun_concept_discovery
        """
        sources = defaultdict(dict)
        # Skip columns which might be set differently in ExtractStage vs
        # TransformStage
        skip_cols = ["VISIBLE"]
        for df_name, df in run_output.items():
            cols = [
                col
                for col in df.columns
                if concept_attr_from(col) not in skip_cols
            ]
            self.logger.info(
                f"Doing concept discovery for {df_name} DataFrame in "
                "transform function output"
            )
            for key in cols:
                sk = sources[key]
                for val in df[key]:
                    # sources entry
                    sk[val] = True

        return {"sources": sources}
