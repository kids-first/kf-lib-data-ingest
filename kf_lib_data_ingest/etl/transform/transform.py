"""
Module for transforming source data DataFrames to the standard model.
"""
import os
from abc import abstractmethod
from pprint import pformat

import pandas

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.io import read_df
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
)


class TransformStage(IngestStage):
    def _read_output(self):
        """
        Read previously written transform stage output

        :return: dict (keyed by target concepts) of pandas.DataFrames
        representing target concept instances (i.e. participant, biospecimen,
        etc)
        :rtype: dict
        """
        output = {
            os.path.splitext(filename)[0]: read_df(
                os.path.join(self.stage_cache_dir, filename)
            )
            for filename in os.listdir(self.stage_cache_dir)
            if filename.endswith(".tsv")
        }
        self.logger.info(
            f"Reading {self.stage_type.__name__} output:\n"
            f"{pformat(list(output.keys()))}"
        )

        return output

    def _write_output(self, output):
        """
        Write output of transform stage.

        :param output: output created by TransformStage._run
        :type output: a dict of pandas.DataFrames
        """
        assert_safe_type(output, dict)
        assert_all_safe_type(output.values(), pandas.DataFrame)

        paths = []

        # Write transform func output to disk
        os.makedirs(self.stage_cache_dir, exist_ok=True)
        for key, df in output.items():
            fp = os.path.join(self.stage_cache_dir, key + ".tsv")
            paths.append(fp)
            df.to_csv(fp, sep="\t", index=False)

        self.logger.info(
            f"Writing {self.stage_type.__name__} output:\n" f"{pformat(paths)}"
        )

    def _validate_run_parameters(self, data_dict):
        """
        Validate the parameters being passed into the _run method. This
        method gets executed before the body of _run is executed.

        A key in df_dict should be a string containing the URL to the
        extract config module used to produce the Pandas DataFrame in the
        value tuple.

        A value in df_dict should be a tuple where the first member is a
        string containing the URL to the source data file, and the second
        member of the tuple is a Pandas DataFrame containing the mapped
        source data.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined above.
        """
        try:
            # Check types
            assert_safe_type(data_dict, dict)
            assert_all_safe_type(data_dict.keys(), str)

            # Check that values are tuples of (string, DataFrames)
            for extract_config_url, df in data_dict.items():
                assert_safe_type(extract_config_url, str)
                assert_safe_type(df, pandas.DataFrame)

        except TypeError as e:
            raise InvalidIngestStageParameters from e

        if not data_dict:
            raise Exception(
                f"{self.stage_type.__name__} received no input. "
                f"Did the previous stage produce anything?"
            )

    @abstractmethod
    def _run(self, data_dict):
        """
        Transform the tabular mapped data into a unified form.

        :param data_dict: the output (a dict of mapped DataFrames) from
        ExtractStage.run. See TransformStage._validate_run_parameters for
        a more detailed description.
        :type data_dict: dict
        """
        pass
