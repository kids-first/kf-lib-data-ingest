"""
Module for transforming source data DataFrames to the standard model.
"""
import os
from pprint import pformat

import pandas

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from kf_lib_data_ingest.common.misc import (
    read_json,
    write_json
)
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)
from kf_lib_data_ingest.etl.transform.common import insert_unique_keys
from kf_lib_data_ingest.etl.transform.auto import AutoTransformer
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformer


class TransformStage(IngestStage):
    def __init__(self, target_api_config_path,
                 target_api_url=None, ingest_output_dir=None,
                 transform_function_path=None):

        super().__init__(ingest_output_dir=ingest_output_dir)

        self.target_api_url = target_api_url
        self.target_api_config = TargetAPIConfig(target_api_config_path)

        if not transform_function_path:
            # ** Temporary - until auto transform is further developed **
            raise FileNotFoundError(
                'Transform module file has not been created yet! '
                'You must define a transform function in order for ingest '
                'to continue.')
            # self.transformer = AutoTransformer(self.target_api_config)
        else:
            self.transformer = GuidedTransformer(self.target_api_config,
                                                 transform_function_path)

    def _read_output(self):
        """
        Read previously written transform stage output

        :returns: dict (keyed by target concepts) of lists of dicts
        representing target concept instances (i.e. participant, biospecimen,
        etc)
        """
        output = {
            os.path.splitext(filename)[0]: read_json(
                os.path.join(self.stage_cache_dir, filename))
            for filename in os.listdir(self.stage_cache_dir)
            if filename.endswith('.json')
        }
        self.logger.info(f'Reading {type(self).__name__} output:\n'
                         f'{pformat(list(output.keys()))}')

        return output

    def _write_output(self, output):
        """
        Write output of transform stage to JSON file

        :param output: output created by TransformStage._run
        :type output: a dict of pandas.DataFrames
        """
        assert_safe_type(output, dict)
        assert_all_safe_type(output.values(), list)
        paths = []
        for key, data in output.items():
            fp = os.path.join(self.stage_cache_dir, key + '.json')
            paths.append(fp)
            write_json(data, fp)
        self.logger.info(f'Writing {type(self).__name__} output:\n'
                         f'{pformat(paths)}')

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
            for extract_config_url, df in data_dict.values():
                assert_safe_type(extract_config_url, str)
                assert_safe_type(df, pandas.DataFrame)

        except TypeError as e:
            raise InvalidIngestStageParameters from e

    def _run(self, data_dict):
        """
        Transform the tabular mapped data into a dict of lists.
        Keys are target entity types and values are lists of target entity
        dicts.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined in _validate_run_parameters.
        :returns target_instances: dict - keyed by target concept -
        of lists containing dicts - representing target concept instances.
        """
        # Insert unique key columns before running transformation
        insert_unique_keys(data_dict)

        target_instances = self.transformer.run(data_dict)

        return target_instances
