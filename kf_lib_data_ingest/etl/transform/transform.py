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
    write_json,
    get_swagger_schema
)
from kf_lib_data_ingest.config import (
    DEFAULT_TARGET_URL
)
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)
from kf_lib_data_ingest.etl.transform.common import insert_unique_keys
from kf_lib_data_ingest.etl.transform.auto import AutoTransformer
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformer


class TransformStage(IngestStage):
    def __init__(self, target_api_config_path,
                 target_api_url=DEFAULT_TARGET_URL, ingest_output_dir=None,
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

    def handle_nulls(self, target_instances, target_schema):
        """
        Convert null property values in `target_instances` to acceptable values
        based on the type of property as defined in target schema.

        See kf_lib_data_ingest.common.misc.get_swagger_schema for expected
        format of target_schema

        `target_instances` is a dict keyed by the `target_concepts` defined in
        this module. The values are lists of dicts, where a dict in the list
        takes on the same form as the dicts in `target_concepts`.

        :param target_instances: a dict of lists containing dicts
        :param target_schema: the target service entity schemas
        :returns target_instances: Updated version of input
        """
        version = target_schema.get('version')
        self.logger.info(f'Do null processing using target schema '
                         f'{target_schema.get("target_service")}, '
                         f'version {pformat(version)}')
        schemas = target_schema.get('definitions')

        for target_concept, list_of_instances in target_instances.items():
            # Get schema for target concept
            schema = schemas.get(target_concept)
            if not schema:
                self.logger.info(
                    f'Skip handle nulls for {target_concept}. No schema '
                    'was found.')
            # Convert nulls
            for i, instance in enumerate(list_of_instances):
                for attr, value in instance['properties'].items():
                    if value is not None:
                        continue

                    property_def = schema['properties'].get(attr)
                    mapped_value = None
                    if not property_def:
                        self.logger.warning(
                            'No property definition found for '
                            f'{target_concept}.{attr} in target schema '
                            'This property may not exist '
                            'anymore in the target service.'
                        )
                    else:
                        if property_def['type'] == 'string':
                            mapped_value = constants.COMMON.NOT_REPORTED
                            # Note - the dataservice should tell us if the
                            # format of the string is `date-time`, then we
                            # could do:
                            #   property_def.get('format') == 'date-time'.
                            # Right now it doesn't do this.
                            # That's what we should use to determine what null
                            # value to use. For now, we're just using the name
                            # of the attribute
                            if 'date' in attr:
                                mapped_value = None
                        else:
                            mapped_value = None

                    instance['properties'][attr] = mapped_value

        return target_instances

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

        # Guided or auto transformation
        target_instances = self.transformer.run(data_dict)

        # Null processing
        target_api_schema = get_swagger_schema(self.target_api_url,
                                               list(target_instances.keys()),
                                               logger=self.logger)
        if target_api_schema:
            self.handle_nulls(target_instances, target_api_schema)
        else:
            self.logger.warning('Skipping null processing because no target '
                                'schema was found')

        return target_instances
