"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the source data tables
should be merged in order to yield a single table per target service entity.
"""
from pandas import DataFrame

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from kf_lib_data_ingest.etl.transform.standard_model.model import StandardModel
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)


class GuidedTransformer(IngestStage):
    def __init__(self, target_api_config, transform_function_path):
        super().__init__()
        self.transform_module = TransformModule(transform_function_path)
        self.target_api_config = target_api_config

    def _read_output(self):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _write_output(self, output):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _validate_run_parameters(self, data_dict):
        """
        Implemented by TransformStage
        """
        pass

    def _run(self, data_dict):
        """
        Transform the tabular mapped data into a unified standard form,
        then transform again from the standard form into a dict of lists.
        Keys are target entity types and values are lists of target entity
        dicts.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined in _validate_run_parameters.
        """
        target_entities = {}

        return target_entities
