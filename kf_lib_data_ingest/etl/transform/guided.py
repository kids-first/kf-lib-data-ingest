"""
Module for transforming source data into target service entities via
a user supplied transform function which specifies how the source data tables
should be merged in order to yield a single table per target service entity.
"""

from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)


class GuidedTransformer():
    def __init__(self, target_api_config, transform_function_path):
        self.transform_module = TransformModule(transform_function_path)
        self.target_api_config = target_api_config

    def run(self, data_dict):
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
