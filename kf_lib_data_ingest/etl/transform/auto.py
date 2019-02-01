"""
Module for 'auto' transforming source data into target service entities.
Auto transformation does not require guidance from the user on how to merge
source data tables.
"""
import logging

from kf_lib_data_ingest.etl.transform.standard_model.model import StandardModel


class AutoTransformer():
    def __init__(self, target_api_config):
        self.logger = logging.getLogger(type(self).__name__)
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
        self.logger.info('Begin auto transformation ...')

        # Insert mapped dataframes into the standard model
        model = StandardModel()
        model.populate(data_dict)

        # Transform the concept graph into target entities
        target_entities = model.transform(self.target_api_config)

        return target_entities
