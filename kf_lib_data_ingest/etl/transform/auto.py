"""
Module for 'auto' transforming source data into target service entities.
Auto transformation does not require guidance from the user on how to merge
source data tables.
"""
import os
import logging

import pandas

from kf_lib_data_ingest.etl.transform.standard_model.model import StandardModel
from kf_lib_data_ingest.etl.transform.standard_model.graph import export_to_gml


class AutoTransformer():
    def __init__(self, target_api_config):
        self.logger = logging.getLogger(type(self).__name__)
        self.target_api_config = target_api_config

    def write_output(self, output_dir):
        """
        Write concept graph to GML file

        See etl.transform.standard_model.graph.export_to_gml for details
        """
        filepath = os.path.join(output_dir, 'graph.gml')
        concept_graph = self.model.concept_graph
        export_to_gml(concept_graph, filepath=filepath)

        self.logger.info(f'Writing {type(self).__name__} output:\n'
                         f'{filepath}')

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
        self.model = StandardModel()
        self.model.populate(data_dict)

        # Transform the concept graph into target entities
        target_entities = self.model.transform(self.target_api_config)

        dfs = {}
        for entity, payloads in target_entities.items():
            for p in payloads:
                p['standard_concept'] = p['standard_concept']._CONCEPT_NAME
            dfs[entity] = pandas.DataFrame(payloads)

        return dfs
