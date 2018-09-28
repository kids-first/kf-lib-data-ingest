from copy import deepcopy

import pandas as pd

from etl.transform.standard_model.graph import ConceptGraph
from etl.transform.standard_model.concept_schema import concept_from


class StandardModel(object):

    def __init__(self):
        self.concept_graph = None
        self.logger = None

    def transform(self, target_api_config, target_concepts_to_transform=None):
        """
        Transform the data in the standard model into a form defined by the
        target_api_config.

        Output the transformed data in a dict where a key is the target
        concept name and the corresponding value is a list of dicts
        representing the target concept instances.

        :param target_api_config: A TargetAPIConfig object containing all
        necessary configuration (defined target concept schemas,
        concept relations, etc.) needed to perform the transformation.
        :param target_concepts_to_transform: List of strings containing
        target concept names. Only target concepts in this list will be
        extracted from the standard model.
        """
        # Use whole concept set if concepts not supplied
        if not target_concepts_to_transform:
            target_concepts_to_transform = (target_api_config
                                            .concept_schemas.keys())

        # Output
        transformed_data = {}
        # The schemas of the target concepts
        schemas = target_api_config.concept_schemas
        # The networkx graph containing how target concepts are related
        relation_graph = target_api_config.relationship_graph

        # For each supplied target concept
        for target_concept in target_concepts_to_transform:
            # Supplied target concept not found in target api config
            if target_concept not in schemas:
                fp = target_api_config.config_filepath
                self.logger.warning(
                    f'Supplied entity "{target_concept}" does not '
                    'exist in target concept schemas. Check defined concepts '
                    f'in {fp}')
                continue

            # Build target concept instances
            schema = schemas[target_concept]
            concept_instances = self._build_concept_instances(target_concept,
                                                              schema,
                                                              relation_graph)
            transformed_data.setdefault(target_concept,
                                        []).extend(concept_instances)

        return transformed_data

    def _build_concept_instances(self, target_concept, schema, relation_graph):
        """
        Build target concept instances for a particular target concept

        :param target_concept: a string denoting the name of the target concept
        :param schema: a dict containing the properties of the target concept
        instance, whose values will be extracted from the standard model graph
        :param relation_graph: a networkx.DiGraph representing relationships
        among target concepts in the target model.
        """
        # Initialize list of output payloads for the target concept
        concept_instances = []

        # Get the standard concept mapped to this target concept
        standard_concept_id_str = list(schema['id'].values())[0]
        standard_concept = concept_from(standard_concept_id_str)

        # Get the ID nodes for this standard concept from the concept graph
        id_nodes = self.concept_graph.id_index.get(standard_concept)
        if not id_nodes:
            self.logger.warning(
                f'The concept graph does not contain any '
                f'"{standard_concept}" ID nodes in the graph! '
                f'Nothing to transform for "{standard_concept}"')
            return concept_instances

        for id_node in id_nodes:
            output = self._build_concept_instance(id_node, schema,
                                                  relation_graph)
            concept_instances.append(output)

        return concept_instances

    def _build_concept_instance(self, id_node, schema, relation_graph):
        # TODO
        output = deepcopy(schema)

        return output

    def populate(self, df_dict, logger=None):
        """
        Iterate over DataFrames and insert each cell as a node in the
        concept attribute graph. Then create edges between nodes.

        :param df_dict: a dict of Pandas DataFrames keyed by source URL
        """
        # If we're in stand alone mode (testing)
        if not logger:
            import logging
            self.logger = logging.getLogger(__name__)

        # If this is called from transform stage
        else:
            self.logger = logger

        # Create an empty concept graph
        self.concept_graph = ConceptGraph(logger=logger)

        # Insert nodes into concept attribute graph
        # For each DataFrame
        for extract_config_url, (source_file_url, df) in df_dict.items():
            # For each row
            for r, row in df.iterrows():
                id_nodes = []
                attribute_nodes = []
                # For each column
                for c, col in enumerate(df.columns):
                    props = {
                        'extract_config_url': extract_config_url,
                        'source_file_url': source_file_url,
                        'row': r,
                        'col': c
                    }
                    # Do not add null nodes to the graph
                    if pd.isnull(row[col]):
                        continue
                    # Add node to graph
                    node = self.concept_graph.add_or_get_node(col, row[col],
                                                              **props)
                    # Sort nodes into ID nodes and attribute nodes
                    if node.is_identifier:
                        id_nodes.append(node)
                    else:
                        attribute_nodes.append(node)

                # Connect id nodes
                self.concept_graph.connect_id_nodes(id_nodes)

                # Connect attribute nodes
                for prop_node in attribute_nodes:
                    self.concept_graph.connect_attribute_node(
                        prop_node, id_nodes
                    )


if __name__ == '__main__':
    pass
