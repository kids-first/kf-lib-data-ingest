from copy import deepcopy

import pandas as pd

from etl.transform.standard_model.graph import ConceptGraph
from etl.configuration.log import create_default_logger


class StandardModel(object):

    def __init__(self, logger=None):
        # If we're in stand alone mode (testing)
        if not logger:
            self.logger = create_default_logger(__name__)
        # If this is called from transform stage
        else:
            self.logger = logger

        self.concept_graph = None

    def transform(self, target_api_config, target_concepts_to_transform=None):
        """
        Transform the data in the standard model into a form defined by the
        target_api_config.

        Output the transformed data in a dict where a key is the target
        concept name and the corresponding value is a list of dicts
        each of which represent a target concept instance.

        :param target_api_config: A TargetAPIConfig object containing all
        necessary configuration (target concept schemas, concept relations,
        etc.) needed to perform the transformation.
        :param target_concepts_to_transform: List of strings containing
        target concept names. Transformation will only occur for thse target
        concepts.
        """
        transformed_data = {}

        # Use whole concept set if target concepts are not supplied
        if not target_concepts_to_transform:
            target_concepts_to_transform = (target_api_config
                                            .concept_schemas.keys())

        # The schemas of the target concepts
        schemas = target_api_config.concept_schemas
        # The networkx graph containing how target concepts are related
        relation_graph = target_api_config.relationship_graph

        # For each supplied target concept
        for target_concept in target_concepts_to_transform:
            # Not found in target api config
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
        standard_concept_str = schema['standard_concept']._CONCEPT_NAME

        # Get the ID nodes for this standard concept from the concept graph
        id_node_keys = self.concept_graph.id_index.get(standard_concept_str)
        if not id_node_keys:
            self.logger.warning(
                f'The concept graph does not contain any '
                f'"{standard_concept_str}" ID nodes in the graph! '
                f'Nothing to transform for "{standard_concept_str}"')
            return concept_instances

        # Build a target concept instance for each id
        for id_node_key in id_node_keys:
            output = self._build_concept_instance(id_node_key, schema,
                                                  relation_graph)
            concept_instances.append(output)

        return concept_instances

    def _build_concept_instance(self, id_node_key, schema, relation_graph):
        """
        Build an instance of the target model concept defined by schema

        To do this, make a copy of the target concept schema. Then for each
        property in the target concept schema, try to find the value for that
        property in the concept graph.

        :param id_node_key: a ConceptNode.key string containing the ID of
        the target concept instance in the concept graph
        :param schema: a dict containing the property schema for the
        target concept
        :param relation_graph: a networkx.DiGraph containing target concept
        hierarchical relations
        """
        # Make copy of schema
        output = deepcopy(schema)

        # Get ConceptNode from concept graph given the node's ID
        id_node = self.concept_graph.get_node(id_node_key)

        # Fill in the mapped id value
        output['id'] = id_node.value

        # Find values for properties
        for key, concept_attr in output.get('properties').items():
            value = self.concept_graph.find_attribute_value(id_node,
                                                            concept_attr,
                                                            relation_graph)
            output['properties'][key] = value

        # Find values for links
        links = output.get('links', {})
        for key, concept_attr in links.items():
            value = self.concept_graph.find_attribute_value(id_node,
                                                            concept_attr,
                                                            relation_graph)
            output['links'][key] = value

        return output

    def populate(self, df_dict, logger=None):
        """
        Iterate over DataFrames and insert each cell as a node in the
        concept attribute graph. Then create edges between nodes.

        :param df_dict: a dict of Pandas DataFrames keyed by source URL
        """
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
