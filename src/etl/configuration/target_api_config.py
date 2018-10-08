"""
A class which first validates and then stores transformation configuration
parameters from a target API configuration module.

An instance of this class is used by the transform stage to populate instances
of target model concepts (i.e. participants, diagnoses, etc) with data from
the standard model before those instances are loaded into the target service
(i.e. Kids First Dataservice).

This module must have the following required attributes:

    - target_concepts
    - relationships
    - endpoints

These required attributes must be dicts.

    - target_concepts
        A dict of dicts. Each inner dict is a target concept dict, which
        contains mappings of target concept properties to standard concept
        attributes.

        A target concept dict has the following schema:

            Required Keys:
                - id
                - properties
            Optional Keys:
                - links

        {
            'id': {
                <target concept identifier property>:
                    <standard concept attribute>
            },
            'properties': {
                '<target concept property>': <standard concept attribute>,
                ...
            },
            links: {
                <target concept property>: <standard concept>
            }
        }

        It may seem unecessary to separate the mappings into
        'id', 'properties', and 'links', but this is important because the
        mappings in 'id' and 'links' are treated differently than those in
        'properties'. The value in a key, value pair under 'id' or 'links',
        during the transform stage, will be looked up in the standard model,
        and then during the load stage be translated into a target model ID.
        A value in a key, value pair under 'properties', during the transform
        stage, will be looked up in the standard model and then kept as is
        during the load stage.

        For example, after transformation and before loading, the 'id' dict
        could be:

            'id': {
                'kf_id': CONCEPT|PARTICIPANT|ID|P1
            }
        And during the loading stage the 'id' dict will be translated into:
            'id': {
                'kf_id': PT_00001111
            }

    - relationships
        A dict of sets which represents an adjacency list. The adjacency list
        codifies the parent-child relationships between target concepts. A
        key in the relationships dict should be a string containing a parent
        target concept, and the values should be a set of child target
        concepts.

    - endpoints
        A dict of key value pairs where the key should be a target concept
        and the value should be an endpoint in the target service to perform
        create, read, update, and delete operations for that target concept.
"""

import networkx as nx

from etl.configuration.base_config import PyModuleConfig
from etl.transform.standard_model.concept_schema import concept_property_set


class TargetAPIConfig(PyModuleConfig):

    def __init__(self, filepath):
        """
        Import python module, validate it, and populate instance attributes
        """
        super().__init__(filepath)
        self._deserialize()

    def tbd_validate(self):
        # TODO - Come back to validation later.
        # Leave actual validation blank for now because we need to figure out
        # what should go in target API config first. The best way to do that is
        # to make a sample kids first API config, write the actual
        # transformation code and try ou the sample config. Too difficult to
        # design this config without knowing exactly how we want to use it.
        pass

    def _validate(self):
        """
        Validate that the target API config Python module has all required
        attributes and valid content
        """
        required = {'target_concepts', 'relationships', 'endpoints'}
        for attr in required:
            # Check for existence of required attributes in config module
            try:
                config_item = getattr(self.contents, attr)
            except AttributeError as e:
                raise AttributeError(f'Attribute `{attr}` is one of the '
                                     f' required attributes in all target API '
                                     ' config modules')

            # Each required attribute should be a dict
            if not isinstance(config_item, dict):
                raise TypeError(f'{attr} must be of type dict')

        # Validate content of required attributes in module
        self._validate_target_concepts()
        self._validate_endpoints()
        self._validate_relationships()

    def _validate_target_concepts(self):
        """
        Validate target concept dicts in target_concepts
        """
        target_concepts = self.contents.target_concepts
        required_keys = {'id', 'properties'}
        for target_concept, target_concept_dict in target_concepts.items():
            # Are all required keys present
            for required_key in required_keys:
                if required_key not in set(target_concept_dict.keys()):
                    raise KeyError('Every target concept dict must '
                                   'have the required keys: '
                                   f'{required_keys}')
                # Validate 'id' mapping
                if required_key == 'id':
                    # There can only be 1 id mapping
                    id_mapping_count = len(target_concept_dict
                                           .get(required_key).keys())
                    if id_mapping_count > 1:
                        raise ValueError(
                            "A target concept mapping can only have 1 "
                            f"mapping for 'id'! {target_concept} has "
                            "{id_mapping_count} mappings.")

            # Keys of mappings must be strings
            # Values must be valid standard concept attributes or None
            keys = set(list(required_keys) + ['links'])
            for k in keys:
                # Target concept mappings
                prop_value_pairs = target_concept_dict.get(k)
                if not prop_value_pairs:
                    continue
                for prop, value in prop_value_pairs.items():
                    # Mapping key validation
                    if not isinstance(prop, str):
                        raise KeyError(
                            'All mappings must have string type keys.'
                            f'Target model concept property: {prop} in '
                            f'{target_concept}.{k} is not of type string.')

                    # Mapping value validation
                    if value is None:
                        continue
                    value = str(value)
                    if value not in concept_property_set:
                        raise ValueError(
                            f'Invalid value mapping: '
                            f'{target_concept}.{k}.{prop} = {value}. '
                            f'Standard concept attribute {value} does not '
                            'exist.'
                        )

    def _validate_endpoints(self):
        """
        Validate endpoints dict
        """
        # All values in endpoints should be strings
        endpoints = self.contents.endpoints
        values_are_strs = all([isinstance(v, str)
                               for v in endpoints.values()])
        if not values_are_strs:
            raise ValueError("All values in 'endpoints' dict must be "
                             "strings.")

    def _validate_relationships(self):
        """
        Validate relationships dict
        """
        # All values in relationships should be sets
        relationships = self.contents.relationships
        values_are_sets = all([isinstance(v, set)
                               for v in relationships.values()])
        if not values_are_sets:
            raise ValueError("All values in 'relationships' dict must "
                             "be sets.")

        # Check for cycles
        rg = self._build_relationship_graph(relationships)
        edges = None
        try:
            edges = nx.find_cycle(rg, orientation='original')
        # No cycles found - pass validation
        except nx.exception.NetworkXNoCycle:
            pass
        # A cycle was found
        else:
            raise ValueError(
                f'Invalid `relationships` graph! `relationships` graph '
                'MUST be a directed acyclic graph. The cycle: {edges} was '
                'detected in the graph.')

    def _deserialize(self):
        """
        Populate attributes of the TargetAPIConfig class from the target API
        config Python module

        The 'relationships' dict will be stored networkX directed graph.
        """
        self.concept_schemas = self.contents.target_concepts
        self.relationship_graph = self._build_relationship_graph(
            self.contents.relationships)
        self.endpoint_map = self.contents.endpoints

    def _build_relationship_graph(self, relationships):
        """
        Build a networkX directed graph from the relationships dict which
        represents an adjacency list.

        This graph codifies the parent-child relationships among target model
        concepts. It is later used in the transform stage when searching
        the concept graph for the value of a target concept attribute.
        """
        graph = nx.DiGraph()

        for node, neighbors in relationships.items():
            # Node key is concept name from concept instance
            node = node._CONCEPT_NAME
            # Add node if not in graph
            if not graph.has_node(node):
                graph.add_node(node)
            # Add directed edge from node to neighbor
            for n in neighbors:
                # Node key is concept name from concept instance
                n = n._CONCEPT_NAME
                # Add neighbor node if not in graph
                if not graph.has_node(n):
                    graph.add_node(n)
                graph.add_edge(node, n)

        return graph
