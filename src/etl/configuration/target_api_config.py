"""
A class which first validates and then stores transformation configuration
parameters from a target API configuration module.

An instance of this class is used by the transform stage to populate instances
of target model concepts (i.e. participants, diagnoses, etc) with data from
the standard model before those instances are loaded into the target service
(i.e. Kids First Dataservice).

This module must have the following required attributes:

    - target_service_entity_id
    - target_concepts
    - relationships

All but the first of these required attributes must be dicts.

    - target_service_entity_id
    A string containing the name of the unique identifier atttribute a
    target entity. This would likely be a primary key in the target service
    and the attribute used in CRUD operations for this entity.

    - target_concepts
        A dict of dicts. Each inner dict is a target concept dict, which
        contains mappings of target concept properties to standard concept
        attributes.

        A target concept dict has the following schema:

            Required Keys:
                - standard_concept
                - properties
                - endpoint

            Optional Keys:
                - links

        {
            'standard_concept': <reference to the standard concept class>,
            'properties': {
                '<target concept property>': <standard concept attribute>,
                ...
            },
            links: {
                <target concept property>: <standard concept id attribute>
            },
            endpoint: <string containing CRUD endpoint for target concept>
        }

        It may seem unecessary to separate the mappings into
        'properties', and 'links', but this is important because the
        mappings in 'links' are treated differently than those in
        'properties'. The value in a key, value pair under 'links',
        during the transform stage, will be looked up in the standard model,
        and then during the load stage be translated into a target model ID.
        A value in a key, value pair under 'properties', during the transform
        stage, will be looked up in the standard model and then kept as is
        during the load stage.

        For example, after transformation and before loading, the 'links' dict
        could be:

            'links': {
                'participant_id': CONCEPT|PARTICIPANT|ID|P1
            }
        And during the loading stage the 'links' dict will be translated into:
            'links': {
                'participant_id': PT_00001111
            }

    - relationships
        A dict of sets which must represent a directed acyclic graph in the
        form of an adjacency list. The relationships graph codifies the
        parent-child relationships between target concepts. A key in the
        relationships dict should be a string containing a parent target
        concept, and the values should be a set of child target concepts.
"""

import networkx as nx

from etl.configuration.base_config import PyModuleConfig
from etl.transform.standard_model.concept_schema import (
    concept_property_set,
    concept_set
)
from common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)


class TargetAPIConfig(PyModuleConfig):

    def __init__(self, filepath):
        """
        Import python module, validate it, and populate instance attributes
        """
        super().__init__(filepath)
        self.concept_schemas = self.contents.target_concepts
        self.relationship_graph = self._build_relationship_graph(
            self.contents.relationships)

    def _validate(self):
        """
        Validate that the target API config Python module has all required
        attributes and valid content
        """
        self._validate_required_attrs()
        self._validate_target_concepts()
        self._validate_relationships()

    def _validate_required_attrs(self):
        """
        Check that all module level required attributes are present and are
        of the correct form
        """
        # Check that all module required attributes are present
        required_dict_attrs = ['target_concepts', 'relationships']
        required = required_dict_attrs + ['target_service_entity_id']
        for attr in required:
            if not hasattr(self.contents, attr):
                raise AttributeError(
                    f'TargetAPIConfig validation failed! Missing '
                    f'one of the required keys: {attr}')

        # Check that required attributes that are supposed to be dicts
        # are actually dicts
        required_dicts = [getattr(self.contents, attr)
                          for attr in required_dict_attrs]
        assert_all_safe_type(required_dicts, dict)

        # Check that target_service_entity_id is a str
        assert_safe_type(self.contents.target_service_entity_id, str)

    def _validate_target_concepts(self):
        """
        Validate target concept dicts in target_concepts

        Check the format and content of each dict
        """
        # Check that all required keys are present
        self._validate_required_keys()

        # Check that all mapped concepts are all valid standard concepts
        self._validate_mapped_standard_concepts()

        # Check concept attribute mappings
        self._validate_target_concept_attr_mappings()

        # Check that all endpoints are strings
        self._validate_endpoints()

    def _validate_required_keys(self):
        """
        Check that every target concept dict has the required keys
        """
        target_concepts = self.contents.target_concepts

        required_keys = {'standard_concept', 'properties', 'endpoint'}
        for target_concept, target_concept_dict in target_concepts.items():
            for required_key in required_keys:
                if required_key not in target_concept_dict.keys():
                    raise KeyError(
                        'TargetAPIConfig validation failed! Missing one of '
                        f'target concept dict required keys: {required_key}')

    def _validate_mapped_standard_concepts(self):
        """
        Check that all mapped concepts are valid, existing standard concepts
        """
        mapped_concepts = [target_concept_dict.get('standard_concept')
                           for target_concept_dict
                           in self.contents.target_concepts.values()
                           ]
        for mapped_concept in mapped_concepts:
            if mapped_concept not in concept_set:
                raise ValueError(
                    'TargetAPIConfig validation failed! The mapped '
                    f'standard concept: {mapped_concept} does not exist in '
                    'the standard concept set!')

    def _validate_target_concept_attr_mappings(self):
        """
        Validate target concept attribute mappings

        All target concept attributes must be strings
        All mapped concept attributes must be valid standard concept attributes
        """
        target_concepts = self.contents.target_concepts
        keys = {'properties', 'links'}
        for target_concept, target_concept_dict in target_concepts.items():
            for key, attr_mappings in target_concept_dict.items():
                if key not in keys:
                    continue

                # Check attribute mappings
                for target_attr, mapped_attr in attr_mappings.items():
                    # Are all target concept attrs strings
                    try:
                        assert_safe_type(target_attr, str)
                    except TypeError as e:
                        raise Exception(
                            f'TargetAPIAll validation failed! All '
                            'target concept attributes must be strings!'
                        ) from e

                    # Are all mapped concept attributes valid
                    # standard concept attrs?
                    if mapped_attr is None:
                        continue
                    mapped_attr = str(mapped_attr)
                    if mapped_attr not in concept_property_set:
                        raise ValueError(
                            'TargetAPIConfig validation failed! '
                            'all target concept attributes must be mapped to '
                            'an existing standard concept attribute. Mapped '
                            f'attribute {mapped_attr} for target attr '
                            f'{target_concept}.{target_attr} does not exist')

    def _validate_endpoints(self):
        """
        Check that all endpoints are of type str
        """
        endpoints = [target_concept_dict.get('endpoint')
                     for target_concept_dict
                     in self.contents.target_concepts.values()]
        try:
            assert_all_safe_type(endpoints, str)
        except TypeError as e:
            raise Exception(
                'TargetAPIConfig validation failed! All values in "endpoints" '
                'dict must be strings.') from e

    def _validate_relationships(self):
        """
        Validate relationships dict

        Values of keys should be sets and the graph which the relationships
        dict contains must be a directed acyclic graph.
        """
        relationships = self.contents.relationships

        # All values in relationships should be sets
        try:
            assert_all_safe_type(relationships.values(), set)
        except TypeError as e:
            raise Exception(
                'TargetAPIConfig validation failed! All values in '
                'relationships dict must be sets.') from e

        # All keys and values in sets should be existing standard concepts
        for parent_concept, child_concepts in relationships.items():
            if parent_concept not in concept_set:
                raise ValueError(
                    'TargetAPIConfig validation failed! Keys in '
                    'relationships dict must be one of the standard '
                    'concepts. {parent_concept} is not a standard concept.')

            for child_concept in child_concepts:
                if child_concept not in concept_set:
                    raise ValueError(
                        'TargetAPIConfig validation failed! Set values in '
                        'relationships dict must be one of the standard '
                        'concepts. {child_concept} is not a standard concept.')

        # Check for cycles
        # It's weird that find_cycle raises an exception if no cycle is found,
        # oh well
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
                'TargetAPIConfig validation failed! Invalid `relationships` '
                'graph! `relationships` MUST be a directed acyclic graph. '
                f'The cycle: {edges} was detected in the graph.')

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
