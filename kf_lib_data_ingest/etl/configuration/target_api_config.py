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
            'standard_concept':
                type: etl.transform.standard_model.concept_schema.CONCEPT
                example: CONCEPT.PARTICIPANT
                description: the standard concept that this target concept
                maps to.

            'properties':
                type: dict

                    key: a string containing the target concept property
                    value: a standard concept attribute from
                    etl.transform.standard_model.concept_schema

                example:
                {
                    'external_id': CONCEPT.PARTICIPANT.ID,
                    'race': CONCEPT.PARTICIPANT.RACE
                }
                description: the target concept property mappings to standard
                concept attributes
            },
            links:
                type: dict

                    key: a string containing the target concept property
                    value: a standard concept UNIQUE_KEY attribute from
                    etl.transform.standard_model.concept_schema

                example:
                {
                    'family_id': CONCEPT.FAMILY.UNIQUE_KEY
                }
                description: identifiers which map to standard concept
                UNIQUE_KEY attributes. These represent foreign keys in the
                target model.
            ,
            'endpoint':
                type: string
                example: '/participants'
                description: the CRUD endpoint for the concept in the target
                service
        }

        It may seem unecessary to separate the mappings into
        'properties', and 'links', but this is important because the
        mappings in 'links' are treated differently than those in
        'properties'.

        The value in a key, value pair under 'links', during the transform
        stage, will be looked up in the standard model, and then during the
        load stage be translated into a target model ID. A value in a key,
        value pair under 'properties', during the transform stage, will be
        looked up in the standard model and then kept as is during the load
        stage.

        For example, after transformation and before loading, the 'links' dict
        could be:

            'links': {
                'participant_id': 'CONCEPT|PARTICIPANT|UNIQUE_KEY|P1'
            }
        And during the loading stage the 'links' dict will be translated into:

            'links': {
                'participant_id': 'PT_00001111'
            }

    - relationships
      A dict of sets which must represent a directed acyclic graph in the
      form of an adjacency list. The relationships graph codifies the
      parent-child relationships between target concepts. A key in the
      relationships dict should be a string containing a parent target
      concept, and the values should be a set of child target concepts.
"""

import networkx as nx

from kf_lib_data_ingest.etl.configuration.base_config import PyModuleConfig
from kf_lib_data_ingest.common.concept_schema import (
    concept_property_set,
    concept_set
)
from kf_lib_data_ingest.common.type_safety import (
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
        Validate that the target API config Python module has all the
        required content with the right types
        """
        self._validate_required_attrs()
        self._validate_target_concepts()
        self._validate_relationships()

    def _validate_required_attrs(self):
        """
        Check that all module level required attributes are present and are
        of the correct form
        """
        required_dict_attrs = ['target_concepts', 'relationships']
        required = required_dict_attrs + ['target_service_entity_id']
        for attr in required:
            if not hasattr(self.contents, attr):
                raise AttributeError(
                    f'Missing one of the required keys: {attr}')

        # Check that required attributes that are supposed to be dicts
        # are actually dicts
        required_dicts = [getattr(self.contents, attr)
                          for attr in required_dict_attrs]
        assert_all_safe_type(required_dicts, dict)

        # Check that target_service_entity_id is a string
        assert_safe_type(self.contents.target_service_entity_id, str)

    def _validate_target_concepts(self):
        """
        Validate target concept dicts in target_concepts

        Check the format and content of each dict.
        """
        # Check that all required keys are present
        self._validate_required_keys()

        # Check that all mapped concepts are all valid standard concepts
        self._validate_mapped_standard_concepts()

        # Check that all properties are mapped to valid standard concept attrs
        self._validate_target_concept_attr_mappings()

        # Check that all endpoints are strings
        self._validate_endpoints()

    def _validate_required_keys(self):
        """
        Check that every target concept dict has the required keys:
            - standard_concept
            - properties
            - endpoint
        """
        target_concepts = self.contents.target_concepts

        required_keys = {'standard_concept', 'properties', 'endpoint'}
        for target_concept, target_concept_dict in target_concepts.items():
            for required_key in required_keys:
                if required_key not in target_concept_dict.keys():
                    raise KeyError(
                        f'Dict for {target_concept} is missing one of '
                        f'target concept dict required keys: {required_key}')

    def _validate_mapped_standard_concepts(self):
        """
        Check that each target dict has a valid mapping for
        standard_concept.

        The mapped value must be an existing standard concept
        in etl.transform.standard_model.concept_schema
        """
        mapped_concepts = [target_concept_dict.get('standard_concept')
                           for target_concept_dict
                           in self.contents.target_concepts.values()
                           ]
        for mapped_concept in mapped_concepts:
            if mapped_concept not in concept_set:
                raise ValueError(
                    f'The mapped standard concept: {mapped_concept} does not '
                    'exist in the standard concept set!')

    def _validate_target_concept_attr_mappings(self):
        """
        Validate target concept attribute mappings

        All target concept attributes must be strings
        All mapped values must be valid standard concept attributes in
        etl.transform.standard_model.concept_schema
        """
        target_concepts = self.contents.target_concepts
        keys = {'properties', 'links'}

        # For each target_concept_dict
        for target_concept, target_concept_dict in target_concepts.items():
            for key, attr_mappings in target_concept_dict.items():
                if key not in keys:
                    continue

                # Are all keys of 'properties' and 'links' strings
                assert_all_safe_type(attr_mappings.keys(), str)

                # Are all mapped values valid standard concept attributes?
                for target_attr, mapped_attr in attr_mappings.items():
                    if mapped_attr is None:
                        continue

                    mapped_attr = str(mapped_attr)
                    if mapped_attr not in concept_property_set:
                        raise ValueError(
                            f'Error in dict for {target_concept} '
                            'All target concept attributes must be mapped to '
                            'an existing standard concept attribute. Mapped '
                            f'attribute {mapped_attr} for target attr '
                            f'{target_concept}.{target_attr} does not exist')

    def _validate_endpoints(self):
        """
        Check that all endpoints are strings
        """
        endpoints = [target_concept_dict.get('endpoint')
                     for target_concept_dict
                     in self.contents.target_concepts.values()]

        assert_all_safe_type(endpoints, str)

    def _validate_relationships(self):
        """
        Validate relationships dict

        Keys should be existing standard concepts
        Values should be sets containing existing standard concepts
        The relationships dict is an adjacency list which represents a directed
        acyclic graph. There should be no cycles in the graph.
        """
        relationships = self.contents.relationships

        # All values in relationships should be sets
        assert_all_safe_type(relationships.values(), set)

        # All keys and values in sets should be existing standard concepts
        for parent_concept, child_concepts in relationships.items():
            if parent_concept not in concept_set:
                raise ValueError(
                    'Keys in relationships dict must be one of the standard '
                    f'concepts. {parent_concept} is not a standard concept.')

            for child_concept in child_concepts:
                if child_concept not in concept_set:
                    raise ValueError(
                        'Set values in relationships dict must be one of the '
                        f'standard concepts. {child_concept} is not a standard'
                        ' concept.')

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
                'Invalid `relationships` graph! `relationships` MUST be a '
                f'directed acyclic graph. The cycle: {edges} was detected in '
                'the graph.')

    def _build_relationship_graph(self, relationships):
        """
        Build a networkX directed graph (DiGraph) from the relationships dict
        which is in the form of an adjacency list.

        This graph codifies the parent-child relationships among target model
        concepts. It is later used in the transform stage as a set of
        restrictions when searching the concept graph for the value of a
        target concept attribute.
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
