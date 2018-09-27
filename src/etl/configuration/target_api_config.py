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

    def _validate(self):
        # TODO - Come back to validation later.
        # Leave actual validation blank for now because we need to figure out
        # what should go in target API config first. The best way to do that is
        # to make a sample kids first API config, write the actual
        # transformation code and try ou the sample config. Too difficult to
        # design this config without knowing exactly how we want to use it.
        pass

    def tbd_validate(self):
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
                raise AttributeError(f'Attribute `{attr}` is required for '
                                     f'in target API config modules')

            # Each required attribute should be a dict
            if not isinstance(config_item, dict):
                raise TypeError(f'{attr} must be of type dict')

            # Validate content of required attributes in module
            if attr == 'target_concepts':
                self._validate_target_concept_dicts(config_item)

            elif attr == 'endpoints':
                # All values in endpoints should be strings
                values_are_strs = all([isinstance(v, str)
                                       for v in config_item.values()])
                if not values_are_strs:
                    raise ValueError("All values in 'endpoints' dict must be "
                                     "strings.")

            elif attr == 'relationships':
                # All values in relationships should be sets
                values_are_sets = all([isinstance(v, set)
                                       for v in config_item.values()])
                if not values_are_sets:
                    raise ValueError("All values in 'relationships' dict must "
                                     "be strings.")

    def _validate_target_concept_dicts(self, target_concepts):
        """
        Validate target concept dicts in target_concepts
        """
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
        represents an adjcency list.
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
