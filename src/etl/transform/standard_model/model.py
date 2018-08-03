import networkx as nx


class StandardModel(object):

    # TODO

    def __init__(self):
        self.concept_property_graph = None
        self.target_schema = None

    def transform(self, target_schema):
        # TODO
        # Return a dict of target entity dicts, keyed by target entity type
        pass

    def build(self, df_dict):
        """
        Iterate over DataFrames and insert each cell as a node in the
        concept property graph. Then create edges between nodes.

        :param df_dict: a dict of Pandas DataFrames keyed by source URL
        """

        # Insert nodes into concept property graph
        # For each DataFrame
        for key, df in df_dict.items():
            # For each row
            for r, row in df.iterrows():
                id_nodes = []
                property_nodes = []
                # For each column
                for c, col in enumerate(df.columns):
                    props = {
                        'source_url': key,
                        'row': r,
                        'col': c
                    }
                    # Add node to graph
                    node = self.concept_property_graph.add_node(col, row[col],
                                                                **props)
                    # Sort nodes into ID nodes and property nodes
                    if node.is_identifier:
                        id_nodes.append(node)
                    else:
                        property_nodes.append(node)

                # Connect id nodes
                self.concept_property_graph.connect_id_nodes(id_nodes)

                # Connect property nodes
                for prop_node in property_nodes:
                    self.concept_property_graph.connect_property_node(
                        prop_node, id_nodes
                    )

    def _create_relationship_graph(self, relationship_dict):
        """
        Build a directed graph (networkx.DiGraph) from an adjacency list.
        This graph captures the relationships between the target model's
        concepts defined by the target schema

        :param relationship_dict: a dict of sets
        """
        # TODO - does not belong here, this should be part of target model
        # schema
        g = nx.DiGraph()
        for parent_concept, children in relationship_dict.items():
            g.add_edges_from([(parent_concept, child_concept)
                              for child_concept in children])

        return g


if __name__ == '__main__':
    pass
