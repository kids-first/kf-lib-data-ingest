#!/usr/bin/env python


"""
A standalone CLI utility to load a serialized ConceptGraph into a
Neo4j graph database for the purpose of deeper debugging and visualization of
the standard concept graph.

Loader expects a JSON file that was written by
standard_model.concept_graph.export_to_file.

* Requires a Neo4j server to be running.

After running this utility, the concept graph can be viewed and queried in
the browser at <host>:<port>/browser, where host and port are the IP and port
number of the running Neo4j server.

See https://neo4j.com/docs/developer-manual for details on how to use the neo4j
browser to query the database using Cypher query language and visualize the
graph.
"""

import argparse

from py2neo import Graph
from py2neo.data import (
    Node,
    Relationship,
    Subgraph
)

from common.misc import (
    read_json
)
from etl.transform.standard_model.concept_schema import (
    DELIMITER as CONCEPT_DELIMITER
)

NEO4J_DELIMITER = '_'


class Neo4jGraphLoader(object):
    def __init__(self, uri=None, username=None, password=None):
        """
        Create a neo4j Graph obj which manages the connection to the
        neo4j server.

        :param uri: the URI of the database. If None, Graph will default to the
        neo4j default server URI, bolt://localhost:7687
        :param username: database user name
        :param password: database password for username
        """
        self.graph = Graph(uri, auth=(username, password))

    def load(self, filepath):
        """
        Create a neo4j graph given a JSON file that was created by
        standard_model.concept_graph.export_to_file.

        This operation is idempotent. The existing neo4j graph will be deleted
        and a new one will be created to replace the deleted one. All nodes
        and relationships are created and then added to a neo4j Subgraph. The
        subgraph is then committed to the database in one transaction.

        :param filepath: path to location of serialized ConceptGraph JSON file
        """
        # Clear concept graph db
        self.graph.delete_all()
        assert len(self.graph.nodes) == 0

        # Read data in
        data = read_json(filepath)

        # Begin transaction
        tx = self.graph.begin()

        # For each edge
        g = None
        for edge in data['edges']:
            # Create first neo4j Node if it doesn't exist
            key = edge['source']
            n1 = self._create_or_get_node(key, data)

            # Create second neo4j node if it doesn't exist
            key = edge['target']
            n2 = self._create_or_get_node(key, data)

            # Create neo4j edge
            r = Relationship(n1, edge['label'], n2)

            # Create neo4j subgraph if it doesn't exist
            if not g:
                g = Subgraph([n1, n2], [r])
            # Add nodes and edge to neo4j subgraph
            else:
                g_part = Subgraph([n1, n2], [r])
                g = g | g_part

        # Create graph transaction and commit
        tx.create(g)
        tx.commit()

    def _create_or_get_node(self, node_key, data):
        """
        Helper method for Neo4jGraphLoader.load

        Create a neo4j Node if it doesn't exist in the data dict containing
        the data for all the nodes and edges to be inserted into the neo4j
        graph. If it exists, return a reference to the Node obj instead.

        :param node_key: the string key of the node in the data dict
        :param data: a dict containing the serialized ConceptGraph
        """
        node = data['nodes'][node_key]
        n = node.get('obj')
        if not n:
            label = node['attributes']['concept']
            # For some reason neo4j does only accepts an underscore as a
            # delimiter for node or relationship labels.
            n = Node(label.replace(CONCEPT_DELIMITER, NEO4J_DELIMITER),
                     **node['attributes'])
            node['obj'] = n
        return n


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath",
                        help='Path to the JSON file containing the serialized '
                        'ConceptGraph')

    parser.add_argument("--host", type=str,
                        help="IP address of the running Neo4j server. If not "
                        "specified will default to 127.0.0.1")

    parser.add_argument("--port", type=str,
                        help="Port of the running Neo4j server. If not "
                        "specified will default to Neo4j default port: 7687")

    parser.add_argument("-u", "--username", type=str,
                        help="Username to authenticate with Neo4j server")

    parser.add_argument("-p", "--password", type=str,
                        help="Password to authenticate with the Neo4j server")

    args = parser.parse_args()

    uri = None
    uname = args.username or None
    pword = args.password or None

    if args.host and args.port:
        uri = f'bolt://{args.host}:{args.port}'

    neo4j = Neo4jGraphLoader(uri=uri, username=uname, password=pword)
    neo4j.load(args.filepath)
