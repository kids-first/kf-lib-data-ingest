"""
Validation for pre-cleaned data.

Call: Validator(hierarchy_override=None).validate(dict_of_dataframes, include_implicit=True)
"""
import logging
from collections import defaultdict, deque
from itertools import combinations
from pprint import pformat

from graph import Graph  # https://github.com/root-11/graph-theory
from kf_lib_data_ingest.validation.hierarchy import get_full_hierarchy
from kf_lib_data_ingest.validation.relations import REVERSE_TESTS, TESTS
from kf_lib_data_ingest.validation.values import INPUT_VALIDATION, NA

logger = logging.getLogger("DataValidator")


class GroupingGraph(Graph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups = defaultdict(set)

    def add_node(self, node, *args, **kwargs):
        super().add_node(node, *args, **kwargs)
        self.groups[node[0]].add(node)


class Validator:
    def __init__(self, hierarchy_override=None):
        (
            self.HIERARCHY,
            self.HIERARCHY_ORDER,
            self.HIERARCHY_PATHS,
            self.ANCESTOR_LOOKUP,
        ) = get_full_hierarchy(hierarchy_override)

    def validate(self, dict_of_dataframes, include_implicit=True):
        """Entry point for validating values and cardinality of relationships in a
        set of dataframes loaded from files.

        :param dict_of_dataframes: dict with filename keys and dataframe values
        :param include_implicit: whether to deduce implied connections, default True
        :yield: dict that includes metadata plus a list of test result dicts
        """
        logger.info("Validating study data")

        for k, df in dict_of_dataframes.items():
            dict_of_dataframes[k] = df.filter(self.ANCESTOR_LOOKUP).fillna(NA)

        graph, node_counts = self._build_graph(
            dict_of_dataframes, include_implicit=include_implicit
        )
        results = {
            "counts": node_counts,
            "files_validated": sorted(dict_of_dataframes.keys()),
        }
        tests = list(
            self._validate_graph_relationships(
                graph, dict_of_dataframes, include_implicit=include_implicit
            )
        )
        tests.extend(self._validate_values(dict_of_dataframes))
        results["validation"] = tests
        return results

    def _format_result(
        self, test_type, desc, valid, errors, from_type=None, to_type=None
    ):
        res = {
            "type": test_type,
            "description": desc,
            "is_applicable": valid,
            "errors": errors,
            "inputs": {},
        }
        if from_type and to_type:
            res["inputs"] = {"from": from_type, "to": to_type}

        logger.debug("\n" + pformat(res))
        return res

    def _validate_values(self, dict_of_dataframes):
        """Validate the values in a set of dataframes loaded from files.

        :param dict_of_dataframes: dict with filename keys and dataframe values
        :yield: test result dicts
        """
        logger.info("---")
        logger.info("Validating data values...")
        for concept, (desc, func) in INPUT_VALIDATION.items():
            logger.info(f"{concept} {desc}...")
            errors = {}
            tested = False
            for fname, df in dict_of_dataframes.items():
                bad_ones = []
                if concept in df:
                    tested = True
                    bad_ones = list(
                        df[concept][~df[concept].apply(func)].unique()
                    )
                    if bad_ones:
                        errors[fname] = bad_ones

            yield self._format_result(
                "attribute", f"{concept} {desc}", tested, errors
            )

    def _build_graph(self, dict_of_dataframes, include_implicit=True):
        """Construct a graph that represents the study data. Nodes are cells in
        the data, edges are relationships between cells across rows according
        to the designated relationship hierarchy among columns (see e.g.
        default_hierarchy.py).

        :param dict_of_dataframes: dict with filename keys and dataframe values
        :param include_implicit: whether to deduce implied connections, default True
        :return: a graph representation of the data
        """
        prep = "WITH" if include_implicit else "WITHOUT"
        logger.info(
            f"Building node graph {prep} implied connection discovery..."
        )
        graph = GroupingGraph()
        all_nodes = set()
        indirect_links = set()

        # ##### Add all nodes and record valid colinear pairs as edges ##### #

        logger.info("Adding nodes and direct edges...")

        for df in dict_of_dataframes.values():
            v = df.values
            colnames = df.columns.values
            colnums = list(range(len(colnames)))
            for r in range(len(df)):
                row_nodes = [
                    (colnames[c], v[r, c]) for c in colnums if v[r, c] != NA
                ]

                # We will need to add nodes even if they have no edges to know
                # if those nodes meet the validation criteria.
                all_nodes.update(row_nodes)

                # If a node has a direct hierarchical connection within a row, we
                # can probably assume that indirect connections in the same row are
                # superfluous
                #
                # We should probably also only connect a given start node to its
                # hierarchically closest destination nodes

                row_node_pairs = list(combinations(row_nodes, 2))
                direct = set()

                # Direct edges go in right away and the source nodes get marked
                for a, b in row_node_pairs:
                    if b[0] in self.ANCESTOR_LOOKUP[a[0]]:
                        graph.add_edge(a, b)
                        all_nodes.discard(a)
                        all_nodes.discard(b)
                        direct.add(a)
                    elif a[0] in self.ANCESTOR_LOOKUP[b[0]]:
                        graph.add_edge(b, a)
                        all_nodes.discard(a)
                        all_nodes.discard(b)
                        direct.add(b)

                if include_implicit:
                    # Indirectly related pairs get grouped by distance from source
                    # node so we can find the closest destinations for each source
                    row_dict = defaultdict(lambda: defaultdict(deque))
                    for a, b in row_node_pairs:
                        if (a not in direct) and (
                            b[0] in self.HIERARCHY_PATHS[a[0]]
                        ):
                            row_dict[a][
                                self.HIERARCHY_PATHS[a[0]][b[0]]
                            ].append(b)
                        elif (b not in direct) and (
                            a[0] in self.HIERARCHY_PATHS[b[0]]
                        ):
                            row_dict[b][
                                self.HIERARCHY_PATHS[b[0]][a[0]]
                            ].append(a)

                    # Use only the closest indirect destinations for each source
                    for a, cost_bs in row_dict.items():
                        nearest_bs = sorted(cost_bs.items())[0][1]
                        for b in nearest_bs:
                            indirect_links.add((a, b))

        for n in all_nodes:
            graph.add_node(n)

        all_nodes = None

        counts = self._get_node_counts(graph)
        colwidth = max(map(len, counts))
        count_block = [
            f"\t{k:<{colwidth}} : {v if v > 0 else ''}"
            for k, v in counts.items()
        ]
        count_string = "\n".join(count_block)
        logger.info(f"{sum(counts.values())} nodes added.\n{count_string}")
        logger.info(f"{len(graph.edges())} direct edges added.")

        if include_implicit and indirect_links:
            # ################################################################## #
            # ######  Code in here is for indirectly implied connections  ###### #
            # ################################################################## #

            def prune_unneeded(links):
                # (A -> B) + (B -> C) doesn't also need (A -> C)
                if links:
                    logger.info(
                        f"{len(links)} links left to insert BEFORE pruning."
                    )
                    links = [
                        e for e in links if not graph.is_connected(e[0], e[1])
                    ]
                    logger.info(
                        f"{len(links)} links left to insert AFTER pruning."
                    )
                return links or []

            indirect_links = prune_unneeded(indirect_links)

            logger.info(
                "Discovering implied direct relationships between nodes that are "
                "indirectly connected in the data..."
            )

            # ################################################################## #
            # Given:
            # - a has type A
            # - b has type B
            # - c has type C
            #
            # And given type hierarchy:  A -> B -> C
            #
            # Then: (a -> c) + (b -> c) should become (a -> b -> c)
            # ################################################################## #

            def acbc_abc(indirect_links):
                """Relating to a thing necessarily also relates to descendants of
                the thing, so try to lower connection endpoints that skip
                relationship generations (A->C instead of A->B) to find their
                implied positions within the hierarchy (from A->C to A->B if B->C).

                :param indirect_links: iterable of potential graph edges
                :return: iterable of remaining potential graph edges
                """
                logger.info("Shaking from A->C to A->B if B->C...")
                new_links = set()
                movement = False

                reverse_graph = Graph()
                for a, b, _ in graph.edges():
                    reverse_graph.add_edge(b, a)

                for ab in indirect_links:
                    a, b = ab
                    new_dests = [
                        n
                        for n in reverse_graph.nodes(b)
                        if n[0] in self.HIERARCHY_PATHS[a[0]]
                    ]
                    if new_dests:
                        movement = True
                        for n in new_dests:
                            if n[0] in self.ANCESTOR_LOOKUP[a[0]]:
                                graph.add_edge(a, n)
                            else:
                                # We could track movements here for detailed errors
                                new_links.add((a, n))
                    else:
                        new_links.add(ab)
                return new_links, movement

            # ################################################################## #
            # Given:
            # - a has type A
            # - b has type B
            # - c has type C
            #
            # And given type hierarchy:  A -> B -> C
            #
            # Then: (a -> c) + (a -> b) should become (a -> b -> c)
            # ################################################################## #

            def acab_abc(indirect_links):
                """Relating to a thing necessarily also relates to descendants of
                the thing, so try to raise connection start points that skip
                relationship generations (A->C instead of B->C) to find their
                implied positions within the hierarchy (from A->C to B->C if A->B).

                :param indirect_links: iterable of potential graph edges
                :return: iterable of remaining potential graph edges
                """
                logger.info("Shaking from A->C to B->C if A->B...")
                new_links = set()
                movement = False
                for ab in indirect_links:
                    a, b = ab
                    new_dests = [
                        n
                        for n in graph.nodes(a)
                        if b[0] in self.HIERARCHY_PATHS[n[0]]
                    ]
                    if new_dests:
                        movement = True
                        for n in new_dests:
                            if b[0] in self.ANCESTOR_LOOKUP[n[0]]:
                                graph.add_edge(n, b)
                            else:
                                # We could track movements here for detailed errors
                                new_links.add((n, b))
                    else:
                        new_links.add(ab)
                return new_links, movement

            # TBD: There might be a third scenario introduced by adding attributes
            # to the graph as generic nodes where A -> B -> D <- C should actually
            # be B -> D <- C <- A. The risk of encountering that in practice is
            # probably quite low. I think addressing it involves least common
            # ancestor discovery, which networkx can do. It might also be better to
            # introduce a distinction between identifier nodes and attribute nodes
            # when constructing the graph.

            # #### Shake the graph back and forth until placements stabilize #### #

            def try_until_stable(f, indirect_links):
                while True:
                    indirect_links, movement = f(indirect_links)
                    if not movement:
                        logger.info("Found stability for uninserted links.")
                        return indirect_links
                    logger.info("Uninserted links not stable yet.")

            def shake(indirect_links):
                # This is a bit like one iteration of a bidirectional bubble sort
                indirect_links, movement1 = acbc_abc(indirect_links)
                indirect_links, movement2 = acab_abc(indirect_links)
                logger.info(
                    f"Uninserted links remaining: {len(indirect_links)}"
                )
                return indirect_links, movement1 or movement2

            indirect_links = prune_unneeded(
                try_until_stable(shake, indirect_links)
            )

            # ###### Throw remaining indirect links into the graph ###### #
            # (I'm not completely sure if we should do this part, but it is needed for
            # the hierarchy gaps test.)
            for a, b in indirect_links:
                graph.add_edge(a, b)

        logger.info(f"Final edge count: {len(graph.edges())}")
        return graph, counts

    def _get_node_counts(self, graph):
        """Count the number of unique instances of each type of value.

        :param graph: a graph representation of the data
        :return: dict of type keys and count values
        """
        return {c: len(graph.groups[c]) for c in self.HIERARCHY_ORDER}

    def _validate_graph_relationships(
        self, graph, dict_of_dataframes, include_implicit=True
    ):
        """Perform tests on the graph to validate which of the hierarchy
        rules have been broken by the study data.

        :param graph: a graph representation of the data
        :param dict_of_dataframes: dict with filename keys and dataframe values
        :yield: test result dicts
        """
        assert isinstance(graph, Graph)
        assert len(graph.nodes()) > 0

        logger.info("Validating graph relationships...")

        reverse_graph = Graph()
        for a, b, _ in graph.edges():
            reverse_graph.add_edge(b, a)

        membership_lookup = {
            f: {col: set(df[col].values) for col in df.columns}
            for f, df in dict_of_dataframes.items()
        }

        def find_in_files(node):
            """Find which files a given node came from.

            :param node: a node in the graph representing a data value
            :return: a list of files
            """
            return [
                f
                for f, ml in membership_lookup.items()
                if (node[0] in ml) and (node[1] in ml[node[0]])
            ]

        def cardinality(typeA, typeB, relation):
            """Tests cardinality of connections between typeA and typeB.

            :param typeA: a type node (nodes are inserted in the graph for value
                types as well as values)
            :param typeB: another type node
            :param relation: one of the tuples from relations.RELATIONS
            :return: a test result dict
            """
            desc, func = relation
            logger.info(f"Testing {typeA} links to {desc} {typeB}...")
            errors = deque()
            A_nodes = graph.groups[typeA]
            if typeB in self.ANCESTOR_LOOKUP[typeA]:
                g = graph
            else:
                g = reverse_graph
            for n in A_nodes:
                links = [c for c in g.nodes(n) if c[0] == typeB]
                if not func(len(links)):
                    locs = {m: find_in_files(m) for m in ([n] + links)}
                    errors.append({"from": n, "to": links, "locations": locs})

            desc = f"Each {typeA} links to {desc} {typeB}"
            return self._format_result(
                "relationship", desc, bool(A_nodes), errors, typeA, typeB
            )

        for a, b, r in self.HIERARCHY.edges():
            if r in TESTS:
                yield cardinality(a, b, TESTS[r])
            if r in REVERSE_TESTS:
                yield cardinality(b, a, REVERSE_TESTS[r])
            logger.info("---")

        if include_implicit:

            def indirect():
                """Tests, according to the relationship hierarchy, that A always -> B
                always -> C, and not A -> C without a B.

                :return: a test result dict
                """
                logger.info(
                    "Looking for hierarchically indirect links in the graph..."
                )
                errors = deque()
                for n in graph.nodes():
                    bad_links = [
                        m
                        for m in graph.nodes(n)
                        if (m[0] not in self.ANCESTOR_LOOKUP[n[0]])
                    ]
                    if bad_links:
                        locs = {m: find_in_files(m) for m in ([n] + bad_links)}
                        errors.append(
                            {"from": n, "to": bad_links, "locations": locs}
                        )

                desc = "All resolved links are hierarchically direct"
                return self._format_result(
                    "gaps", desc, bool(graph.nodes()), errors
                )

            yield indirect()
