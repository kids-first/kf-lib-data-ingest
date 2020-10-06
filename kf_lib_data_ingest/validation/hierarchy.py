from math import inf

from graph import Graph  # https://github.com/root-11/graph-theory
from kf_lib_data_ingest.validation.default_hierarchy import DEFAULT_HIERARCHY


def get_full_hierarchy(H=None):
    HIERARCHY = H if H is not None else DEFAULT_HIERARCHY

    # Make a faster lookup than HIERARCHY.nodes()
    ANCESTOR_LOOKUP = {n: HIERARCHY.nodes(n) for n in HIERARCHY.nodes()}

    # Find a hierarchy node without ancestors to use as a starting point, and
    # breadth-first crawl the hierarchy from there to create a decent ordering
    # for listing node counts
    #
    # (This is optional and could just be removed as unimportant later)
    _bidirectional = Graph()
    for a, b, _ in HIERARCHY.edges():
        _bidirectional.add_edge(a, b, bidirectional=True)

    top = None
    for n, v in ANCESTOR_LOOKUP.items():
        if not v:
            top = n
            break

    HIERARCHY_ORDER = list(_bidirectional.breadth_first_walk(top))

    # Make a faster lookup than H.is_connected (also bakes in distance costs)
    #
    # (It doesn't matter that the edge weights in the hierarchy graph are arbitrary
    # cardinality enumerations and not actual distances. As long as the values are
    # all positive, the "cost" here will monotonically increase along the
    # hierarchy, which is all we care about.)
    HIERARCHY_PATHS = {
        source: {
            dest: cost
            for dest, cost in connected.items()
            if cost != inf and cost != 0
        }
        for source, connected in HIERARCHY.all_pairs_shortest_paths().items()
    }

    return HIERARCHY, HIERARCHY_ORDER, HIERARCHY_PATHS, ANCESTOR_LOOKUP
