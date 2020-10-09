from graph import Graph
from kf_lib_data_ingest.common.concept_schema import CONCEPT as C
from kf_lib_data_ingest.validation.relations import RELATIONS as R

HIERARCHY = Graph()

HIERARCHY.add_edge(C.PARTICIPANT.ID, C.FAMILY.ID, R.MANY_ANY)
HIERARCHY.add_edge(C.BIOSPECIMEN.ID, C.PARTICIPANT.ID, R.MANY_ONE)
HIERARCHY.add_edge(C.GENOMIC_FILE.URL_LIST, C.BIOSPECIMEN.ID, R.MANY_ONE)
