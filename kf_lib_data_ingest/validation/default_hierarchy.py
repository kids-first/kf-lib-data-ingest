from graph import Graph  # https://github.com/root-11/graph-theory
from kf_lib_data_ingest.common.concept_schema import CONCEPT as C
from kf_lib_data_ingest.validation.relations import RELATIONS as R

DEFAULT_HIERARCHY = Graph()
DH = DEFAULT_HIERARCHY

# ######### Hierarchy edges point --> upwards toward the concept root

# relationships between identifiers

DH.add_edge(C.PARTICIPANT.ID, C.FAMILY.ID, R.MANY_ANY)
DH.add_edge(C.BIOSPECIMEN.ID, C.PARTICIPANT.ID, R.MANY_ONE)
DH.add_edge(C.GENOMIC_FILE.ID, C.SEQUENCING.ID, R.MANY_ONE)
DH.add_edge(C.GENOMIC_FILE.ID, C.BIOSPECIMEN.ID, R.MANY_ONE)

# participant optional attributes

DH.add_edge(C.PARTICIPANT.GENDER, C.PARTICIPANT.ID, R.ONEZERO_MANY)
DH.add_edge(C.PARTICIPANT.ETHNICITY, C.PARTICIPANT.ID, R.ONEZERO_MANY)
DH.add_edge(C.PARTICIPANT.RACE, C.PARTICIPANT.ID, R.ONEZERO_MANY)
DH.add_edge(C.PARTICIPANT.SPECIES, C.PARTICIPANT.ID, R.ONEZERO_MANY)
DH.add_edge(C.PARTICIPANT.ENROLLMENT_AGE_DAYS, C.PARTICIPANT.ID, R.ONEZERO_MANY)

# biospecimen required attributes

DH.add_edge(C.BIOSPECIMEN.ANALYTE, C.BIOSPECIMEN.ID, R.ONE_MANY)

# biospecimen optional attributes

DH.add_edge(C.BIOSPECIMEN.COMPOSITION, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.TISSUE_TYPE, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.ANATOMY_SITE, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.TUMOR_DESCRIPTOR, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.EVENT_AGE_DAYS, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.SPATIAL_DESCRIPTOR, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.SHIPMENT_ORIGIN, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.SHIPMENT_DATE, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(
    C.BIOSPECIMEN.CONCENTRATION_MG_PER_ML, C.BIOSPECIMEN.ID, R.ONEZERO_MANY
)
DH.add_edge(C.BIOSPECIMEN.VOLUME_UL, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)
DH.add_edge(C.BIOSPECIMEN.SAMPLE_PROCUREMENT, C.BIOSPECIMEN.ID, R.ONEZERO_MANY)

# genomic file required attributes

DH.add_edge(C.GENOMIC_FILE.HARMONIZED, C.GENOMIC_FILE.ID, R.ONE_MANY)
DH.add_edge(C.GENOMIC_FILE.REFERENCE_GENOME, C.GENOMIC_FILE.ID, R.ONE_MANY)
DH.add_edge(C.GENOMIC_FILE.URL_LIST, C.GENOMIC_FILE.ID, R.ONE_ONE)

# sequencing required attributes

DH.add_edge(C.SEQUENCING.LIBRARY_NAME, C.SEQUENCING.ID, R.ONEZERO_ONE)
DH.add_edge(C.SEQUENCING.STRATEGY, C.SEQUENCING.ID, R.ONE_MANY)
DH.add_edge(C.SEQUENCING.PAIRED_END, C.SEQUENCING.ID, R.ONE_MANY)
DH.add_edge(C.SEQUENCING.PLATFORM, C.SEQUENCING.ID, R.ONEZERO_MANY)
DH.add_edge(C.SEQUENCING.INSTRUMENT, C.SEQUENCING.ID, R.ONEZERO_MANY)
