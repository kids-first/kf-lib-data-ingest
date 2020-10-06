counts = {
    "FAMILY|ID": 2,
    "PARTICIPANT|ID": 2,
    "BIOSPECIMEN|ID": 1,
}

validation = {
    "Each PARTICIPANT|ID links to at least 1 BIOSPECIMEN|ID": [
        {
            "from": ("PARTICIPANT|ID", "P2"),
            "to": [],
            "locations": {("PARTICIPANT|ID", "P2"): ["fp.csv"]},
        }
    ],
    "Each BIOSPECIMEN|ID links to at least 1 GENOMIC_FILE|URL_LIST": [
        {
            "from": ("BIOSPECIMEN|ID", "B1"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "B1"): ["pb.csv"]},
        }
    ],
}
