counts = {
    "FAMILY|ID": 2,
    "PARTICIPANT|ID": 2,
    "BIOSPECIMEN|ID": 1,
    "GENOMIC_FILE|URL_LIST": 1,
}

validation = {
    "Each PARTICIPANT|ID links to at least 1 BIOSPECIMEN|ID": [
        {
            "from": ("PARTICIPANT|ID", "P2"),
            "to": [],
            "locations": {("PARTICIPANT|ID", "P2"): ["pg.csv", "fp.csv"]},
        }
    ],
    "Each GENOMIC_FILE|URL_LIST links to exactly 1 BIOSPECIMEN|ID": [
        {
            "from": ("GENOMIC_FILE|URL_LIST", "['s1.txt']"),
            "to": [],
            "locations": {("GENOMIC_FILE|URL_LIST", "['s1.txt']"): ["pg.csv"]},
        }
    ],
    "Each BIOSPECIMEN|ID links to at least 1 GENOMIC_FILE|URL_LIST": [
        {
            "from": ("BIOSPECIMEN|ID", "B1"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "B1"): ["pb.csv"]},
        }
    ],
    "All resolved links are hierarchically direct": [
        {
            "from": ("GENOMIC_FILE|URL_LIST", "['s1.txt']"),
            "to": [("PARTICIPANT|ID", "P2")],
            "locations": {
                ("GENOMIC_FILE|URL_LIST", "['s1.txt']"): ["pg.csv"],
                ("PARTICIPANT|ID", "P2"): ["pg.csv", "fp.csv"],
            },
        }
    ],
}
