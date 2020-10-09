counts = {
    "FAMILY|ID": 2,
    "PARTICIPANT|ID": 2,
    "BIOSPECIMEN|ID": 0,
    "GENOMIC_FILE|URL_LIST": 1,
}

validation = {
    "Each PARTICIPANT|ID links to at least 1 BIOSPECIMEN|ID": [
        {
            "from": ("PARTICIPANT|ID", "P1"),
            "to": [],
            "locations": {("PARTICIPANT|ID", "P1"): ["fp.csv"]},
        },
        {
            "from": ("PARTICIPANT|ID", "P2"),
            "to": [],
            "locations": {("PARTICIPANT|ID", "P2"): ["fp.csv"]},
        },
    ],
    "Each GENOMIC_FILE|URL_LIST links to exactly 1 BIOSPECIMEN|ID": [
        {
            "from": ("GENOMIC_FILE|URL_LIST", "['s1.txt']"),
            "to": [],
            "locations": {("GENOMIC_FILE|URL_LIST", "['s1.txt']"): ["fg.csv"]},
        }
    ],
    "All resolved links are hierarchically direct": [
        {
            "from": ("GENOMIC_FILE|URL_LIST", "['s1.txt']"),
            "to": [("PARTICIPANT|ID", "P2"), ("PARTICIPANT|ID", "P1")],
            "locations": {
                ("GENOMIC_FILE|URL_LIST", "['s1.txt']"): ["fg.csv"],
                ("PARTICIPANT|ID", "P2"): ["fp.csv"],
                ("PARTICIPANT|ID", "P1"): ["fp.csv"],
            },
        }
    ],
}
