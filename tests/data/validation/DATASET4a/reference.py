counts = {
    "FAMILY|ID": 2,
    "PARTICIPANT|ID": 2,
    "BIOSPECIMEN|ID": 1,
}

validation = {
    "Each BIOSPECIMEN|ID links to exactly 1 PARTICIPANT|ID": [
        {
            "from": ("BIOSPECIMEN|ID", "B1"),
            "to": [("PARTICIPANT|ID", "P1"), ("PARTICIPANT|ID", "P2")],
            "locations": {
                ("BIOSPECIMEN|ID", "B1"): ["fb.csv"],
                ("PARTICIPANT|ID", "P1"): ["fp.csv"],
                ("PARTICIPANT|ID", "P2"): ["fp.csv"],
            },
        }
    ],
    "Each BIOSPECIMEN|ID links to at least 1 GENOMIC_FILE|URL_LIST": [
        {
            "from": ("BIOSPECIMEN|ID", "B1"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "B1"): ["fb.csv"]},
        }
    ],
}
