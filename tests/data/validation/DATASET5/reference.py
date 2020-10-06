counts = {
    "FAMILY|ID": 2,
    "PARTICIPANT|ID": 1,
    "BIOSPECIMEN|ID": 2,
    "GENOMIC_FILE|URL_LIST": 0,
}

validation = {
    "Each BIOSPECIMEN|ID links to at least 1 GENOMIC_FILE|URL_LIST": [
        {
            "from": ("BIOSPECIMEN|ID", "B2"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "B2"): ["fb.csv"]},
        },
        {
            "from": ("BIOSPECIMEN|ID", "B1"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "B1"): ["pb.csv", "fb.csv"]},
        },
    ]
}
