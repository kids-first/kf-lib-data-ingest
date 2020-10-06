counts = {
    "FAMILY|ID": 2,
    "PARTICIPANT|ID": 2,
    "BIOSPECIMEN|ID": 1,
    "GENOMIC_FILE|URL_LIST": 1,
}

validation = {
    "Each BIOSPECIMEN|ID links to exactly 1 PARTICIPANT|ID": [
        {
            "from": ("BIOSPECIMEN|ID", "B1"),
            "to": [("PARTICIPANT|ID", "P1"), ("PARTICIPANT|ID", "P2")],
            "locations": {
                ("BIOSPECIMEN|ID", "B1"): ["pb.csv"],
                ("PARTICIPANT|ID", "P1"): ["pb.csv", "fp.csv"],
                ("PARTICIPANT|ID", "P2"): ["fp.csv"],
            },
        }
    ]
}
