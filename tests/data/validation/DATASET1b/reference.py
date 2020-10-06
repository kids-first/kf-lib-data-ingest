counts = {
    "BIOSPECIMEN_GROUP|ID": 11,
    "BIOSPECIMEN|ID": 11,
    "FAMILY|ID": 7,
    "PARTICIPANT|ID": 10,
    "GENOMIC_FILE|URL_LIST": 5,
}

validation = {
    "Each FAMILY|ID links to at least 1 PARTICIPANT|ID": [
        {
            "from": ("FAMILY|ID", "F12"),
            "to": [],
            "locations": {("FAMILY|ID", "F12"): ["pf.csv"]},
        }
    ],
    "Each BIOSPECIMEN_GROUP|ID links to exactly 1 PARTICIPANT|ID": [
        {
            "from": ("BIOSPECIMEN_GROUP|ID", "S2"),
            "to": [("PARTICIPANT|ID", "P2"), ("PARTICIPANT|ID", "P1")],
            "locations": {
                ("BIOSPECIMEN_GROUP|ID", "S2"): ["sfp2.csv", "spf.csv"],
                ("PARTICIPANT|ID", "P2"): ["sfp2.csv"],
                ("PARTICIPANT|ID", "P1"): ["spf.csv"],
            },
        },
        {
            "from": ("BIOSPECIMEN_GROUP|ID", "S8"),
            "to": [],
            "locations": {("BIOSPECIMEN_GROUP|ID", "S8"): ["spf.csv"]},
        },
    ],
    "Each PARTICIPANT|ID links to at least 1 BIOSPECIMEN_GROUP|ID": [
        {
            "from": ("PARTICIPANT|ID", "P11"),
            "to": [],
            "locations": {("PARTICIPANT|ID", "P11"): ["pf.csv"]},
        },
        {
            "from": ("PARTICIPANT|ID", "P13"),
            "to": [],
            "locations": {("PARTICIPANT|ID", "P13"): ["pf.csv"]},
        },
    ],
    "Each BIOSPECIMEN|ID links to at least 1 GENOMIC_FILE|URL_LIST": [
        {
            "from": ("BIOSPECIMEN|ID", "S2"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "S2"): ["sfp2.csv", "spf.csv"]},
        },
        {
            "from": ("BIOSPECIMEN|ID", "S1"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "S1"): ["spf.csv"]},
        },
        {
            "from": ("BIOSPECIMEN|ID", "S3"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "S3"): ["spf.csv"]},
        },
        {
            "from": ("BIOSPECIMEN|ID", "S6"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "S6"): ["spf.csv"]},
        },
        {
            "from": ("BIOSPECIMEN|ID", "S8"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "S8"): ["spf.csv"]},
        },
        {
            "from": ("BIOSPECIMEN|ID", "S4"),
            "to": [],
            "locations": {("BIOSPECIMEN|ID", "S4"): ["spf.csv"]},
        },
    ],
}
