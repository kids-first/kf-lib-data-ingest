"""
Sample validation results from do_all_validation() in
kf_lib_data_ingest.validation.data_validator.py
"""

results = {
    "counts": {"PARTICIPANT|ID": 10, "BIOSPECIMEN|ID": 20, "FAMILY|ID": 1},
    "files_validated": ["foo.txt", "bar.txt"],
    "validation": [
        # Relationship test example 1 - entity relation test
        {
            # Must be one of [ relationship | attribute | count ]
            "type": "relationship",
            "description": "Every BIOSPECIMEN|ID must have 1 PARTICIPANT|ID",
            # Must be one of [ True | False ] True
            # True means the test ran
            # False means the test did not run
            "is_applicable": True,
            "inputs": {"from": "BIOSPECIMEN|ID", "to": "PARTICIPANT|ID"},
            # Format for `errors` when type = relationship
            "errors": [
                {
                    # `from` value must be a concept tuple
                    "from": ("BIOSPECIMEN|ID", "B1"),
                    # `to` value must be a list of concept tuples
                    "to": [
                        ("PARTICIPANT|ID", "P1"),
                        # 'a;dljfa', bad type
                        ("PARTICIPANT|ID", "P2"),
                    ],
                    # Must be a dict. A key must be a concept tuple and
                    # its value must be a set of file path stringss
                    "locations": {
                        ("BIOSPECIMEN|ID", "B1"): {"foo.txt", "bar.txt"},
                        ("PARTICIPANT|ID", "P1"): {"foo.txt", "bar.txt"},
                        ("PARTICIPANT|ID", "P2"): {"foo.txt", "bar.txt"},
                    },
                },
                {
                    "from": ("BIOSPECIMEN|ID", "B2"),
                    "to": [],
                    "locations": {("BIOSPECIMEN|ID", "B2"): {"foo.txt"}},
                },
            ],
        },
        # Relationship test example 2 - entity attribute relation test
        {
            "type": "relationship",
            "description": "Every PARTICIPANT|ID must have 1 Gender",
            "is_applicable": True,
            "inputs": {"from": "PARTICIPANT|ID", "to": "PARTICIPANT|GENDER"},
            "errors": [
                {
                    "from": ("PARTICIPANT|ID", "P2"),
                    "to": [],
                    "locations": {("PARTICIPANT|ID", "P2"): {"foo.txt"}},
                },
                {
                    "from": ("PARTICIPANT|ID", "P1"),
                    "to": [
                        ("PARTICIPANT|GENDER", "male"),
                        ("PARTICIPANT|GENDER", "female"),
                    ],
                    "locations": {
                        ("PARTICIPANT|ID", "P1"): {"foo.txt"},
                        ("PARTICIPANT|GENDER", "male"): {"foo.txt"},
                        ("PARTICIPANT|GENDER", "female"): {"foo.txt"},
                    },
                },
            ],
        },
        {
            "type": "gaps",
            "inputs": {},
            "description": "All resolved links are hierarchically direct",
            "is_applicable": True,
            "errors": [
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
        },
        # Attribute test example 1 - Success
        {
            "type": "relationship",
            "description": "Every PARTICIPANT|ID must have 1 FAMILY|ID",
            "is_applicable": True,
            "inputs": {"from": "PARTICIPANT|ID", "to": "FAMILY|ID"},
            "errors": [],
        },
        # Attribute test example 2 - Failed
        {
            "type": "attribute",
            "description": "PARTICIPANT|GENDER must be one of: Male, Female",
            "is_applicable": True,
            "inputs": {"from": "PARTICIPANT|GENDER"},
            # Format for `errors` when type = attribute
            # A key must be a file path str and its value must be a
            # set of invalid values
            "errors": {"foo.txt": {"mae", "blah", "M", "f"}, "bar.txt": {"m"}},
        },
        # Attribute test example 3 - Failed
        {
            "type": "attribute",
            "description": "BIOSPECIMEN|VOLUME_ML must be >= 0 mL",
            "is_applicable": True,
            "inputs": {"from": "BIOSPECIMEN|VOLUME_ML"},
            "errors": {"foo.txt": {-12123}, "bar.txt": {-1}},
        },
        # Attribute test example 4 - Did not run
        {
            "type": "attribute",
            "description": "BIOSPECIMEN|ANALYTE_TYPE must be one of {DNA, RNA}",
            "is_applicable": False,
            "inputs": {"from": "BIOSPECIMEN|ANALYTE_TYPE"},
            "errors": {},
        },
        # Count test example 1 - Success
        {
            "type": "count",
            "description": "FAMILY|ID: Found == Expected",
            "is_applicable": True,
            "inputs": {"from": "FAMILY|ID"},
            "errors": {},
        },
        # Count test example 2 - Failed
        {
            "type": "count",
            "description": "PARTICIPANT|ID: Found == Expected",
            "is_applicable": True,
            "inputs": {"from": "PARTICIPANT|ID"},
            # Format for `errors` when type = count
            "errors": {"expected": 10, "found": 20},
        },
        # Count test example 3 - Did not run
        {
            "type": "count",
            "description": "BIOSPECIMEN|ID: Found == Expected",
            "is_applicable": False,
            "inputs": {"from": "BIOSPECIMEN|ID"},
            "errors": {},
        },
    ],
}
