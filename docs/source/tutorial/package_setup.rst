.. _Tutorial-Package-Setup:

========================
New Ingest Package Setup
========================

Create a new ingest package named ``my_study`` by using the ingest library
CLI::

$ kidsfirst new --dest_dir=my_study

This will generate a minimal study directory for you that looks like this::

    my_study/
    ├── ingest_package_config.py
    ├── transform_module.py
    ├── data
    │   └── clinical.tsv
    ├── extract_configs/
    │   └── extract_config.py
    └── tests
        ├── conftest.py
        └── test_custom_counts.py

Now we are ready to start configuration for the :ref:`Tutorial-Extract-Stage`.
