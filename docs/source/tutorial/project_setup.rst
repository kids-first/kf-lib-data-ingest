.. _Tutorial-Project-Setup:

=============
Project Setup
=============

If you haven't done so already make sure to complete the installation before
moving on.

Create Your Ingest Project
==========================

Create a new ingest package named ``my_study`` by using the ingest library
CLI::

$ kidsfirst new my_study

This will generate a minimal study directory for you. It will look like this::

    my_study/
    ├── data
    │   └── clinical.tsv
    ├── dataset_ingest_config.yml
    ├── transform_module.py
    └── extract_configs/
    │   └── extract_config.py
    ├── tests
    │   ├── conftest.py
    │   └── test_custom_counts.py

You can fill in the study and investigator information in the
``dataset_ingest_config.yml`` file if you want, but we won't actually use this
until later in the Load stage. You can always come back to modify it later.

Ok now we are ready to start configuration for the
:ref:`Tutorial-Extract-Stage`.
