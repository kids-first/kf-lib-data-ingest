.. _Tutorial-Project-Setup:

=============
Project Setup
=============

If you haven't done so already, make sure to complete the installation before
moving on.

Create Your Ingest Project
==========================

Create a new ingest package named ``my_study`` by using the ingest library
CLI::

$ kidsfirst new my_study

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

You can fill in the study information in the ``ingest_package_config.yml`` file
if you want, but we won't actually use it until later in the Load stage. You
can always modify it later.

Now we are ready to start configuration for the :ref:`Tutorial-Extract-Stage`.
