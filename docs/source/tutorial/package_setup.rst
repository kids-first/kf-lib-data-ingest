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

Try running your ingest package like this to make sure it works:

.. code-block:: text

    $ kidsfirst test my_study

The logs on your screen should indicate a successful test run like this:

.. code-block:: text

    ======================================== 1 passed in 0.12s ============================================================
    2020-02-18 17:40:36,039 - DataIngestPipeline - Thread: MainThread - INFO - ✅ User defined data validation tests passed
    2020-02-18 17:40:36,039 - DataIngestPipeline - Thread: MainThread - INFO - END data ingestion
    2020-02-18 17:40:36,039 - kf_lib_data_ingest.app.cli - Thread: MainThread - INFO - ✅ Ingest pipline passed validation!

You'll learn more about the ``test`` and ``ingest`` commands later in
:ref:`Tutorial-Package-Testing`.

Now we are ready to start configuration for the :ref:`Tutorial-Extract-Stage`.
