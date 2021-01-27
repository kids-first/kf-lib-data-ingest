========================
New Ingest Package Setup
========================

Each study that needs to be ingested will have a single outer directory and one
or more inner directories that each contain the configuration files needed by
the ingest pipeline to extract, transform, and load parts of the study into a
target data store or service. The inner directories are known as the ingest
packages.

.. note::

    We want to have an outer directory for each study, because we often need
    multiple ingest packages to complete an entire study for
    practical/logistical reasons.

Create an outer directory for ``my_study`` and inside it a new ingest package
named ``ingest_package_1``:

.. code-block:: text

    $ kidsfirst new --dest_dir=my_study/ingest_package_1

This will generate a minimal study directory for you that looks like this::

    my_study/
    └── ingest_package_1/
        ├── ingest_package_config.py
        ├── transform_module.py
        ├── data
        │   └── clinical.tsv
        └── extract_configs/
            └── extract_config.py

Ingest Package Components
=========================

Each ingest package contains three components:

1. A top-level configuration file called ``ingest_package_config.py``. This
defines study metadata such as the study's target ID as well as paths to the
other two components and logging parameters. It will look something like:

    .. code-block:: python

        study = 'SD_ME0WME0W'
        extract_config_dir = 'extract_configs'
        transform_function_path = 'transform_module.py'
        log_level = 'debug'
        overwrite_log = True

2. A directory of `Extract Configuration` files.
These files define how the extract stage interprets each of the data files
provided by the investigator. These are described in the
:ref:`Tutorial-Extract-Stage` section.

3. A guided `Transform Module` file.
This is used by the transform stage and replaces automatic graph-based
transformation with a predictable set of user-programmed operations. This
is described in the :ref:`Tutorial-Transform-Stage` section.
