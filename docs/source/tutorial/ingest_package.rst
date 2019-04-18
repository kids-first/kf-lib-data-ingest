============================
Ingest Package Configuration
============================

Each study that needs to be ingested will have a single directory that contains
all of the configuration/inputs needed by the ingest pipeline to extract,
transform, and load the study into a target data store or service.

The ingest package contains three components:

1. A top-level configuration file called ``ingest_package_config.py``
    This defines metadata about the study itself such as the study's
    target ID as well as paths to the other two components.
2. A directory of `Extract Configuration` files
    This is the configuration needed by the extract stage. These files define
    how data is interpreted and extracted from each of the files provided by
    the investigator.
3. (optional) A guided `Transform Module` file
    This is the configuration used by the transform stage. The
    `Transform Module` file replaces automatic graph-based transformation with
    a predictable set of user-programmed operations. If this file does not
    exist, then automatic graph-based transformation will be used by the
    transform stage. More on this later in the tutorial.

The ingest package directory
=============================

Your complete ingest package for a study will look like this::

    my_study/
    ├── ingest_package_config.py
    ├── extract_configs/
    │   ├── one_file_extract_config.py
    │   ├── another_file_extract_config.py
    │   └── ...
    └── transform_module.py  (optional)

ingest_package_config.py
=========================

This file contains details that control ingestion of the study as a whole. It
includes study metadata, the list of investigators, logging parameters, and
paths to the extract configs directory and the optional transform module.

It looks something like:

.. code-block:: python

    study = 'SD_ME0WME0W'

    extract_config_dir = 'extract_configs'

    transform_function_path = 'transform_module.py'

    log_level = 'debug'
    overwrite_log = True


Ok, let's head on over to :ref:`Tutorial-Project-Setup` to start building
the study.