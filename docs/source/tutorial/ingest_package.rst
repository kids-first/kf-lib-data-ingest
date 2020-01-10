============================
Ingest Package Configuration
============================

Each study that needs to be ingested will have a single directory that contains
all of the configuration needed by the ingest pipeline to extract, transform,
and load the study into a target data store or service.

The ingest package contains three components:

1. A top-level configuration file called ``ingest_package_config.py``
    This defines study metadata such as the study's target ID as well as paths
    to the other two components and logging parameters.
    It will look something like:

.. code-block:: python

    study = 'SD_ME0WME0W'
    extract_config_dir = 'extract_configs'
    transform_function_path = 'transform_module.py'
    log_level = 'debug'
    overwrite_log = True

2. A directory of `Extract Configuration` files
    These files define how the extract stage interprets each of the data files
    provided by the investigator. These are described in the
    :ref:`Tutorial-Extract-Stage` section.

3. (optional) A guided `Transform Module` file
    This is used by the transform stage and replaces automatic graph-based
    transformation with a predictable set of user-programmed operations. This
    is described in the :ref:`Tutorial-Transform-Stage` section.

Your ingest package directory for a study would look something like this::

    my_study/
    ├── ingest_package_config.py
    ├── extract_configs/
    │   ├── one_file_extract_config.py
    │   ├── another_file_extract_config.py
    │   └── ...
    └── transform_module.py

Ok, let's head on over to :ref:`Tutorial-Package-Setup` to start building
the study.
