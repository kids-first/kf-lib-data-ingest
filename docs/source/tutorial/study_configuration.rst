===================
Study Configuration
===================

Each study that needs to be ingested will have a single directory that contains
all of the configuration/inputs needed by the ingest pipeline to extract,
transform, and load the study into a target data store or service.

The study directory contains three components:

1. A top-level study configuration file called ``dataset_ingest_config.yml``
    This defines metadata about the study itself such as what the study's name
    is and who the investigators are as well as paths to the other two
    components.
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

The study directory
===================

Your complete ingest configuration for a study will look like this::

    my_study/
    ├── dataset_ingest_config.yml
    ├── extract_configs/
    │   ├── one_file_extract_config.py
    │   ├── another_file_extract_config.py
    │   └── ...
    └── transform_module.py  (optional)

dataset_ingest_config.yml
=========================

This file contains details that control ingestion of the study as a whole. It
includes study metadata, the list of investigators, logging parameters, and
paths to the extract configs directory and the optional transform module.

It looks something like:

.. code-block:: yaml

    study:
        kf_id: 'SD_MEOWMEOW'
        external_id: 'phs0001999'
        short_name: 'Cat Study'
        name: 'The long study of cats'
        authority: 'dbgap'
        version: 'v2.p1'
        release_status: 'Pending'
        category: 'Cancer'
        attribution: 'https://www.meowmeowmeow.cat/study/attribution.html'

    investigators:
        - 'IG_12345678'
        - 'IG_87654321'

    extract_config_dir: 'extract_configs'

    transform_function_path: 'transform_module.py'

    logging:
        log_level: 'info'
        overwrite_log: True


Ok, let's head on over to :ref:`Tutorial-Project-Setup` to start building
the study.
