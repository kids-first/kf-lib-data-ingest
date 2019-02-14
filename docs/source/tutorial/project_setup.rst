==============
Project Setup
==============

If you haven't done so already make sure to complete the installation before moving on.

Create Your Ingest Project
==========================

Create a new ingest project named ``My_Study`` by using the ingest library CLI::

$ kidsfirst new My_Study

This will generate a minimal study directory for you. It will look like this::

    My_Study/
    ├── dataset_ingest_config.yml
    ├── extract_configs/
    │   ├── example_extract_config.py
    └── transform_module.py

If you would like, you can fill in the study and investigator information in the
``dataset_ingest_config.yml`` file. We won't actually use this until later on in the load
stage so you can always come back to modify it. Leave everything else as is for now.

Get Some Source Data
=====================
TODO

Go to <> and copy over the `data` folder into your `My_Study` folder. Now it should look like this::

    My_Study/
    ├── dataset_ingest_config.yml
    ├── extract_configs/
    │   ├── example_extract_config.py
    └── transform_module.py
    ├── data/
    │   ├── family_and_phenotype.tsv
    │   ├── sample.tsv
    │   ├── sample_file_manifest.tsv

Ok now we are ready to start configuration for the extract stage.
