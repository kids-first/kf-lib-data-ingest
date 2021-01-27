.. _Quickstart:

**********
Quickstart
**********

If you want to get up and running quickly with creating an ingest package
and running it, then the quick start guide is the best place to start. It's ok
if you do not understand what each of the steps are doing. The tutorial
section goes through the exact same demonstration, but it includes detailed
explanations for each step.

Generate a new ingest package
=============================

Each study that needs to be ingested will have a single outer directory and one
or more inner directories that each contain the configuration files needed by
the ingest pipeline to extract, transform, and load parts of the study into a
target data store or service. The inner directories are known as the ingest
packages.

.. note::

    We want to have an outer directory for each study, because we often need
    multiple ingest packages to complete an entire study for
    practical/logistical reasons.

The first thing to do after installing the ingest library CLI is to create an
outer directory for a study named ``my_study`` and inside it a new ingest
package named ``ingest_package_1``:

.. code-block:: text

    $ kidsfirst new --dest_dir=my_study/ingest_package_1

Your new ingest package is extremely basic. It has 1 test source data file and
the Python modules needed to extract, clean, and transform the data for the
target service. If you look inside it, it will look like this::

    my_study/
    └── ingest_package_1/
        ├── ingest_package_config.py
        ├── transform_module.py
        ├── data/
        │   └── clinical.tsv
        ├── extract_configs/
        │   └── extract_config.py
        └── tests/
            ├── conftest.py
            └── test_custom_counts.py

And the included test data looks like

.. csv-table:: my_study/ingest_package_1/data/clinical.tsv
    :delim: tab
    :header-rows: 1
    :file: ../../kf_lib_data_ingest/templates/my_ingest_package/data/clinical.tsv

Test
====

We can make sure everything works with the ``test`` command:

.. code-block:: text

    $ kidsfirst test my_study/ingest_package_1 --no_validate

The logs on your screen should indicate a successful unvalidated test run.

.. code-block:: text

    $ kidsfirst test my_study/ingest_package_1 --no_validate
    2020-11-27 11:39:26,161 - DataIngestPipeline - Thread: MainThread - WARNING - Ingest will run with validation disabled!
    ...
    2020-11-27 11:39:26,575 - DataIngestPipeline - Thread: MainThread - INFO - Ingest skipped validation!
    2020-11-27 11:39:26,575 - DataIngestPipeline - Thread: MainThread - INFO - ✅ Ingest pipeline completed execution!
    2020-11-27 11:39:26,575 - DataIngestPipeline - Thread: MainThread - INFO - END data ingestion

If you scroll up through the log output, you should see that it pretends to
load families, participants, diagnoses, and biospecimens from clinical.tsv.

Add another extract config
==========================

We're going to create a second extract config to extract data from the
following source data (different from the file you already have and stored in
the cloud):

.. csv-table:: family_and_phenotype.tsv
    :delim: tab
    :header-rows: 2
    :file: ../../docs/data/family_and_phenotype.tsv

Download the extract configuration for this new data
:download:`family_and_phenotype.py<tutorial/extract/family_and_phenotype.py>`
and put it in ``my_study/ingest_package_1/extract_configs`` like this::

    my_study/
    └── ingest_package_1/
        └── extract_configs/
           └── family_and_phenotype.py

The full tutorial has a detailed explanation of what this file does, but for
now the file should look like this:

.. literalinclude:: tutorial/extract/family_and_phenotype.py
   :caption: my_study/ingest_package_1/extract_configs/family_and_phenotype.py

Modify the Transform module
===========================

Next we want to merge our extracted tables together to properly connect the
data needed for generating complete target entities.

For the quickstart, this will be performed by
``my_study/ingest_package_1/transform_module.py`` which currently looks like
this:

.. literalinclude:: ../../kf_lib_data_ingest/templates/my_ingest_package/transform_module.py
   :caption: my_study/ingest_package_1/transform_module.py
   :emphasize-lines: 53

Find the line near the bottom that says (highlighted above)

.. code-block:: Python

    df = mapped_df_dict["extract_config.py"]

and replace that line with

.. code-block:: Python

    df = outer_merge(
        mapped_df_dict['extract_config.py'],
        mapped_df_dict['family_and_phenotype.py'],
        on=CONCEPT.BIOSPECIMEN.ID,
        with_merge_detail_dfs=False
    )

You can also just uncomment the block immediately below it that contains the
same new code in a Python comment. This now defines ``df`` as the combination
of the two data files joined together according to the values in their
biospecimen identifier columns.

Test Again
==========

Then again run

.. code-block:: text

    $ kidsfirst test my_study/ingest_package_1 --no_validate

Now you should see that it also pretends to load phenotypes from the new data
in addition to the rest of the information.
