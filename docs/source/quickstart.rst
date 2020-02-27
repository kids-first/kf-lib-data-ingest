.. _Quickstart:

**********
Quickstart
**********

If you want to get up and running quickly with creating an ingest package
and running it, then the quick start guide is the best place to start. It's ok
if you do not understand what each of the steps are doing. The tutorial
section goes through the exact same demonstration as the quickstart, but
it includes detailed explanations for each step.

Generate a new ingest package
=============================

Each study that needs to be ingested will have a single directory that contains
all of the configuration needed by the ingest pipeline to extract, transform,
and load the study into a target data store or service. This is known as the
ingest package.

The first thing to do after installing the ingest library CLI
is to create a new ingest package named ``my_study``::

$ kidsfirst new --dest_dir=my_study

We can run it and test to make sure everything works by using the following
command

.. code-block:: text

    $ kidsfirst test my_study

The logs on your screen should indicate a successful test run like this:

.. code-block:: text

    ======================================== 1 passed in 0.12s ============================================================
    2020-02-18 17:40:36,039 - DataIngestPipeline - Thread: MainThread - INFO - ✅ User defined data validation tests passed
    2020-02-18 17:40:36,039 - DataIngestPipeline - Thread: MainThread - INFO - END data ingestion
    2020-02-18 17:40:36,039 - kf_lib_data_ingest.app.cli - Thread: MainThread - INFO - ✅ Ingest pipline passed validation!

So far your ingest package is pretty basic. It has 1 source data file and
the Python modules needed to extract, clean, and transform the data for the
target service. If you inspect it, it will look like this::

    my_study/
    ├── ingest_package_config.py
    ├── transform_module.py
    ├── data/
    │   └── clinical.tsv
    ├── extract_configs/
    │   └── extract_config.py
    └── tests/
        ├── conftest.py
        └── test_custom_counts.py

Modify a template extract config
================================
An extract config file does 3 things to the source data:

1. Select or extract the desired subset of data from the source data
2. Clean the selected data (remove trailing whitespaces, etc)
3. Map the cleaned data's attributes and values to Kids First entity attributes
   and acceptable values.

We're going to create an additional extract config to extract data from the
following source data file (different
from the file you already have):

.. csv-table:: family_and_phenotype.tsv
    :delim: tab
    :url: https://raw.githubusercontent.com/kids-first/kf-lib-data-ingest/master/docs/data/family_and_phenotype.tsv

The extract config file we're going to use is below. Much more detail on this
later.

.. code-block:: python

    from kf_lib_data_ingest.common import constants
    from kf_lib_data_ingest.etl.extract.operations import *
    from kf_lib_data_ingest.common.concept_schema import (
        CONCEPT
    )
    import re

    source_data_url = "https://raw.githubusercontent.com/kids-first/kf-lib-data-ingest/master/docs/data/family_and_phenotype.tsv"

    source_data_read_params = {
        "header": 1,
        "usecols": lambda x: x != "[ignore]"
    }


    def observed_yes_no(x):
        if isinstance(x, str):
            x = x.lower()
        if x in {"true", "yes", 1}:
            return constants.PHENOTYPE.OBSERVED.YES
        elif x in {"false", "no", 0}:
            return constants.PHENOTYPE.OBSERVED.NO
        elif x in {"", None}:
            return None


    operations = [
        value_map(
            in_col="participant",
            m={
                r"PID(\d+)": lambda x: int(x),  # strip PID and 0-padding
            },
            out_col=CONCEPT.PARTICIPANT.ID
        ),
        keep_map(
            in_col="mother",
            out_col=CONCEPT.PARTICIPANT.MOTHER_ID
        ),
        keep_map(
            in_col="father",
            out_col=CONCEPT.PARTICIPANT.FATHER_ID
        ),
        value_map(
            in_col="gender",
            # Don't worry about mother/father gender here.
            # We can create them in a later phase.
            m={
                "F": constants.GENDER.FEMALE,
                "M": constants.GENDER.MALE
            },
            out_col=CONCEPT.PARTICIPANT.GENDER
        ),
        value_map(
            in_col="specimens",
            m=lambda x: Split(re.split("[,;]", x)),
            out_col=CONCEPT.BIOSPECIMEN.ID
        ),
        [
            value_map(
                in_col=6,  # age (hrs) (first)
                m=lambda x: int(x) / 24,
                out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
            ),
            melt_map(
                var_name=CONCEPT.PHENOTYPE.NAME,
                map_for_vars={
                    "CLEFT_EGO": "Cleft ego",
                    "CLEFT_ID": "Cleft id"
                },
                value_name=CONCEPT.PHENOTYPE.OBSERVED,
                map_for_values=observed_yes_no
            )
        ],
        [
            value_map(
                in_col=9,  # age (hrs) (second)
                m=lambda x: int(x) / 24,
                out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
            ),
            melt_map(
                var_name=CONCEPT.PHENOTYPE.NAME,
                map_for_vars={
                    "EXTRA_EARDRUM": "Extra eardrum"
                },
                value_name=CONCEPT.PHENOTYPE.OBSERVED,
                map_for_values=observed_yes_no
            )
        ]
    ]

It goes in the ``extract_configs`` directory like this::

    my_study/
    ├── data/
    │   └── clinical.tsv
    └── extract_configs/
        ├── extract_config.py
        └── family_and_phenotype.py

Debugging
=========

Run single stage
----------------

The entire ingest pipeline consists of the familiar ETL steps (extract,
transform, and load). The package allows any logical subset of these three
stages to be executed instead (output from the previous stages must exist in
order for extract or transform to be run). This can be done using the
``--stages`` argument with a subset of the char sequence ``etl``.

View ingest log
---------------

The pipeline logs messages to the console and also stores them in a log file,
which is by default located at ``<ingest package dir>/logs/ingest.log``.

View single stage output
------------------------

Every ingest stage has the option to write its output to a directory that
follows this path pattern: ``<ingest package dir>/output/<name of stage>``.

Modify the Transform module
===========================

Next in the standard ETL sequence is the transform step. This will merge our
extracted tables together such that the data needed for generating complete
target entities from our extracted data is properly connected.

For this demo, this is controlled by the file ``transform_module.py``. Check it
out:

.. code-block:: python

    """
    Auto-generated transform module

    Replace the contents of transform_function with your own code

    See documentation at
    https://kids-first.github.io/kf-lib-data-ingest/ for information on
    implementing transform_function.
    """

    from kf_lib_data_ingest.common.concept_schema import CONCEPT
    # Use these merge funcs, not pandas.merge
    from kf_lib_data_ingest.common.pandas_utils import (
        merge_wo_duplicates,
        outer_merge
    )
    from kf_lib_data_ingest.config import DEFAULT_KEY


    def transform_function(mapped_df_dict):
        """
        Merge DataFrames in mapped_df_dict into 1 DataFrame if possible.

        Return a dict that looks like this:

        {
            DEFAULT_KEY: all_merged_data_df
        }

        If not possible to merge all DataFrames into a single DataFrame then
        you can return a dict that looks something like this:

        {
            '<name of target concept>': df_for_<target_concept>,
            DEFAULT_KEY: all_merged_data_df
        }

        Target concept instances will be built from the default DataFrame unless
        another DataFrame is explicitly provided via a key, value pair in the
        output dict. They key must match the name of an existing target concept.
        The value will be the DataFrame to use when building instances of the
        target concept.

        A typical example would be:

        {
            'family_relationship': family_relationship_df,
            'default': all_merged_data_df
        }

        """
        df = mapped_df_dict["extract_config.py"]

        # df = outer_merge(
        #     mapped_df_dict['extract_config.py'],
        #     mapped_df_dict['family_and_phenotype.py'],
        #     on=CONCEPT.BIOSPECIMEN.ID,
        #     with_merge_detail_dfs=False
        # )

        return {DEFAULT_KEY: df}

The interesting part that we need is the block at the bottom that's commented
out. Uncomment this block and then run ``kidsfirst test`` to perform
extraction and tranformation.

Run the ingest pipeline
=======================

The Warehouse Server
--------------------

Data that we ingest is meant to be shared, not stored locally on your machine!
It's easy to configure the ingest system to connect to a remote warehousing
database which will centrally store the relevant data.

We're ready to test the ingest package again, and, since we're just seeing how
this package works on dummy data, we don't want
to actually upload anything to a warehouse. This won't happen if we use the
``--no_warehouse`` option command, like so::

    $ kidsfirst test --no_warehouse my_study

If the ingest package passes validation with the test command and you're happy
with the output, you're ready to ingest the data into your target service using
the ingest command::

    $ kidsfirst ingest my_study

The library defaults to ingesting data into the Kids First Data Service at
the base url: ``http://localhost:5000``. You can change the base URL for this
service using the ``--target_url`` CLI option.
