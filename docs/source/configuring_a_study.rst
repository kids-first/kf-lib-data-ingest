============================
Configuring your first study
============================

Study configuration consists of a directory containing three components:

1. A top-level study configuration file called ``dataset_ingest_config.yml``
    This defines metadata about the study itself such as what the study's name
    is and who the investigators are as well as paths to the other two
    components.
2. A directory of `Extract Configuration` files
    These define how data is interpreted and extracted from each of the files
    provided by the investigator.
3. (optional) A guided `Transform Module` file
    This replaces automatic graph-based transformation with a predictable set
    of user-programmed operations.

The study directory
===================

Your complete ingest configuration for a study will look like this::

    My_Study/
    ├── dataset_ingest_config.yml
    ├── extract_configs/
    │   ├── one_file_extract_config.py
    │   ├── another_file_extract_config.py
    │   └── ...
    └── transform_module.py

dataset_ingest_config.yml
=========================

This file contains details that control ingestion of the study as a whole. It
includes study metadata, the list of investigators, and paths to the extract
configs directory and the optional transform module.

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

Extract Configuration files
===========================

Every data file provided by the investigator will require at least one extract
configuration file. The reasons why will hopefully become clear after reading
this section.

Here's an example data file:
----------------------------

.. csv-table:: simple_headered_tsv_1.tsv
    :header: "participant", "mother", "father", "gender", "consent", "age in hours", "CLEFT_EGO", "CLEFT_ID", "age in hours", "EXTRA_EARDRUM"

    "PID001", "2", "3", "F", "1", "4", "TRUE", "FALSE", "4", "FALSE"
    "PID002", "", "", "", "1", "435", "TRUE", "FALSE", "435", "FALSE"
    "PID003", "", "", "", "1", "34", "TRUE", "FALSE", "34", "FALSE"
    "PID004", "5", "6", "M", "2", "4", "TRUE", "TRUE", "4", "FALSE"
    "PID005", "", "", "", "1", "345", "TRUE", "TRUE", "34", "FALSE"
    "PID006", "", "", "", "2", "34", "TRUE", "TRUE", "43545", "FALSE"
    "PID007", "8", "9", "M", "1", "34", "TRUE", "FALSE", "5", "TRUE"
    "PID008", "", "", "", "1", "43545", "TRUE", "TRUE", "52", "TRUE"
    "PID009", "", "", "", "1", "5", "FALSE", "TRUE", "25", "TRUE"

In order to merge this data into our system, we need to:
--------------------------------------------------------

* Convert the column headers into something standardized that the toolchain can
  understand
* Unify the formats of the participant IDs and the mother/father IDs
* Convert the M and F in the gender column to standardized values for
  Male/Female
* Convert the consent numbers to standardized consent codes
* Convert age in hours to age in days
* Reshape the CLEFT_EGO, CLEFT_ID, and EXTRA_EARDRUM columns into observation
  events
* Convert the TRUE and FALSE strings into standardized observation codes

The following extract configuration file accomplishes all of those needs
------------------------------------------------------------------------

(We'll explain how each piece works after showing the whole thing)

.. code-block:: python

    from kf_lib_data_ingest.common import constants
    from kf_lib_data_ingest.etl.extract.operations import *
    from kf_lib_data_ingest.etl.transform.standard_model.concept_schema import (
        CONCEPT
    )

    source_data_url = 'file://../data/simple_headered_tsv_1.tsv'

    source_data_loading_parameters = {}


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
        value_map(
            in_col="mother",
            m=lambda x: x,
            out_col=CONCEPT.PARTICIPANT.MOTHER_ID
        ),
        value_map(
            in_col="father",
            m=lambda x: x,
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
            in_col="consent",
            m={
                "1": constants.CONSENT_TYPE.GRU,
                "2": constants.CONSENT_TYPE.HMB_IRB,
                "3": constants.CONSENT_TYPE.DS_OC_PUB_MDS
            },
            out_col=CONCEPT.PARTICIPANT.CONSENT_TYPE
        ),
        [
            value_map(
                in_col=6,  # age in hours (first)
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
                in_col=9,  # age in hours (second)
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

Now let's break down how it works...

Imports!
--------

.. code-block:: python

    from kf_lib_data_ingest.common import constants
    from kf_lib_data_ingest.etl.extract.operations import *
    from kf_lib_data_ingest.etl.transform.standard_model.concept_schema import (
        CONCEPT
    )

It's a Python module! Cool! That lets us do all kinds of neat stuff like
import predefined constants and functions.

Fetching the data
-----------------

.. code-block:: python

    source_data_url = 'file://../data/simple_headered_tsv_1.tsv'

The first thing that the extractor does for every config file is fetch the
related source data. This specifies where the file lives so that the code can
fetch it.

Supported protocol prefixes are:
``file://``, ``s3://``, ``http://``, ``https://``

Loading the data
----------------

.. code-block:: python

    source_data_loading_parameters = {}

This file contains tab-separated values with a simple layout where the first
row is the column headers, so it gets loaded correctly by default. With more
complex files we'd have to define how to load the data. Those arguments would
go here. For more information on custom loading read <TODO>.

Extract operations
------------------

.. code-block:: python

    operations = [
        ...
    ]

This is a list of special functions that implement all of the
convert/unify/reshape/standardize stuff described earlier. The most useful
functions are already written for you. You just have to invoke them
appropriately.

For more information about extract operation functions, read <TODO>.

.. code-block:: python

    value_map(
        in_col="participant",
        m={
            r"PID(\d+)": lambda x: int(x),  # strip PID and 0-padding
        },
        out_col=CONCEPT.PARTICIPANT.ID
    )

This says "Use the ``participant`` column as input (in_col=), replace
everything that matches the regular expression pattern ``^PID(\d+)$`` with just
the captured part and remove the zero padding by running the captured
part through the function ``lambda x: int(x)`` (m={...}), and then output the result to a
``CONCEPT.PARTICIPANT.ID`` column (out_col=)."

The resulting intermediate output will look like:

.. csv-table::
    :header: "index", "CONCEPT.PARTICIPANT.ID"

    "0", "1"
    "1", "2"
    "2", "3"
    "3", "4"
    "4", "5"
    "5", "6"
    "6", "7"
    "7", "8"
    "8", "9"

``lambda x: int(x)`` could be replaced by just ``int``, since the two
expressions are functionally equivalent (both single-argument functions that
effectively strip the leading zeros).

We could also have kept these IDs as they were and instead converted the
mother/father IDs, but, in the absence of an overriding directive such as input
from the investigators about their preferences, it doesn't really make a
difference which way we choose.

.. code-block:: python

    value_map(
        in_col="gender",
        m={
            "F": constants.GENDER.FEMALE,
            "M": constants.GENDER.MALE
        },
        out_col=CONCEPT.PARTICIPANT.GENDER
    )

This says "Use the ``gender`` column as input (in_col=), replace everything
that matches the regular expression pattern ``^F$`` with the standard code for
Female and replace everything that matches ``^M$`` with the standard code for
Male (m={...}), and then output the result to a ``CONCEPT.PARTICIPANT.GENDER``
column (out_col=)."

