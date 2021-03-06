.. _Extract-Example:

Example Extract Configuration
=============================

Every data file provided by the investigator will require at least one extract
configuration file. The reasons why will hopefully become clear after reading
this section.

We will step through how to write an extract configuration file for a source
data file.

Here's an example source data file:
-----------------------------------

.. csv-table:: family_and_phenotype.tsv
    :header-rows: 2
    :delim: tab
    :file: ../../../data/family_and_phenotype.tsv

In order to ingest this data into the Kids First ecosystem, we will need to:
----------------------------------------------------------------------------

* Ignore everything marked with [ignore], meaning our table actually starts at
  the second row and second column
* Convert the column headers into a standardized set of concepts that the
  toolchain can understand. The values are defined in the Standard Concept
  Schema located in
  ``kf_lib_data_ingest.common.concept_schema.CONCEPT``
* Unify the formats of the participant IDs and the mother/father IDs
* Convert the M and F in the gender column to standardized values for
  Male/Female found in ``kf_lib_data_ingest.common.constants``, because we want
  to use standard constant codes wherever possible.
* Split delimited specimens apart into their own rows
* Convert age (hrs) to age in days
* Reshape the CLEFT_EGO, CLEFT_ID, and EXTRA_EARDRUM columns into observation
  events
* Convert the TRUE and FALSE strings into standardized observation codes

The following extract configuration file accomplishes all of those needs
------------------------------------------------------------------------

(We will explain how each piece works after showing the whole thing)

.. literalinclude:: family_and_phenotype.py
   :caption: family_and_phenotype.py

First, download :download:`family_and_phenotype.py<family_and_phenotype.py>`
and put it in the extract_configs directory like this::

    my_study/
    └── ingest_package_1/
        └── extract_configs/
           └── family_and_phenotype.py

Now let's break down how it works...

Imports!
--------

.. code-block:: python

    import re
    from kf_lib_data_ingest.common import constants
    from kf_lib_data_ingest.common.concept_schema import CONCEPT
    from kf_lib_data_ingest.common.pandas_utils import Split
    from kf_lib_data_ingest.etl.extract.operations import *

It's a Python module! Cool! That lets us do all kinds of neat stuff like import
predefined constants and functions and use arbitrarily complex function code to
process our data.

.. _Fetching-the-Data:

Fetching the data
-----------------

.. code-block:: python

    source_data_url = "https://raw.githubusercontent.com/kids-first/kf-lib-data-ingest/master/docs/data/family_and_phenotype.tsv"

The first thing that the extractor does for every config file is fetch the
related source data. This specifies where the file lives so that the code can
fetch it.

Supported protocol prefixes are:
``file://``, ``s3://``, ``http://``, ``https://``

For files stored by the Kids First Data Tracker, see the
":ref:`Tutorial-Study-Creator`" tutorial.

If you downloaded the test data file and want to load your local copy instead
of from a remote server, you would use:

.. code-block:: python

    source_data_url = "file://path/to/family_and_phenotype.tsv"

Reading the file
----------------

.. code-block:: python

    source_data_read_params = {
        "header": 1,
        "usecols": lambda x: x != "[ignore]"
    }

Extract tries to automatically pick the right pandas file reader for the given
file extension (read_json for json files, read_csv for csv/tsv, read_excel for
excel files), but sometimes the reader needs guidance from input parameters. To
pass keyword parameters to the file reader, we define a dict called
``source_data_read_params`` which corresponds with the Python pandas IO
parameters described in
http://pandas.pydata.org/pandas-docs/stable/user_guide/io.html

The example data file contains tab-separated values (hence the filename ending
with ".tsv") with a non-standard layout where we need to ignore the first row
and the first column.

If the data had had the simplest layout (the column headers being on the first
row, etc), then it would get loaded correctly by default without needing any
parameters here, but with complex arrangements we have to configure the reader.

Extract operations
------------------

The operations list
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    operations = [
        ...
    ]

This is a list of special functions that the extract stage will execute to
select subsets of source data and then clean and map that data to the desired
attributes and value formats. The most useful functions are already written for
you. You just have to invoke them appropriately.

For more information about extract operation functions, read
:ref:`Map-Operations`.

A value map operation with functional replacements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    value_map(
        in_col="participant",
        m={
            r"PID(\d+)": lambda x: int(x),  # strip PID and 0-padding
        },
        out_col=CONCEPT.PARTICIPANT.ID
    )

This says "Use the ``"participant"`` column as input, replace everything that
matches (``m={...}``) the regular expression pattern ``^PID(\d+)$`` with just
the captured part and remove the zero padding by running the captured part
through the function ``lambda x: int(x)``, and then output the result to a
standard concept column for the participant ID."

The resulting intermediate output will look like:

.. csv-table::
    :header: "index", "<CONCEPT.PARTICIPANT.ID>"

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

A keep-the-original-values map operation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    keep_map(
        in_col="mother",
        out_col=CONCEPT.PARTICIPANT.MOTHER_ID
    )

This says "Put all of the values from the ``"mother"`` column into a standard
concept column for the ID of the participant's mother, but keep all of the
values the same." ``keep_map`` is the same as a ``value_map`` where every value
is mapped to itself.

The resulting intermediate output will look like:

.. csv-table::
    :header: "index", "<CONCEPT.PARTICIPANT.MOTHER_ID>"

    "0", "2"
    "1", ""
    "2", ""
    "3", "5"
    "4", ""
    "5", ""
    "6", "8"
    "7", ""
    "8", ""

A value map operation with variable replacements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    value_map(
        in_col="gender",
        m={
            "F": constants.GENDER.FEMALE,
            "M": constants.GENDER.MALE
        },
        out_col=CONCEPT.PARTICIPANT.GENDER
    )

This says "Use the ``gender`` column as input, replace everything that matches
the regular expression pattern ``^F$`` with the standard code for Female and
replace everything that matches ``^M$`` with the standard code for Male, and
then output the result to a standard concept column for participant gender."

The resulting intermediate output will look like:

.. csv-table::
    :header: "index", "<CONCEPT.PARTICIPANT.ID>"

    "0", "Female"
    "1", ""
    "2", ""
    "3", "Male"
    "4", ""
    "5", ""
    "6", "Male"
    "7", ""
    "8", ""

A value map that splits cells apart
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    value_map(
        in_col="specimens",
        m=lambda x: Split(re.split("[,;]", x)),
        out_col=CONCEPT.BIOSPECIMEN.ID
    )

This says "Use the ``specimens`` column as input, split any ``,`` or ``;``
delimited values apart into their own entries, and then output the result to a
standard concept column for biospecimen ID."

.. note::

    We use the special ``Split()`` object for lists of values that we want to
    split apart into multiple rows. Just returning a list will not split the
    contained items apart. Read :ref:`Splitting-Cells`.

The resulting intermediate output will look like:

.. csv-table::
    :header: "index", "<CONCEPT.BIOSPECIMEN.ID>"

    "0", "SP001A"
    "0", "SP001B"
    "1", "SP002A"
    "1", "SP002B"
    "2", "SP003A"
    "2", "SP003B"
    "3", "SP004A"
    "3", "SP004B"
    "4", "SP005A"
    "4", "SP005B"
    "5", "SP006"
    "6", "SP007"
    "7", "SP008A"
    "7", "SP008B"
    "8", "SP009A"
    "8", "SP009B"
    "0", "SP001A"
    "0", "SP001B"
    "1", "SP002A"
    "1", "SP002B"
    "2", "SP003A"
    "2", "SP003B"
    "3", "SP004A"
    "3", "SP004B"
    "4", "SP005A"
    "4", "SP005B"
    "5", "SP006"
    "6", "SP007"
    "7", "SP008A"
    "7", "SP008B"
    "8", "SP009A"
    "8", "SP009B"
    "0", "SP001A"
    "0", "SP001B"
    "1", "SP002A"
    "1", "SP002B"
    "2", "SP003A"
    "2", "SP003B"
    "3", "SP004A"
    "3", "SP004B"
    "4", "SP005A"
    "4", "SP005B"
    "5", "SP006"
    "6", "SP007"
    "7", "SP008A"
    "7", "SP008B"
    "8", "SP009A"
    "8", "SP009B"

A melt map operation
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

        melt_map(
            var_name=CONCEPT.PHENOTYPE.NAME,
            map_for_vars={
                "CLEFT_EGO": "Cleft ego",
                "CLEFT_ID": "Cleft id"
            },
            value_name=CONCEPT.PHENOTYPE.OBSERVED,
            map_for_values=observed_yes_no
        )

This says "Generate new standard concept columns for phenotype name and
observation by melting (read
https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.melt.html)
the ``CLEFT_EGO`` and ``CLEFT_ID`` columns into the `variables` ``Cleft ego``
and ``Cleft id`` and map the ``TRUE``/``FALSE`` `values` by passing them
through the included ``observed_yes_no`` function."

The resulting intermediate output will look like:

.. csv-table::
    :header: "index", "<CONCEPT.PHENOTYPE.NAME>", "<CONCEPT.PHENOTYPE.OBSERVED>"

    "0", "Cleft ego", "Positive"
    "1", "Cleft ego", "Positive"
    "2", "Cleft ego", "Positive"
    "3", "Cleft ego", "Positive"
    "4", "Cleft ego", "Positive"
    "5", "Cleft ego", "Positive"
    "6", "Cleft ego", "Positive"
    "7", "Cleft ego", "Positive"
    "8", "Cleft ego", "Negative"
    "0", "Cleft id", "Negative"
    "1", "Cleft id", "Negative"
    "2", "Cleft id", "Negative"
    "3", "Cleft id", "Positive"
    "4", "Cleft id", "Positive"
    "5", "Cleft id", "Positive"
    "6", "Cleft id", "Negative"
    "7", "Cleft id", "Positive"
    "8", "Cleft id", "Positive"

A nested operation sub-list
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

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
    ]

Having a sub-list says "Treat the enclosed operations as a single
logically-linked unit".

For this particular scenario it gives a way to say that **these** phenotype
columns go with **this** age column and not **that other** age column. It
should also always be possible to accomplish the same thing by making a
separate extract configuration file for those operations.

The resulting intermediate output for both of these operations together will
look like:

.. csv-table::
    :header: "index", "<CONCEPT.PHENOTYPE.EVENT_AGE_DAYS>", "<CONCEPT.PHENOTYPE.NAME>", "<CONCEPT.PHENOTYPE.OBSERVED>"

    "0", "0.166667", "Cleft ego", "Positive"
    "1", "18.125", "Cleft ego", "Positive"
    "2", "1.416667", "Cleft ego", "Positive"
    "3", "0.166667", "Cleft ego", "Positive"
    "4", "14.375", "Cleft ego", "Positive"
    "5", "1.416667", "Cleft ego", "Positive"
    "6", "1.416667", "Cleft ego", "Positive"
    "7", "1814.375", "Cleft ego", "Positive"
    "8", "0.208333", "Cleft ego", "Negative"
    "0", "0.166667", "Cleft id", "Negative"
    "1", "18.125", "Cleft id", "Negative"
    "2", "1.416667", "Cleft id", "Negative"
    "3", "0.166667", "Cleft id", "Positive"
    "4", "14.375", "Cleft id", "Positive"
    "5", "1.416667", "Cleft id", "Positive"
    "6", "1.416667", "Cleft id", "Negative"
    "7", "1814.375", "Cleft id", "Positive"
    "8", "0.208333", "Cleft id", "Positive"

The final Extraction product
----------------------------

Once all of the operations are complete and the extract stage has done its
magic, the final extracted result given the data and our configuration is:

.. csv-table::
    :header: "index", "<CONCEPT.PARTICIPANT.ID>", "<CONCEPT.PARTICIPANT.MOTHER_ID>", "<CONCEPT.PARTICIPANT.FATHER_ID>", "<CONCEPT.PARTICIPANT.GENDER>", "<CONCEPT.BIOSPECIMEN.ID>", "<CONCEPT.PHENOTYPE.EVENT_AGE_DAYS>", "<CONCEPT.PHENOTYPE.NAME>", "<CONCEPT.PHENOTYPE.OBSERVED>"

    "0", "1", "2", "3", "Female", "SP001A", "0.166666667", "Cleft ego", "Positive"
    "0", "1", "2", "3", "Female", "SP001B", "0.166666667", "Cleft ego", "Positive"
    "1", "2", "", "", "", "SP002A", "18.125", "Cleft ego", "Positive"
    "1", "2", "", "", "", "SP002B", "18.125", "Cleft ego", "Positive"
    "2", "3", "", "", "", "SP003A", "1.416666667", "Cleft ego", "Positive"
    "2", "3", "", "", "", "SP003B", "1.416666667", "Cleft ego", "Positive"
    "3", "4", "5", "6", "Male", "SP004A", "0.166666667", "Cleft ego", "Positive"
    "3", "4", "5", "6", "Male", "SP004B", "0.166666667", "Cleft ego", "Positive"
    "4", "5", "", "", "", "SP005A", "14.375", "Cleft ego", "Positive"
    "4", "5", "", "", "", "SP005B", "14.375", "Cleft ego", "Positive"
    "5", "6", "", "", "", "SP006", "1.416666667", "Cleft ego", "Positive"
    "6", "7", "8", "9", "Male", "SP007", "1.416666667", "Cleft ego", "Positive"
    "7", "8", "", "", "", "SP008A", "1814.375", "Cleft ego", "Positive"
    "7", "8", "", "", "", "SP008B", "1814.375", "Cleft ego", "Positive"
    "8", "9", "", "", "", "SP009A", "0.208333333", "Cleft ego", "Negative"
    "8", "9", "", "", "", "SP009B", "0.208333333", "Cleft ego", "Negative"
    "0", "1", "2", "3", "Female", "SP001A", "0.166666667", "Cleft id", "Negative"
    "0", "1", "2", "3", "Female", "SP001B", "0.166666667", "Cleft id", "Negative"
    "1", "2", "", "", "", "SP002A", "18.125", "Cleft id", "Negative"
    "1", "2", "", "", "", "SP002B", "18.125", "Cleft id", "Negative"
    "2", "3", "", "", "", "SP003A", "1.416666667", "Cleft id", "Negative"
    "2", "3", "", "", "", "SP003B", "1.416666667", "Cleft id", "Negative"
    "3", "4", "5", "6", "Male", "SP004A", "0.166666667", "Cleft id", "Positive"
    "3", "4", "5", "6", "Male", "SP004B", "0.166666667", "Cleft id", "Positive"
    "4", "5", "", "", "", "SP005A", "14.375", "Cleft id", "Positive"
    "4", "5", "", "", "", "SP005B", "14.375", "Cleft id", "Positive"
    "5", "6", "", "", "", "SP006", "1.416666667", "Cleft id", "Positive"
    "6", "7", "8", "9", "Male", "SP007", "1.416666667", "Cleft id", "Negative"
    "7", "8", "", "", "", "SP008A", "1814.375", "Cleft id", "Positive"
    "7", "8", "", "", "", "SP008B", "1814.375", "Cleft id", "Positive"
    "8", "9", "", "", "", "SP009A", "0.208333333", "Cleft id", "Positive"
    "8", "9", "", "", "", "SP009B", "0.208333333", "Cleft id", "Positive"
    "0", "1", "2", "3", "Female", "SP001A", "0.166666667", "Extra eardrum", "Negative"
    "0", "1", "2", "3", "Female", "SP001B", "0.166666667", "Extra eardrum", "Negative"
    "1", "2", "", "", "", "SP002A", "18.125", "Extra eardrum", "Negative"
    "1", "2", "", "", "", "SP002B", "18.125", "Extra eardrum", "Negative"
    "2", "3", "", "", "", "SP003A", "1.416666667", "Extra eardrum", "Negative"
    "2", "3", "", "", "", "SP003B", "1.416666667", "Extra eardrum", "Negative"
    "3", "4", "5", "6", "Male", "SP004A", "0.166666667", "Extra eardrum", "Negative"
    "3", "4", "5", "6", "Male", "SP004B", "0.166666667", "Extra eardrum", "Negative"
    "4", "5", "", "", "", "SP005A", "1.416666667", "Extra eardrum", "Negative"
    "4", "5", "", "", "", "SP005B", "1.416666667", "Extra eardrum", "Negative"
    "5", "6", "", "", "", "SP006", "1814.375", "Extra eardrum", "Negative"
    "6", "7", "8", "9", "Male", "SP007", "0.208333333", "Extra eardrum", "Positive"
    "7", "8", "", "", "", "SP008A", "2.166666667", "Extra eardrum", "Positive"
    "7", "8", "", "", "", "SP008B", "2.166666667", "Extra eardrum", "Positive"
    "8", "9", "", "", "", "SP009A", "1.041666667", "Extra eardrum", "Positive"
    "8", "9", "", "", "", "SP009B", "1.041666667", "Extra eardrum", "Positive"
