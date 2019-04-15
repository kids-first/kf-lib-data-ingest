.. image:: _static/images/logo.png
   :alt: Kids First Data Ingest Library

********
Overview
********

The Kids First Data Ingest Library is both an ETL (extract, transform, load)
framework and a library that standardizes the ingestion of raw Kids First study
data into target Kids First services.

Framework and Library
=====================

Since this is a library as well as a framework, there are components of the
``kf_lib_data_ingest`` package that can be used as standalone tools or
utilities. The ``ExtractStage`` for example can be used as a standalone tool
for data selection and mapping (see :ref:`Tutorial-Extract-Stage` for details).
Anything in ``kf_lib_data_ingest.utilities`` can be used as a standalone tool.

Additionally, there are a handful of helper functions in
``kf_lib_data_ingest.common.pandas_utils`` that could be used outside of the
framework for data wrangling tasks. See <TODO> for details.

Command Line Interface
======================

A command line interface (CLI) wraps the library and is the primary user
interface for executing the ingest pipeline. Users will use the CLI to create
new ingest packages, test packages, and ingest Kids First X01 study datasets
into the Kids First Data Service. Currently, the Kids First Data Service
is the only supported target service at this point.

Ingest Packages
==========================
In order to use this library to ingest a study, users must create an ingest
package which contains all of the necessary configuration expected by the
library's ingest pipeline. The ingest package is just configuration as code.
It contains Python files with code for extracting and transforming data,
and basic ingest input parameters, etc.

Ingest Pipeline
===============

.. figure:: _static/images/ingest-pipeline.png
   :target: _images/ingest-pipeline.png
   :alt: Ingest Pipeline Overview

   Overview of the ingest pipeline

Users
-----

Users of the ingest system will likely fall into 2 categories:

1. **Ingest Operator**

   - Will never create a new study ingest package
   - Will likely just be running existing ingest packages
   - May need to learn how to modify configuration by inspection of existing
     ingest package configurations

2. **Ingest Package Developer**

   - Runs ingest packages
   - Understands how to create new ingest packages and modify existing ones
   - Knows Python well and likely knows Pandas fairly well

Inputs
------

- **Local or remote source data files**
- **Configuration files**

Source data files might look like this:

.. csv-table:: data.tsv
    :header: "p id", "gender", "sample id"

    "PID001", "f", "SS001"
    "PID002", "female", "SS002"
    "PID003", "m", "SS003"


Outputs
-------

- **Ingest log** - A log file containing runtime details of an executed ingest
  job
- **Serialized stage output** - The output of each stage execution in a
  serializable form written to disk


Extract Stage
-------------

The extract stage does the following:

1. **Retrieve** - Retrieve the source data files whether they are local or
   remote and read into memory
2. **Select** - Extract the desired subset of data from the source data files
3. **Clean**- Clean the source data (i.e. remove trailing whitespaces, etc)
4. **Map** - Map the cleaned data to a set of standard Kids First attributes
   and values

The standard set of attributes and values are defined in the Kids First
Standard Concept Schema. See <TODO> for more details. Extract stage output
might look like this:

.. csv-table:: data.tsv
    :header: "CONCEPT.PARTICIPANT.ID", "CONCEPT.PARTICIPANT.GENDER", "CONCEPT.BIOSPECIMEN.ID"

    "PID001", "Female", "SS001"
    "PID002", "Female", "SS002"
    "PID003", "Male", "SS003"

Transform Stage
---------------

The transform stage converts the clean and standardized data into a form that
closely resembles what is expected by the target database or service.
These are the main steps transform stage takes to make this happen:

1. **Merge ExtractStage tables into 1 table**

    The transform stage applies the user defined transform function which
    specifies how the individual tables from ExtractStage should be merged
    into a single table.

2. **Convert from standard form to target form**

    Next, the transform stage iterates over the rows in the merged table
    and emits a target concept instance (dict) for each row.

    Columns/standard concept attributes in the merged table are converted
    to target concept properties using the Kids First target API configuration
    file.

3. **Apply common transformations**

    For example, filling in left over null or unknown values with standard
    values (i.e. Not Reported, Unknown).


Transform stage output might look like this:

.. code-block:: json

    {
        "participant": [
            {
                "endpoint": "/participants",
                "id": "P001",
                "links": {
                    "family_id": null,
                    "study_id": null
                },
                "properties": {
                    "affected_status": null,
                    "consent_type": "GRU",
                    "ethnicity": "Not Reported",
                    "external_id": "1",
                    "gender": "Female",
                    "is_proband": null,
                    "race": "Not Reported",
                    "visible": null
                }
            }
        ]
    }



Load Stage
----------

At this point, the data is in a form that is almost ready to be submitted to
the target service. The load stage will take the necessary steps to build
the target payloads and either POST or PATCH them to the target service.

You will learn more about how the load stage determines whether to send a
POST vs PATCH request in the tutorial.

Head on over to the :ref:`tutorial` to get started with data ingestion!

.. toctree::
   :caption: Table of Contents
   :maxdepth: 2

   install
   quickstart
   tutorial/index.rst
   developer
   reference/index.rst
   design/overview
