.. image:: _static/images/logo.png
   :alt: Kids First Data Ingest Library

*********
Overview
*********

The Kids First Data Ingest Library is both an ETL (extract, transform, load)
framework and a library that standardizes the ingestion of raw Kids First study
data into target Kids First services.

Framework and Library
=======================
Since this is a library as well as a framework, there are components of the
``kf_lib_data_ingest`` package that can be used as standalone tools or
utilities. The ``ExtractStage`` for example can be used as a standalone tool
for data selection and mapping (see :ref:`Tutorial-Extract-Stage` for details).
Anything in ``kf_lib_data_ingest.utilities`` can be used as a standalone tool.

Additionally, there are a handful of helper functions in
``kf_lib_data_ingest.common.pandas_utils`` that could be used outside of the
framework for data wrangling tasks. See <TODO> for details.

Command Line Interface
=======================
A command line interface (CLI) wraps the library and is the primary user
interface for executing the ingest pipeline. Users will use the CLI to generate
new ingest modules and run ingest jobs to ETL Kids First X01 study datasets
into the Kids First Data Service. The Data Service is currently the only
supported target service at this point.

Ingest Pipeline
===============

.. figure:: _static/images/ingest-pipeline.png
   :alt: Ingest Pipeline Overview

   Overview of the ingest pipeline

Users
-----
Users of the ingest system will likely fall into 3 categories:

1. **Ingest Operator**
   - Will never create a new study ingest module
   - Will likely just be running existing ingest modules
   - May need to learn how to modify configuration by inspection of existing
   ingest module configurations.

2. **Ingest Configuration Developer**
   - Runs ingest modules
   - Understands how to create new configurations and modify existing ones.
   - Knows Python well and likely knows Pandas fairly well.

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
- **Ingest log**
    - A log file containing runtime details of an executed ingest job
- **Serialized stage output**
    - The output of each stage execution in a serializable form written to disk


Extract Stage
-------------
The extract stage does the following:

1. **Retrieve** - Retrieve the source data files whether they are local or
remote and read into memory.
2. **Select** - Extract the desired subset of data from the source data files
3. **Clean**- Clean the source data (i.e. remove trailing whitespaces, etc)
4. **Map** - Map the cleaned data to a set of standard Kids First attributes
and values.

The standard set of attributes and values are defined in the Kids First
Standard Concept Schema. See <TODO> for more details. Extract stage output
might look like this:

.. csv-table:: data.tsv
    :header: "CONCEPT.PARTICIPANT.ID", "CONCEPT.PARTICIPANT.GENDER", "CONCEPT.BIOSPECIMEN.ID"

    "PID001", "Female", "SS001"
    "PID002", "Female", "SS002"
    "PID003", "Male", "SS003"

Transform Stage
----------------
The transform stage converts the clean and standardized data into a form that
is expected by the target database or service.

1. **Convert from standard form to target form**

    To do this, the transform stage requires an additional input -
    the target API configuration file. This file specifies how entities and
    their attributes in the Kids First Standard Concept Schema map to entities
    and their attributes in the target service.

2. **Apply common transformations**

    For example, filling in left over null or unknown values with standard
    values (i.e. Not Reported, Unknown).

3. **Translate Source Data Entity IDs**

    Every time the ingest pipeline runs and loads new entities into the target,
    it keeps a record of the ID created by the target service for an entity and
    the corresponding ID for that entity in the source data.

    This part of transform looks up a source data ID in the ID cache and
    translates it to its corresponding target service ID, if one exists.

Transform stage output might look like this:

.. code-block:: json

    {
        'participant': [
            {
                'kf_id': 'PT_00001111',
                'external_id': 'PID001',
                'gender': 'Female',
            },
            {
                'kf_id': 'PT_00001112',
                'external_id': 'PID002',
                'gender': 'Female',
            }
            {
                'kf_id': 'PT_00001113',
                'external_id': 'PID003',
                'gender': 'Male',
            }
        ]
    }



Load Stage
-----------
At this point, all transformations to data values are complete and the
data is in a form that is expected by the target database or service.
The load stage transmits the entity payloads to the target.


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
