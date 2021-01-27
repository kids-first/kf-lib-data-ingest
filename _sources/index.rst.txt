.. image:: _static/images/logo.png
   :alt: Kids First Data Ingest Library

----

Library Code Repository: https://github.com/kids-first/kf-lib-data-ingest

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

Ingest App
==========

The library comes with a built-in command-line-interface-based (CLI) app
which is the primary user interface for executing the ingest pipeline.
Most users will use this app to create new ingest packages, test packages,
and ingest Kids First study datasets into the Kids First Data Service.

Ingest Packages
===============

In order to use this library to ingest a study, users must create ingest
packages which contain all of the necessary configuration defining basic ingest
input parameters and also how to extract and transform data.

Ingest Pipeline
===============

.. figure:: _static/images/ingest-pipeline.png
   :target: _images/ingest-pipeline.png
   :alt: Ingest Pipeline Overview

   Overview of the ingest pipeline

Users
-----

Users of the ingest system will likely fall into 3 categories:

1. **Ingest Operator**

   - Will likely just be running existing ingest packages
   - May need to learn how to modify configuration by inspection of existing
     ingest package configurations

2. **Ingest Package Developer**

   - Understands how to create new ingest packages and modify existing ones
   - Knows Python well and likely knows Pandas fairly well

3. **Target Service Plugin Developer**

   - Understands the intricacies of how to query and submit data to their
     target service
   - Understands the Load process and plugin API defined in this documentation
     and explored in existing target service plugins
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

#. **Retrieve** - Fetch the source data files, local or remote, and read them
   into memory
#. **Select** - Extract the desired subset of data from the source data files
#. **Clean** - Clean the source data (remove trailing spaces, select
   substrings, etc)
#. **Map** - Map the cleaned data to a set of standard Kids First attributes
   and values

The standard set of attributes and values are defined in the Kids First
Standard Concept Schema. (See `kf_lib_data_ingest/common/concept_schema.py
<https://github.com/kids-first/kf-lib-data-ingest/blob/master/kf_lib_data_ingest/common/concept_schema.py>`_
and `kf_lib_data_ingest/common/constants.py
<https://github.com/kids-first/kf-lib-data-ingest/blob/master/kf_lib_data_ingest/common/constants.py>`_)

Extract stage output might look like this:

.. csv-table:: data.tsv
    :header: "CONCEPT.PARTICIPANT.ID", "CONCEPT.PARTICIPANT.GENDER", "CONCEPT.BIOSPECIMEN.ID"

    "PID001", "Female", "SS001"
    "PID002", "Female", "SS002"
    "PID003", "Male", "SS003"

Transform Stage
---------------

Using a user-defined transform function, the transform stage combines the
individual tables created by the Extract Stage into a new set of "records"
tables where each table row contains all of the information for an instance of
a real world entity or event.

**Example:** If one of your extracted files contains participant age and
consent information, and another extracted file contains participant race and
ethnicity information, join the two tables on the participant ID column to
produce a new participant records table that contains age, consent, race, and
ethnicity for your participants.

**Example:** If half of your specimens are recorded in one file, and the other
half are recorded in another file, concatenate the extracted tables from those
two files together to produce one unified specimen records table.

Load Stage
----------

The Load Stage builds target entity payloads from the records emitted by the
transform stage, determines whether each entity already exists in the target
service by its uniquely identifying components, and then submits each completed
entity payload to the target service.

.. toctree::
   :caption: Table of Contents
   :maxdepth: 2

   self
   install
   quickstart
   design/overview.rst
   tutorial/index.rst
   developer
   reference/index.rst
