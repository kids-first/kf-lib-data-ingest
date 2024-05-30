.. _about_concepts:

==============
Concept Schema
==============

A key part of the ingest library is the intermediate model that raw data is
mapped to. The intermediate model is called the "concept schema". The concept
schema is essentially just a list of column names that follow a standard that
denotes the type/concept and the attribute (e.g. `PARTICIPANT.GENDER`). Data
that has been mapped to the concept schema is later transformed into the final
schema that is used to create the tables in the target API.



.. toctree::
    :maxdepth: 2

    samples_and_specimens.rst
