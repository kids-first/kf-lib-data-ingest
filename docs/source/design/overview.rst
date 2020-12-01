.. _Design-Overview:

===============
Design Overview
===============

The ultimate goal for ingest is to take raw investigator data and compose from
it a series of simple and unambiguous factual statements of the form:

1. There is a thing X.
2. Thing X has properties A, B, and C.
3. Thing Y comes from thing X.

Example:

1. There is a participant P1.
2. Participant P1 has properties age=7, sex=male, and race=unknown.
3. There is a biospecimen S1.
4. Biospecimen S1 comes from Participant P1.

For the most part, the investigator's raw data tables already implicitly encode
the above statements as row colinearity. When the investigator puts a specimen
ID in the same row as a participant ID, usually that means that the specimen
came from that participant.

For instance, the above statements might have been written like this:

.. csv-table::
    :header: Participant ID, Specimen ID, Participant Age, Participant Sex, Participant Race

    P1, S1, 7, m, unknown

The Extract stage exists partly to fix any source data that either does *not*
encode the three fundamental statements by row colinearity or accidentally
encodes a relationship that does not actually exist among the things being
described.

.. toctree::
   :caption: Contents
   :maxdepth: 1

   value_principles
   extract_mapping
   transform
   load
   warehousing
   logs
