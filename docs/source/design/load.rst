.. _Design-Load:

=================
Load Stage Design
=================

The load stage is responsible for taking your transformed data and sending it
to the target service in the right format. It receives sets of records where
each set contains all of the necessary values for populating a kind of entity
in the target service, and it produces a series of service requests to make
service additions or updates.

To do that, the Load stage feeds data records through a plugin interface that
must be implemented for each target service.

Target service plugins implement the necessary routines for converting record
data coming in from the previous stages into message payloads and also
implement the logic for sending those payloads to the target service.

.. image:: /_static/images/load_stage.svg
  :alt: Diagram showing how the load stage hooks into target service plugins

**The plugin includes a builder class for each kind of entity that it wants to
send to the target service.**

Each builder class implements methods for the following behaviors:

#. (optional) **Transform the given records list into whatever new record
   format the entity builder needs**.
#. **Compose the entity's uniquely identifying primary elements**. This is used
   for querying identifiers assigned by the target service so that they can be
   repeatably assigned to the same entities again. (Target services often don't
   do this equivalent entity replacement themselves.)
#. **Build a complete message payload that will be sent to the server to
   populate a new (or updated) entity of its type**.

For more details, read :ref:`Tutorial-Make-Target-Service-Plugin`

----

**A Note on the ID Cache**

It may be easier to understand the purpose of the unique key components with an
example.

Given the record:

.. csv-table::
   :header: STUDY.TARGET_SERVICE_ID, PARTICIPANT.ID, PARTICIPANT.AGE, PARTICIPANT.SEX

   SD_12345678,  P1, 17, Male

The uniquely identifying components for the participant in this record would
be: ``{"study_id": "SD_12345678", "external_id": "P1"}`` because every
participant external ID is guaranteed to be unique only within a given study.
Any future corrections to the participant's age or sex should be associated
with the same entry.

Let's say that, after submission to the target service, the returned identifier
for the participant entity is something like: ``"PT_00001111"``.

The ingest library will internally create an association relating this unique
key with its target identifier like this:

+------------------------------------------------------------------+
| participants                                                     |
+--------------------------------------------------+---------------+
| Unique Components                                | Target ID     |
+==================================================+===============+
| {"study_id": "SD_12345678", "external_id": "P1"} |  PT_00001111  |
+--------------------------------------------------+---------------+

Then when the ingest library sees a record in the source data with the same
unique key components, it can always associate the resulting entity in the
target service with the same target ID. That way we can update characteristics
of existing participants without locally knowing in advance how the target
service identifies them.
