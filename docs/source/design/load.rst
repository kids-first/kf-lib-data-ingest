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
#. (soon to be optional) **Compose a unique keystring from the entity's primary
   constituent elements**. This is used internally for caching identifiers
   assigned by the target service (received from the target service as part of
   the response for sending an entity payload) so that they can be repeatably
   assigned to the same entities again. (Target services often don't do this
   themselves.)
#. **Build a message payload that will be sent to the server to populate a new
   (or updated) entity of its type**. The unique keystrings generated in the
   previous steps are passed to this method as well so that they can be
   included as part of the payload if desired.

For more details, read :ref:`Tutorial-Make-Target-Service-Plugin`

----

**A Note on the ID Cache**

It may be easier to understand the purpose of the ID cache and
unique keys with an example.

Given the record:

.. csv-table::
   :header: PARTICIPANT.ID, PHENOTYPE.NAME, PHENOTYPE.EVENT_AGE_DAYS,\
            PHENOTYPE.OBSERVED

   P1,  Cleft Ear, 765, Positive

The unique keystring for the phenotype observation in this record could be:
``"p1__cleft-ear__765"``. (Assume that participant P1 is not Schrödinger's cat
and can only be either positive or negative for that characteristic at that
time, but not both simultaneously.)

And, after submission to the target service, the returned identifier for the
phenotype observation entity might be something like: ``"PH_00001111"``.

The ingest library will internally create an entry in an ID cache for the
current target service URL which associates this unique key with its target
identifier like this:

+------------------------------------+
| phenotypes                         |
+--------------------+---------------+
| Unique Key         | Target ID     |
+====================+===============+
| p1__cleft-ear__765 |  PH_00001111  |
+--------------------+---------------+

Then whenever the ingest library sees a record in the source data with the
unique key components above, it will always associate the resulting phenotype
observation with the unique keystring ``"p1__cleft-ear__765"`` which will
always point to the phenotype observation entity in the target service
identified by ``"PH_00001111"``.
