.. _Tutorial-Make-Target-Service-Plugin:

##############################
Making a Target Service Plugin
##############################

You have a bunch of extracted data that has been suitably merged together, and
you have a server with a submission API, and now you want to put that data into
that server. You need a target service plugin module that will convert from
your tabular extracted data into the appropriate form for sending to your
target service and that will negotiate the submission according to the demands
of the service.

Existing Plugins For Reference
==============================

Here are two complete examples that work with the same input data for different
target servers. Seeing what they look like in their entirety may be helpful in
understanding what we're trying to make:

- | Plugin for loading data into the Kids First FHIR server:
  | https://github.com/kids-first/kf-model-fhir/tree/master/kf_model_fhir/ingest_plugin

- | Plugin for loading data into the Kids First non-FHIR Dataservice:
  | https://github.com/kids-first/kf-lib-data-ingest/blob/master/kf_lib_data_ingest/target_api_plugins/kids_first_dataservice.py

The Parts of a Target Service Plugin
====================================

The target service plugin API has gone through multiple revisions. This
documentation only describes the latest version of the plugin API (currently
version 2). These are identified by defining a variable ``LOADER_VERSION = 2``
in the plugin body. Plugins that don't define this variable are assumed to use
version 1 of the plugin interface for backwards compatibility.

**Target service plugins have two parts:**

#. **The plugin API version identifier**

   - | As described above, after version 1, plugins must indicate which version
       of the plugin API they conform to by defining a variable
       ``LOADER_VERSION = <version number>`` in the plugin body.
     | Example: ``LOADER_VERSION = 2``

#. **The list of entity builder classes**

   - These classes are responsible for converting lists of records into
     entity payloads, querying the target server for existing entities that
     have the same key components, and submitting completed payloads to the
     target service. Putting the builder classes into a list lets us indicate
     which order to load entities in.

The target service plugin structure details are explained in the
:ref:`Design-Load` section and in the header of
``kf_lib_data_ingest/etl/configuration/target_api_config.py``:

.. literalinclude:: ../../../../kf_lib_data_ingest/etl/configuration/target_api_config.py
   :start-after: """
   :end-before: """

Example
=======

Say that you have a hypothetical service with the following specification for
submitting Participant data:

.. image:: ../../_static/images/example_target_api.png
  :alt: HTTP POST API specification for submitting new participants

Example Entity Builder Class
----------------------------

This class tells the Load stage how to build our hypothetical participants from
extracted data:

.. code-block:: python

    from kf_lib_data_ingest.common.concept_schema import CONCEPT
    import requests

    class Participant:
        class_name = "participant"
        target_id_concept = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID

        @classmethod
        def get_key_components(cls, record, get_target_id_from_record):
            assert record[CONCEPT.STUDY.TARGET_SERVICE_ID] is not None
            assert record[CONCEPT.PARTICIPANT.ID] is not None
            return {
                "study_link": record[CONCEPT.STUDY.TARGET_SERVICE_ID],
                "external_id": record[CONCEPT.PARTICIPANT.ID],
            }

        @classmethod
        def query_target_ids(cls, host, key_components):
            response = requests.get(url=f"{host}/participants", json=key_components)
            if response.status_code == 200:
                return [r["id"] for r in response.json()]

        @classmethod
        def build_entity(cls, record, get_target_id_from_record):
            secondary_components = {
                "id": get_target_id_from_record(cls, record),
                "family_link": get_target_id_from_record(Family, record),
                "sex": record.get(CONCEPT.PARTICIPANT.SEX),
                "race": record.get(CONCEPT.PARTICIPANT.RACE)
                "ethnicity": record.get(CONCEPT.PARTICIPANT.ETHNICITY)
            }
            return {
                **cls.get_key_components(record, get_target_id_from_record),
                **secondary_components,
            }

        @classmethod
        def submit(cls, host, body):
            response = requests.post(url=f"{host}/participants", json=body)
            if response.status_code in {200, 201}:
                return response.json()["id"]
            else:
                raise requests.RequestException(
                    f"Sent to {response.url}:\n{body}\nGot:\n{response.text}"
                )

Example Target Service Plugin
-----------------------------

In this example there's only one entry in the ``all_targets`` list because
we only defined a builder for participants, but you will probably have many.

.. code-block:: python

    from example_participant_builder import Participant

    LOADER_VERSION = 2

    all_targets = [
        Participant
    ]
