==========
Load Stage
==========

The Load stage feeds record data through a plugin interface that must be
implemented for each target service.

Target service plugins implement the necessary routines for converting record
data coming in from the previous stage into message payloads and also
implement the logic for sending those payloads to the target service.

.. image:: /_static/images/load_stage.svg
  :alt: Diagram showing how the load stage hooks into target service plugins

