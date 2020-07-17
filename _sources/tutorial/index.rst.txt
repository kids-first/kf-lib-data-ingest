.. _how_to:

######
How To
######

The quickstart example uses a simple dataset and is primarily intended to show
a user how to run the ingest pipeline and view basic outputs. This tutorial
uses a more complex study dataset and steps through each ingest stage in much
more depth.

In this tutorial we will cover (not necessarily in this order):

1. How to configure complex extractions with multiple messy source data files
2. How to write a transform function for the transform stage
3. How to run the ingest pipeline from end to end
4. How to test your ingest package each step of the way
5. How to debug steps 1 through 3 by:

   - Running stages individually
   - Viewing stage outputs
   - Viewing ingest logs

6. Basic usage and configuration of ingest app-level settings

.. toctree::
   :caption: Contents
   :maxdepth: 2

   app.rst
   ingest_package.rst
   package_setup.rst
   debugging.rst
   extract/index.rst
   extract/challenge_scenarios/index.rst
   study_creator.rst
   package_testing.rst
   transform.rst
   target_service_plugins/index.rst
   load.rst
