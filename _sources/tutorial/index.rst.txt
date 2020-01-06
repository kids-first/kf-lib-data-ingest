.. _how_to:

======
How To
======

The quickstart example uses a simple dataset and is primarily intended to show
a user how to run the ingest pipeline and view basic outputs. This tutorial
uses a more complex study dataset and steps through each ingest stage in much
more depth.

In this tutorial we will cover:

1. Basic usage and configuration of ingest app-level settings
2. How to configure complex extractions with multiple messy source data files
3. How to write a transform function for the transform stage
4. How to run the ingest pipeline from end to end
5. How to test your ingest package each step of the way
6. How to debug steps 1 through 3 by:

   - Running stages individually
   - Viewing stage outputs
   - Viewing ingest logs

.. toctree::
   :caption: Contents
   :maxdepth: 2

   app.rst
   files.rst
   ingest_package.rst
   package_setup.rst
   extract/index.rst
   extract/challenge_scenarios/index.rst
   study_creator.rst
   package_testing.rst
   debugging.rst
   transform.rst
   load.rst
