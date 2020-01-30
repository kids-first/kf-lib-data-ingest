===========================
Testing Your Ingest Package
===========================

Before we go any further in the tutorial, let's take a look at the recommended
way to test your ingest package.

At this point, your ingest package should have the source data file and extract
config that came with the package template along with the extract configuration
from :ref:`Extract-Example`.

Your package probably looks like this::

    my_study/
    ├── data
    │   └── clinical.tsv
    ├── extract_configs
    │   ├── extract_config.py
    │   └── family_and_phenotype.py
    ├── tests
    │   ├── conftest.py
    │   └── test_custom_counts.py
    ├── ingest_package_config.py
    └── transform_module.py

Testing Your Package
========================

Let's see what happens if you run:

.. code-block:: text

    $ kidsfirst test my_study

Near the end of the log, you should see something like this:

.. code-block:: text

    2019-05-01 13:08:40,563 - DataIngestPipeline - INFO - ❌ Count Analysis Failed!
    See /path/to/my_study/logs/counts_for_ingest.log for details

Inside of that ``counts_for_ingest.log`` file, you should some things that look
like::

    EXPECTED COUNT CHECKS
    +----------------+------------+---------+---------+
    | key            |   expected |   found | equal   |
    |----------------+------------+---------+---------|
    | FAMILY|ID      |          2 |       2 | ✅      |
    | PARTICIPANT|ID |          5 |       9 | ❌      |
    | BIOSPECIMEN|ID |         10 |      16 | ❌      |
    +----------------+------------+---------+---------+

It looks like some tests failed. Let's break down what is going on, and then we
can make the necessary changes so that our tests pass.

The ``test`` CLI Command
========================

Under the hood, the ``test`` command is really just an alias for:

.. code-block:: shell

    $ kidsfirst ingest <your package> --dry_run

The ``--dry_run`` flag says to ingest your package without actually sending
anything to the target service. This will help you determine whether your
ingest package runs correctly before you try to load your study into the target
service.

Data Validation Tests
=====================

Whenever you run the ``ingest`` command (regardless of whether you use
``--dry_run`` or not) the data validation tests will always execute, allowing
you to quickly determine whether or not the output of each stage matches a set
of custom accounting expectations specified in your ingest package.

If any of the tests fail, the ingest app will wait until it executes all of the
tests and then exit with a non-zero exit code. The log will indicate what
caused the failure(s).

Standard Counting Tests
-----------------------

Each stage has a post-run analysis method which performs some basic accounting
on the stage output and then validates count results against a set of expected
counts.

Count Analysis
^^^^^^^^^^^^^^

The types of things that are counted are:

- Concepts (families, participants, biospecimens, etc)
- Concept attributes (participant.gender, biospecimen.analyte, etc)
- Relationships between concepts or attributes (biospecimens with 1
  participant, participants with at least 1 biospecimen, etc)

It is important to know that some of these things (such as relationships)
cannot be counted reliably until after the transform stage completes, since
that is where all of the data is merged together.

Concept Discovery
~~~~~~~~~~~~~~~~~

After an ingest stage is run, the post-run analysis iterates over the stage's
output and builds a ``concept_discovery`` dict, which stores the following:

- A mapping from every concept attribute found to a list of all of the
  source data files that the concept attribute was found in
- A mapping between every pair of concept attribute values found

The concept discovery data is used to compute the counts of concepts,
attributes, and relationships.

Every stage's concept discovery data structure is written to a file named
``<stage name>_concept_discovery.json`` in the stage's output directory. You
will see how this can be used to write custom data validation tests in the
:ref:`user-defined-tests` section.

Set Expected Counts
===================

Now let's go back and take a look at the count results we saw in the log. It
looks like our tests are failing because in almost every case the count
analysis is finding more concepts in the source data than were expected.

This is probably because we've added a new source data file
``family_and_phenotype.tsv``. We can test this theory by removing the extract
config for ``family_and_phenotype.tsv`` and seeing if the tests pass.

Try moving the ``extract_configs/family_and_phenotype.py`` file out of the
extract configs folder and re-running the test command. The log should show
that ingest passed validation::

    2019-04-16 10:14:58,519 - kf_lib_data_ingest.app.cli - INFO - ✅  Ingest pipeline passed validation!

Ok, now put the extract config back into its directory and let's update the
expected counts for our ingest package.

The expected counts for an ingest package are set in the
``ingest_package_config.py`` file. Update the counts to the following:

.. code-block:: py

    expected_counts = {
        CONCEPT.FAMILY: 2,
        CONCEPT.PARTICIPANT: 9,
        CONCEPT.BIOSPECIMEN: 16
    }

Now re-run the test command. You should see your tests passing in the
``counts_for_ingest.log`` file::

    EXPECTED COUNT CHECKS
    +----------------+------------+---------+---------+
    | key            |   expected |   found | equal   |
    |----------------+------------+---------+---------|
    | FAMILY|ID      |          2 |       2 | ✅      |
    | PARTICIPANT|ID |          9 |       9 | ✅      |
    | BIOSPECIMEN|ID |         16 |      16 | ✅      |
    +----------------+------------+---------+---------+

.. _user-defined-tests:

User Defined Tests
------------------

If there is some sort of more complex data validation that is not covered by
the expected_counts table, an ingest developer may write custom tests to
implement their own data validation.

These tests must be placed inside of a ``tests`` directory in the ingest
package. The popular `pytest
<https://docs.pytest.org/en/latest/contents.html>`_ testing framework is used
to execute the user defined tests, so all tests should conform to the
``pytest`` standard.

You can see an example of a user defined test in your ingest package. This test
validates that there are exactly 2 duo-type families and 1 trio-type family.

conftest.py
^^^^^^^^^^^

Every ingest package created using the ``kidsfirst new`` command comes with
a pytest ``conftest.py`` module which includes a method to load a stage's
concept discovery data.

As you can see, rather than reading in the extract stage output and
re-implementing the counting logic, we can simply use the concept discovery
data from the extract stage to count the duos and trios fairly easily.

Best Practices
==============

Use the ``kidsfirst test`` command to test early and often so that there are no
surprises when you ingest into your target service.

Ok, that's it for testing. Let's head to the next section!
