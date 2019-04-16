========================
Test Your Ingest Package
========================

Before we go any further in the tutorial, let's take a look at the recommended
way to test your ingest package.

At this point, your ingest package should have the source data file and
extract config that came with the package template, along with the
``family_and_phenotype.tsv`` data file and extract configuration file
that you added in the previous step of this tutorial.

Your package probably looks like this so far::

    my_study/
    ├── data
    │   ├── clinical.tsv
    │   └── family_and_phenotype.tsv
    ├── dataset_ingest_config.yml
    ├── extract_configs
    │   ├── extract_config.py
    │   └── family_and_phenotype.py
    ├── tests
    │   ├── conftest.py
    │   └── test_custom_counts.py
    └── transform_module.py


Try Testing Your Package
========================
Let's see what happens if you run:

.. code-block:: bash

    $ kidsfirst test my_study

At the end of the log you should see something like this::

    2019-04-16 10:08:00,101 - kf_lib_data_ingest.app.cli - ERROR - ❌  Ingest pipeline failed validation! See /path/to/my_study/logs/ingest.log for details

And if you scroll up through the log a bit, you should also see::

    EXPECTED COUNT CHECKS
    +------------------------+------------+---------+---------+
    | key                    |   expected |   found | equal   |
    |------------------------+------------+---------+---------|
    | CONCEPT|FAMILY|ID      |          2 |       2 | ✅      |
    | CONCEPT|PARTICIPANT|ID |          5 |       9 | ❌      |
    | CONCEPT|BIOSPECIMEN|ID |         10 |      16 | ❌      |
    +------------------------+------------+---------+---------+

It looks like the tests failed. Let's break down what is going on and then
we can update our tests so that they pass.

The ``test`` CLI Command
========================
Under the hood, the ``test`` command is really just an alias for:

.. code-block:: shell

    $ kidsfirst ingest <your package> --dry_run


The ``test`` command will run ingest on your package without actually loading
anything into the target service. This will help you quickly determine
whether your ingest package runs without any exceptions.

Data Validation Tests
=====================
Additionally, whenever you run the ``ingest`` command (regardless of whether
you ran with ``--dry_run`` or not) the data validation tests
will always execute, allowing you to quickly determine whether or not the
output of each stage matches a set of accounting expectations you specify in
your ingest package.

If any one of the test fails, the ingest app will wait until it executes
all of the tests and then exit with a non-zero exit code.
The log will indicate what caused the test failure(s).

Standard Counting Tests
-----------------------------------
Each stage has a post-run analysis method which performs some basic
accounting on the stage output and then validates the count results against
a set of expected counts.

Count Analysis
^^^^^^^^^^^^^^
The types of things that are counted are:

- Concepts (i.e. families, participants, biospecimens, etc)
- Concept attributes (i.e. participant.gender, biospecimen.analyte, etc.)
- Relationships between concepts or attributes (i.e. biospecimens with 1
  participant, participants with at least 1 biospecimen, etc.)

It is important to know that some of these things, such as relationships,
cannot reliably be counted until after the transform stage completes, since
this is where the all of the data is merged into a single table.

Concept Discovery
~~~~~~~~~~~~~~~~~
After an ingest stage is run, the post-run analysis iterates over the stage
output files and builds up the ``concept_discovery`` dict, which stores
the following:

    - A mapping of every concept it found in all of the stage output files
      to a list of all of the source data files that the concept was found in

    - A mapping of every concept attribute it found in all of the stage
      output files to a list of all of the source data files that the
      concept attribute was found in

    - A mapping of every pair of concept attributes it found in all of the
      stage output files to a list of all of the source data files that the
      concept attribute pair was found in

The concept discovery data is used to compute the counts of
concepts, attributes, and relationships.

Every stage's concept discovery data structure is written to a file
in the stage's output directory, and named
``<stage name>_concept_discovery.json``. You will see how this can be used
to write custom data validation tests in the User Defined Tests section.


Set Expected Counts
===================
Ok, now let's go back and take a look at the count results we saw in the log.
It looks like our tests are failing because in almost every case the
count analysis is finding more concepts in the source data as compared to the
expectations.

This is probably because we've added a new source data file
``family_and_phenotype.tsv`` where more concepts were found. We can test this
theory out by removing the extract config for ``family_and_phenotype.tsv``
and see if the tests pass.

Try moving the ``extract_configs/family_and_phenotype.py`` file out of
the extract configs folder and re-running the test command. The log should show
that ingest passed validation::

    2019-04-16 10:14:58,519 - kf_lib_data_ingest.app.cli - INFO - ✅  Ingest pipeline passed validation!

Ok, now put the extract config back into its directory and let's update the
expected counts for our ingest package.

The expected counts for an ingest package are set in the
``dataset_ingest_config.yaml`` file. Update the counts to the following:

.. code-block:: yaml

    expected_counts:
        'CONCEPT|FAMILY': 2
        'CONCEPT|PARTICIPANT': 9
        'CONCEPT|BIOSPECIMEN': 16

Now re-run the test command. You should see your tests passing::

    EXPECTED COUNT CHECKS
    +------------------------+------------+---------+---------+
    | key                    |   expected |   found | equal   |
    |------------------------+------------+---------+---------|
    | CONCEPT|FAMILY|ID      |          2 |       2 | ✅      |
    | CONCEPT|PARTICIPANT|ID |          5 |       5 | ✅      |
    | CONCEPT|BIOSPECIMEN|ID |         10 |      10 | ✅      |
    +------------------------+------------+---------+---------+

User Defined Tests
------------------
If there is some sort of more complex data validation that is not covered
by the tests above, an ingest developer may write custom tests to
implement their own data validation.

These tests must be placed inside of a ``tests`` directory in the ingest
package. The popular `pytest <https://docs.pytest.org/en/latest/contents.html>`_
testing framework is used to execute the user defined tests so all tests should
conform to the ``pytest`` standard.

You can see an example of a user defined test in your ingest package.
This test validates that there are exactly 2 duo type families and 1 trio
type family.

conftest.py
^^^^^^^^^^^
Every ingest package created using the ``kidsfirst new`` command comes with
a pytest ``conftest.py`` module which includes a method to load a stage's
concept discovery data so you don't have to.

As you can see, rather than reading in the extract stage output and
re-implementing the counting logic, we can simply use the concept discovery
data from the extract stage to count the duos and trios fairly easily.


Best Practices
==============
Use the ``kidsfirst test`` command to test early and often so that there are
no surprises when you ingest into your target service.

Ok, that's it for testing. Let's head to the next section!
