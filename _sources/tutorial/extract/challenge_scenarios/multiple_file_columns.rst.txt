==========================================
Multiple Columns of The Same Kind of Thing
==========================================

Scenario
========

You want to extract specimen IDs and file names from data that looks like this:

.. csv-table::
    :header: Specimen, BAM File, Index File

    S1, S1.bam, S1.index
    S2, S2.bam, S2.index

Note that there are two columns with file names in them.

Correct Output
==============

.. csv-table::
    :header: BIOSPECIMEN.ID, GENOMIC_FILE.FILE_NAME

    S1, S1.bam
    S2, S2.bam
    S1, S1.index
    S2, S2.index

Solution with Parallel Column Stacking
======================================

Use :ref:`Column-Stacking` to send both of the file columns to "File Name":

.. code-block:: python

    operations = [
        keep_map(in_col="Specimen", out_col=CONCEPTS.BIOSPECIMEN.ID),
        keep_map(in_col="BAM File", out_col=CONCEPTS.GENOMIC_FILE.FILE_NAME),
        keep_map(in_col="Index File", out_col=CONCEPTS.GENOMIC_FILE.FILE_NAME)
    ]

Solution with Multiple Extractions
==================================

Do separate extractions. One with:

.. code-block:: python

    operations = [
        keep_map(in_col="Specimen", out_col=CONCEPTS.BIOSPECIMEN.ID),
        keep_map(in_col="BAM File", out_col=CONCEPTS.GENOMIC_FILE.FILE_NAME),
    ]

And one with:

.. code-block:: python

    operations = [
        keep_map(in_col="Specimen", out_col=CONCEPTS.BIOSPECIMEN.ID),
        keep_map(in_col="Index File", out_col=CONCEPTS.GENOMIC_FILE.FILE_NAME),
    ]

And then stack them later in your Transform function.
