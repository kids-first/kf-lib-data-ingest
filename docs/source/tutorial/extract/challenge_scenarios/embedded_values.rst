===============================
Values Embedded in Other Values
===============================

Scenario
========

You have a column of file names that secretly also contain other values like
specimen ID and sequencing library.

.. csv-table::
    :header: "File Name"

    BSID1234_SL1234.bam
    BSID1235_SL1235.bam
    BSID1236_SL1236.bam
    BSID1237_SL1237.bam

Correct Output
==============

.. csv-table::
    :header: BIOSPECIMEN.ID, SEQUENCING.LIBRARY_NAME, GENOMIC_FILE.FILE_NAME

    BSID1234, SL1234, BSID1234_SL1234.bam
    BSID1235, SL1235, BSID1235_SL1235.bam
    BSID1236, SL1236, BSID1236_SL1236.bam
    BSID1237, SL1237, BSID1237_SL1237.bam

Solution with Regular Expressions
=================================

.. code-block:: python

    operations = [
        keep_map(
            in_col="File Name",
            out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
        ),
        value_map(
            in_col="File Name",
            out_col=CONCEPT.BIOSPECIMEN.ID,
            m=r'^([^_]+).+$'
        ),
        value_map(
            in_col="File Name",
            out_col=CONCEPT.SEQUENCING.LIBRARY_NAME,
            m=r'^[^_]+_(.+)$'
        )
    ]

Solution with Functions
=======================

.. code-block:: python

    operations = [
        keep_map(
            in_col="File Name",
            out_col=CONCEPT.GENOMIC_FILE.FILE_NAME
        ),
        value_map(
            in_col="File Name",
            out_col=CONCEPT.BIOSPECIMEN.ID,
            m=lambda x: x.split('_')[0]
        ),
        value_map(
            in_col="File Name",
            out_col=CONCEPT.SEQUENCING.LIBRARY_NAME,
            m=lambda x: x.split('_')[1].split('.')[0]
        )
    ]
