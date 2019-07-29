======================================================
Data That Only Makes Sense In Relation To Another File
======================================================

Scenario
========

The ``source_data_url`` gives you access to one data file, but you need to
access one or more other files to make sense of your source data.

File Fetching Solution
======================

You can access the file fetching methods yourelf to load the data from as many
secondary files as you want. For instance, for a secondary TSV file, you can
do:

.. code-block:: Python

    from kf_lib_data_ingest.common.file_retriever import FileRetriever

    f = FileRetriever().get("<URL for your TSV file>")
    file_df = pandas.read_csv(
        f.name, dtype=str, engine='python', na_filter=False, sep='\t'
    )

Keep in mind that ``in_col`` arguments in the :ref:`operations list
<Extract-Mapping>` can only read from the primary source data file referenced
by :ref:`source_data_url<Fetching-the-Data>`.
