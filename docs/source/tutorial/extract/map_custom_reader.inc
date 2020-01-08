.. _Extract-Custom-Reader:

Files That Need Custom Readers
==============================

Source data can't always be read automatically by the default tabular read
functions. We provide readers for CSV/TSV/Excel sheets, but maybe the data you
need is embedded in a PDF or PowerPoint file. 

If the right reader cannot be automatically inferred, you can define a custom
function called ``source_data_read_func`` that takes a local file path and
keyword arguments (also given in ``source_data_read_params``) and returns a
pandas DataFrame:

.. code-block:: python

    # a special custom file reader which must return a pandas DataFrame
    def source_data_read_func(filepath, **kwargs):
        return my_df
