.. _Extract-Preprocessing:

Preprocessing Before the Mapping Operations List
================================================

Source data can't always be mapped right away, or perhaps your preferred
mapping operations require an initial pre-processing step, like e.g.
transposing the table to turn rows into columns.

Initial manipulations can be done in a ``do_after_read`` function that you may
optionally define in your extract configuration file like this:

.. code-block:: python

    # This function will be called before performing the mapping operations list.
    def do_after_read(df):
        # do something with df
        return new_df
