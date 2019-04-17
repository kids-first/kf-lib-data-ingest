.. _Tutorial-Transform-Stage:

===============
Transform Stage
===============

The transform stage does 4 major things:

1. Merge extract stage tables together
2. Create unique identifiers for every concept instance
3. Convert from standard form to target form
4. Apply common transformations

From an ingest developer perspective, step 1 is the only one that you need to
worry about.

Guided Transform Module
=======================

This is a Python module in your ingest package which must include a method
called ``transform_function``. This method will contain code that merges the
extract stage's output Pandas DataFrames into a single DataFrame and then
returns the merged DataFrame.

If you used ``kidsfirst new`` to create your ingest package, you should already
have a `transform_module.py` file with the correct method signature, sample
code, and return type for ``transform_function``.

Let's take a look:

.. literalinclude:: ../../../kf_lib_data_ingest/factory/templates/my_study/transform_module.py
   :language: python
   :caption: my_study/transform_module.py

The ``transform_function`` method has only one argument, the ``mapped_df_dict``
which is a dict of extract config file paths and the corresponding Pandas
DataFrames produced by the extract configs.

As you can see, right now the transform module is very simple. It doesn't
really merge anything. It simply returns one of the extract stage DataFrames.

Transform Function
==================

Let's modify the transform function so that it merges our two extracted
DataFrames (corresponding to `data/clinical.tsv` and
`data/family_and_phenotype.tsv`).

We'll explain the pieces, but this is the end result:

.. code-block:: python

    """
    Auto-generated transform module

    Replace the contents of transform_function with your own code

    See documentation at
    https://kids-first.github.io/kf-lib-data-ingest/ for information on
    implementing transform_function.
    """
    import os

    # Use these merge funcs, not pandas.merge
    from kf_lib_data_ingest.common.pandas_utils import (
        outer_merge,
        merge_wo_duplicates
    )
    from kf_lib_data_ingest.common.concept_schema import CONCEPT


    def transform_function(mapped_df_dict):
        dfs = {
            os.path.basename(fp): df
            for fp, df in
            mapped_df_dict.items()
        }
        clinical_df = dfs['extract_config.py']
        family_and_phenotype_df = dfs['family_and_phenotype.py']

        merged = outer_merge(clinical_df,
                             family_and_phenotype_df,
                             on=CONCEPT.PARTICIPANT.ID,
                             with_merge_detail_dfs=False)

        return merged

First we just reshape the input data so that it's easier to work with:

.. code-block:: python

    dfs = {
        os.path.basename(fp): df
        for fp, df in
        mapped_df_dict.items()
    }
    clinical_df = dfs['extract_config.py']
    family_and_phenotype_df = dfs['family_and_phenotype.py']

Next, we merge the two DataFrames together on the participant ID column.

.. code-block:: python

    merged = outer_merge(clinical_df,
                         family_and_phenotype_df,
                         on=CONCEPT.PARTICIPANT.ID,
                         with_merge_detail_dfs=False)

There are a few important things to note in this step:

1. We outer merged the DataFrames
2. We used CONCEPT.PARTICIPANT.ID, not the string "CONCEPT|PARTICIPANT|ID"
   itself
3. We did **not** use the ``Pandas.merge`` method to merge the DataFrames

Outer Merge
^^^^^^^^^^^

You will likely NEVER want to inner merge/join your DataFrames since this will
result in a DataFrame with records that only matched in both DataFrames. This
may cause you to lose records.

Use concept schema to reference columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The value of ``CONCEPT.PARTICIPANT.ID`` equates to, "CONCEPT|PARTICIPANT|ID",
a string representing the participant concept's identifier.

You should always use the ``CONCEPT`` class from concept schema and not strings
to reference join columns. This way, if the value of the concept attribute
changes (to say, "PARTICIPANT.IDENTIFIER"), your code won't silently break.

Avoid Pandas.merge - use ingest library's pandas_utils.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You `can` use the Pandas.merge method if you want, but the ingest library
provides merge functions that add useful functionality on top of Pandas.merge.

For example, ``outer_merge`` has a keyword argument called
`with_merge_detail_dfs`, that will output 3 additional DataFrames useful for
debugging:

- a DataFrame of rows that matched in both the left and right
  DataFrames (equivalent to the DataFrame returned by an inner merge)
- a DataFrame of rows that were ONLY in the left DataFrame
- a DataFrame of rows that were ONLY in the right DataFrame

If you need to do a non-outer merge, you should use the ``merge_wo_duplicates``
method. This method automatically fills some of the holes that will naturally
result from doing multiple sequential merges on partially-overlapping data.

See ``kf_lib_data_ingest.common.pandas_utils`` for details.

Test Your Package

=================

If you run your ingest package now with ``kidsfirst test`` and everything goes
well, your log should indicate that the transform function was applied and
that the GuidedTransformStage began and ended.
