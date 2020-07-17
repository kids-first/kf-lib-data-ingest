.. _Tutorial-Transform-Stage:

===============
Transform Stage
===============

The transform stage does one major thing:

1. Merge extracted tables together so that the data needed for generating
   complete target entities from the extracted data is properly connected.

Guided Transform Module
=======================

This is a Python module in your ingest package which must include a method
called ``transform_function``. This method will merge the extract stage's
output Pandas DataFrames into one or more composite DataFrame(s) and then
returns the result.

If you used ``kidsfirst new`` to create your ingest package, you should already
have a `transform_module.py` file with the correct method signature, sample
code, and return type for ``transform_function``.

Let's take a look:

.. literalinclude:: ../../../kf_lib_data_ingest/templates/my_study/transform_module.py
   :language: python
   :caption: my_study/transform_module.py

The ``transform_function`` method has only one argument, the ``mapped_df_dict``
which is a dict of extract config file paths and the corresponding Pandas
DataFrames produced by the extract configs.

In the :ref:`Extract-Example` section of this guide, we explored an extract
configuration for a file called `family_and_phenotype.tsv`. Note that the
function shown above has a commented-out section of code that would, if we
chose to name that new configuration `family_and_phenotype.py`, merge the two
extracted outputs together by joining them on their respective participant ID
columns.

Modify your transform_function so that it merges your extracted DataFrames
appropriately according to the function docstring shown above.

There are a few important things to note in the commented-out example code:

1. We outer merge DataFrames so that we don't lose data
2. We use CONCEPT.PARTICIPANT.ID, not the string "PARTICIPANT|ID"
   itself
3. We do **not** use the ``Pandas.merge`` method to merge the DataFrames

Outer Merge
^^^^^^^^^^^

You will likely NEVER want to inner merge/join your DataFrames since this will
result in a DataFrame with records that only match in both DataFrames. This may
cause you to lose records.

Use concept schema to reference columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The value of ``CONCEPT.PARTICIPANT.ID`` equates to "PARTICIPANT|ID", a string
representing the participant concept's identifier.

You should always use the ``CONCEPT`` class from concept schema and not strings
to reference join columns. This way, if the value of the concept attribute
changes (to say, "PARTICIPANT.IDENTIFIER"), your code won't break silently.

Avoid Pandas.merge - use ingest library's pandas_utils.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You `may` use the Pandas.merge method if you want, but the ingest library
provides merge functions that add useful functionality on top of Pandas.merge.

``outer_merge`` automatically fills some of the data holes that will naturally
result from doing multiple sequential merges on partially-overlapping data.

It also has a keyword argument called `with_merge_detail_dfs` that will output
3 additional DataFrames useful for debugging:

1. a DataFrame of rows that matched in both the left and right
   DataFrames (equivalent to the DataFrame returned by an inner merge)
2. a DataFrame of rows that were ONLY in the left DataFrame
3. a DataFrame of rows that were ONLY in the right DataFrame

If you need to do a non-outer merge, you should use the ``merge_wo_duplicates``
method, which is what provides ``outer_merge``'s automatic hole-filling
behavior.

See ``kf_lib_data_ingest.common.pandas_utils`` for details.

Optional - Return more than 1 DataFrame
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes it isn't possible to cleanly merge all of your extracted data into
one monolithic DataFrame. In this case, you might merge subsets of your data
into less-than-everything DataFrames which you only use to build instances of
particular target concepts.

You can do this by setting a key in the transform function's output dict
specifically named after a particular target concept, with its value set to the
DataFrame containing the data to build instances of that target concept.

.. note::

    To further understand this, read: :ref:`Tutorial-Load-Stage-Expects`

For example:

.. code-block:: python

    from kf_lib_data_ingest.common.concept_schema import CONCEPT
    # Use these merge funcs, not pandas.merge
    from kf_lib_data_ingest.common.pandas_utils import (
        merge_wo_duplicates,
        outer_merge
    )
    from kf_lib_data_ingest.config import DEFAULT_KEY


    def transform_function(mapped_df_dict):
        clinical_df = mapped_df_dict['extract_config.py']
        family_and_phenotype_df = mapped_df_dict['family_and_phenotype.py']
        merged = outer_merge(
            clinical_df,
            family_and_phenotype_df,
            on=CONCEPT.PARTICIPANT.ID,
            with_merge_detail_dfs=False
        )

        family_relationship_df = mapped_df_dict['pedigree_to_fam_rels.py']

        return {
            'family_relationship': family_relationship_df,
            DEFAULT_KEY: merged
        }

This transform stage output signals to use the ``family_relationship_df``
DataFrame to build instances of ``family_relationship`` and the ``merged``
DataFrame to build instances of all the other target concepts (e.g.
participant, biospecimen, genomic_file)

Test Your Package
=================

If you run your ingest package now with ``kidsfirst test`` and everything goes
well, your log should indicate that the transform function was applied and
that the GuidedTransformStage began and ended.
