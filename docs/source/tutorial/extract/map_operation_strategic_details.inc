.. _Strategic-Mapping-Details:

Important Strategic Details
===========================

Avoid Hard-coded Strings
------------------------

Avoid using hard-coded strings for output column headers and values. As much as
possible, favor variables in ``common.constants`` for cell values and variables
in ``common.concept_schema.CONCEPT`` for ``out_col`` assignment. That way
everyone has the chance to use the same shared set of values.

Repeated Information
--------------------

Multiple identical declarations don't add new information and therefore don't
matter. They also don't hurt anything. This table means exactly the same thing
as if it had only one row:

.. csv-table::
    :header: Participant ID, Specimen ID, Participant Age, Participant Sex, Participant Race

    P1, S1, 7, m, unknown
    P1, S1, 7, m, unknown

.. _Normalized-Denormalized:

You Don't Need To Extract The Whole Document All At Once
--------------------------------------------------------

If creating multiple extraction configurations for a single source data file to
generate multiple smaller tables during Extract that can be manipulated in a
Transform function is easier for you than making one complex table for the
whole data file, then do it. If not, that's fine too.

.. _Column-Stacking:

Parallel Column Stacking
------------------------

If the same ``out_col`` is used multiple times, the column output from
successive operations will be vertically concatenated.

.. image:: /_static/images/columns.svg

Length Rectification
--------------------

The length disparity between output columns produced by `Parallel Column
Stacking`_ is then rectified by least-common-multiple repetition of output
columns which gives a final result that preserves row colinearity from the
original source data. Note in the next image that A is inline with A, B is
inline with B, etc.

.. image:: /_static/images/length_rectification.svg

.. caution::

    The lengths of all output columns **must** be multiples of the original
    source data length. If they aren't, the library will raise an error.

.. _Splitting-Cells:

Splitting Cells
---------------

Sometimes you will have source documents that lump multiple distinct values
into the same table cell. For instance, study participants might have multiple
specimens reported together as follows:

.. csv-table::
   :header: Participant, Specimens

    P1, SP1a/SP1b
    P2, SP2
    P3, SP3a/SP3b/SP3c

Because of the practical implications of the :ref:`Value-Principles`,
you will want to have independent associations (P1, SP1a), (P1, SP1b), etc, so
you need to separate the "/"-delimited specimen values into separate entries.
Replacing each of the Specimens cells with the appropriate ``Split`` object
will tell the Extract Stage to do the right thing.

The Split Object
^^^^^^^^^^^^^^^^

The ``Split`` object (``common.pandas_utils.Split``) is instantiated with a
list of values and optionally a group ID. After all of the extract mapping
operations are finished, any Split objects found in your output will trigger a
"split" of the row into multiple rows with identical non-Split cells and each
with a different value from among the Split members.

For instance, from:

.. csv-table::
    :header: A, B, C

    "Split([1, 2, 3])", "Split([4, 5, 6])", "7"

to:

.. csv-table::
    :header: A, B, C

    1, 4, 7
    1, 5, 7
    1, 6, 7
    2, 4, 7
    2, 5, 7
    2, 6, 7
    3, 4, 7
    3, 5, 7
    3, 6, 7

Without groups, multiple Splits on the same row will multiply to produce the
cartesian product of the values in those Splits. However, if Splits are
assigned a group value, then Splits on the same row in the same group will be
linked to each other so that they do not form a cartesian product with each
other.

.. csv-table::
    :header: A, B, C

    "Split([8, 9], group=1)", "Split([10, 11, 12], group=1)", "13"

to:

.. csv-table::
    :header: A, B, C

    8,    10, 13
    9,    11, 13
    None, 12, 13

And if you combine both of these forms together, you go from:

.. csv-table::
    :header: A, B, C

    "Split([1, 2], group=1)", "Split([3, 4, 5], group=1)", "Split([6, 7])"

to:

.. csv-table::
    :header: A, B, C

    1,    3, 6
    2,    4, 6
    None, 5, 6
    1,    3, 7
    2,    4, 7
    None, 5, 7

Going back to the participants and specimens example, you might use a ``Split``
object as part of a ``value_map`` operation like this:

.. code-block:: python

    value_map(
        in_col="Specimens",
        out_col="<example_output_col>",
        m=lambda x: Split(x.split("/"))
    )

.. note::

    Interpretation of ``Split`` objects happens after all the mapping
    operations are complete, so the newly created rows will not be affected by
    the length-multiple restriction cautioned in `Parallel Column Stacking`_.

.. _Nested-Operation-Sublists:

Nested Operations Sublists
--------------------------

You can nest groups of operations inside of sublists to do length rectification
on just the group before joining it with the rest of the operations output.

It looks like this:

.. code-block:: python

    operations = [
        [
            # operations group 1
        ],
        [
            # operations group 2
        ],
        # etc
    ]

The reason for doing this is that `Parallel Column Stacking`_ can lead to row
mismatch problems in scenarios where several columns form logical groups with
each other and the mapping operations don't all produce the same length output.

Consider the situation of extracting KFDRC phenotypes from:

.. csv-table::
    :header: "Participant", "Age", "Mass", "Cleft Ear", "Mass/Cleft Age", "Tennis Fingers", "Tennis Age"

    P1, 70, yes, yes, 60, no, 65
    P2, 80, no, yes, 80, no, 45

Note that there are two different groups of phenotype measurements recorded at
different ages.

A naïve and incorrect approach would be to do this:

.. code-block:: Python

    operations = [
        melt_map(
            var_name=CONCEPT.PHENOTYPE.NAME,
            map_for_vars={
                "Mass": "Mass",
                "Cleft Ear": "Cleft Ear"
            },
            value_name=CONCEPT.PHENOTYPE.OBSERVED,
            map_for_values={
                "yes": constants.PHENOTYPE.OBSERVED.POSITIVE,
                "no": constants.PHENOTYPE.OBSERVED.NEGATIVE
            }
        ),
        keep_map(
            in_col="Mass/Cleft Age",
            out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
        ),
        melt_map(
            var_name=CONCEPT.PHENOTYPE.NAME,
            map_for_vars={
                "Tennis Fingers": "Tennis Fingers"
            },
            value_name=CONCEPT.PHENOTYPE.OBSERVED,
            map_for_values={
                "yes": constants.PHENOTYPE.OBSERVED.POSITIVE,
                "no": constants.PHENOTYPE.OBSERVED.NEGATIVE
            }
        ),
        keep_map(
            in_col="Tennis Age",
            out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
        )
    ]

But this will cause the following **bad** result because the operations stack
output columns with different length outputs:

.. image:: /_static/images/bad_melt.svg

Then the `Length Rectification`_ will produce this mess:

.. image:: /_static/images/wrong.svg

If you need to group values together and also stack columns that belong to the
groups, all of the columns in each group must be the same length, otherwise
groups will invade each others' space. When melting multiple columns, this is
naturally not going to be the case.

Putting each of the groups in its own nested sublist solves this problem by
rectifying lengths for each of the groups independently first.

.. code-block:: Python

    operations = [
        [   # mass/cleft group
            melt_map(
                var_name=CONCEPT.PHENOTYPE.NAME,
                map_for_vars={
                    "Mass": "Mass",
                    "Cleft Ear": "Cleft Ear"
                },
                value_name=CONCEPT.PHENOTYPE.OBSERVED,
                map_for_values={
                    "yes": constants.PHENOTYPE.OBSERVED.POSITIVE,
                    "no": constants.PHENOTYPE.OBSERVED.NEGATIVE
                }
            ),
            keep_map(
                in_col="Mass/Cleft Age",
                out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
            ),
        ],
        [   # tennis fingers group
            melt_map(
                var_name=CONCEPT.PHENOTYPE.NAME,
                map_for_vars={
                    "Tennis Fingers": "Tennis Fingers"
                },
                value_name=CONCEPT.PHENOTYPE.OBSERVED,
                map_for_values={
                    "yes": constants.PHENOTYPE.OBSERVED.POSITIVE,
                    "no": constants.PHENOTYPE.OBSERVED.NEGATIVE
                }
            ),
            keep_map(
                in_col="Tennis Age",
                out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
            )
        ]
    ]

So now you get this instead:

.. image:: /_static/images/good_melt.svg
