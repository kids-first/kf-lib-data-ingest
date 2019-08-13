========================
Multiple Meltable Groups
========================

This can get tricky. There are many ways to deal with this, each with their
own pros and cons.

Scenario
========

You want to create KFDRC dataservice Participant and Phenotype entities from
data that looks like this:

.. csv-table::
    :header: "Participant", "Age", "Mass", "Cleft Ear", "Mass/Cleft Age", "Tennis Fingers", "Tennis Age"

    P1, 70, yes, yes, 60, no, 65
    P2, 80, no, yes, 80, no, 45

Note that there two different groups of phenotype measurements recorded at
different ages.

Correct Output
==============

.. csv-table::
    :header: "Participant", "Age", "Phenotype Name", "Phenotype Observed", "Phenotype Age"

    P1, 70, Mass, yes, 60
    P2, 80, Mass, no, 80
    P1, 70, "Cleft Ear", yes, 60
    P2, 80, "Cleft Ear", yes, 80
    P1, 70, "Tennis Fingers", no, 65
    P2, 80, "Tennis Fingers", no, 45

Non-Melting Solution
====================

You can avoid melting and use basic :ref:`Column-Stacking` with a triplet of
operations for each phenotype. Just be careful about operation order.


.. code-block:: Python

    operations = [
        # participants
        keep_map(
            in_col="Participant",
            out_col=CONCEPTS.PARTICIPANT.ID
        ),
        keep_map(
            in_col="Age",
            out_col=CONCEPTS.PARTICIPANT.AGE
        ),

        # massless phenotypes
        keep_map(
            in_col="Mass/Cleft Age",
            out_col=CONCEPTS.PHENOTYPE.AGE_AT_OBSERVATION
        ),
        constant_map(
            m="Mass",
            out_col=CONCEPTS.PHENOTYPE.NAME
        ),
        value_map(
            in_col="Mass",
            out_col=CONCEPTS.PHENOTYPE.OBSERVED,
            m={
               "yes": constants.PHENOTYPE.OBSERVED.POSITIVE,
               "no": constants.PHENOTYPE.OBSERVED.NEGATIVE
            }
        )

        # cleft elbow phenotypes
        keep_map(
            in_col="Mass/Cleft Age",
            out_col=CONCEPTS.PHENOTYPE.AGE_AT_OBSERVATION
        ),
        constant_map(
            m="Cleft Ear",
            out_col=CONCEPTS.PHENOTYPE.NAME
        ),
        value_map(
            in_col="Cleft Ear",
            out_col=CONCEPTS.PHENOTYPE.OBSERVED,
            m={
               "yes": constants.PHENOTYPE.OBSERVED.POSITIVE,
               "no": constants.PHENOTYPE.OBSERVED.NEGATIVE
            }
        )

        # tennis finger phenotypes
        keep_map(
            in_col="Tennis Age",
            out_col=CONCEPTS.PHENOTYPE.AGE_AT_OBSERVATION
        ),
        constant_map(
            m="Tennis Fingers",
            out_col=CONCEPTS.PHENOTYPE.NAME
        ),
        value_map(
            in_col="Tennis Fingers",
            out_col=CONCEPTS.PHENOTYPE.OBSERVED,
            m={
               "yes": constants.PHENOTYPE.OBSERVED.POSITIVE,
               "no": constants.PHENOTYPE.OBSERVED.NEGATIVE
            }
        )
    ]

As long as you consistently put the grouped operations together, the result
should be correct. If, however, you were to swap the two value_map operations
with each other, then you would be associating the wrong observation to each
phenotype.

Melting Solution
================

Use melt, and wrap the melt groups in :ref:`Nested-Operation-Sublists` to
safely navigate the column-length consequences of having multiple columns
melted together and then grouped with another shorter column which is then
lengthened by :ref:`Column-Stacking` from another melt group.

.. code-block:: Python

    operations = [
        # participants
        keep_map(
            in_col="Participant",
            out_col=CONCEPTS.PARTICIPANT.ID
        ),
        keep_map(
            in_col="Age",
            out_col=CONCEPTS.PARTICIPANT.AGE
        ),

        # mass/cleft phenotypes group
        [
            keep_map(
                in_col="Mass/Cleft Age",
                out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
            ),
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
            )
        ],

        # tennis fingers phenotype group
        [
            keep_map(
                in_col="Tennis Age",
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
            )
        ],

.. caution::

    Without nested sublists clustering the operations into groups, the result
    would be wrong.

Melting Solution With Multiple Smaller Extracts
===============================================

Nothing says that you need to extract the whole file all at once. You can also
choose to virtually divide the source data into simple chunks, which is another
way of dealing with the complex length stacking problem:

.. csv-table::
    :header: "Participant", "Age"

    P1, 70
    P2, 80

.. code-block:: Python

    operations = [
        keep_map(
            in_col="Participant",
            out_col=CONCEPTS.PARTICIPANT.ID
        ),
        keep_map(
            in_col="Age",
            out_col=CONCEPTS.PARTICIPANT.AGE
        )
    ]

and

.. csv-table::
    :header: "Participant", "Mass/Cleft Age", "Mass", "Cleft Ear"

    P1, 60, yes, yes
    P2, 80, no, yes

.. code-block:: Python

    operations = [
        keep_map(
            in_col="Participant",
            out_col=CONCEPTS.PARTICIPANT.ID
        ),
        keep_map(
            in_col="Mass/Cleft Age",
            out_col=CONCEPT.PHENOTYPE.EVENT_AGE_DAYS
        ),
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
        )
    ]

and

.. csv-table::
    :header: "Participant", "Tennis Age", "Tennis Fingers"

    P1, 65, no
    P2, 45, no

.. code-block:: Python

    operations = [
        keep_map(
            in_col="Participant",
            out_col=CONCEPTS.PARTICIPANT.ID
        ),
        keep_map(
            in_col="Tennis Age",
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
        )
    ]
