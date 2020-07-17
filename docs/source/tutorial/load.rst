.. _Tutorial-Load-Stage:

============================
Understanding the Load Stage
============================

.. _Tutorial-Load-Stage-Expects:

What the Load Stage Expects
===========================

The Load stage expects to receive a dictionary of pandas DataFrames keyed by
hints about which data can be found in the tables.

Example input:

.. code-block:: python

    {
        "default": <pandas DataFrame 1>,
        "phenotypes": <pandas DataFrame 2>,
        "family_relationships": <pandas DataFrame 3>,
        ...
    }

The keys must either be the value of a ``class_name`` attribute from one of
your target service plugin's included entity builders or ``"default"`` (where the
``"default"`` entry will be used for any entity builder that isn't explicitly named
in the dict), and each DataFrame must contain all of the necessary data used by
its respective target service entity builder.

In the above example, if building ``"phenotypes"`` requires knowing who is
being described, what characteristic they have, and when the characteristic was
observed, the attached DataFrame must therefore include those values.

Using The Load Stage On Its Own
===============================

If your data is already appropriately standardized, you don't need to use the
Extract and Transform stages in order to use the Load Stage. The Load stage can
be invoked on its own in Python like so:

.. code-block:: python

    from kf_lib_data_ingest.common.io import read_df
    from kf_lib_data_ingest.etl.load.load import LoadStage

    path_to_my_target_service_plugin = "foo/bar/my_plugin.py"
    target_service_base_url = "https://my_service:8080"
    list_of_class_names_to_load = ["patient", "family_relationship", "specimen"]
    study_id = "SD_ME0WME0W"
    path_to_cache_storage_directory = "foo/bar/my_output"

    LoadStage(
        path_to_my_target_service_plugin,
        target_service_base_url,
        list_of_class_names_to_load,
        study_id,
        path_to_cache_storage_directory
    ).run(
        {
            "default": read_df("my_input/default.tsv"),
            "family_relationship": read_df("my_input/family_relationships.tsv")
        }
    )

As mentioned, you may choose to do this if all of your incoming data is
guaranteed to be in a strict standardized format that requires no special
slicing, chopping, or blending.
