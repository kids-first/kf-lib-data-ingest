.. _Tutorial-Study-Creator:

=================================================
Source Data stored by the Kids First Data Tracker
=================================================

If you are developing an ingest package for Kids First study data, then the
source data files for the study will be managed by the
`Kids First Data Tracker <https://kf-ui-data-tracker.kidsfirstdrc.org>`_.

The :ref:`Tutorial-Extract-Stage` tutorial uses an unprotected file for example
purposes, but, in a more realistic scenario, the source data files for a study
would have been uploaded to the Data Tracker, and your ingest package would
need to be configured to access these files with the proper authorization
headers.

.. note::

    Uploading files to the Data Tracker is outside of the scope of this
    tutorial. Read the `Study Creator API docs
    <https://kids-first.github.io/kf-api-study-creator/>`_ or visit the `Data
    Tracker web app <https://kf-ui-data-tracker.kidsfirstdrc.org>`_ to learn
    more about how to do that.

Get Your Developer Token
========================

All source data files in the Data Tracker are protected by some sort of
authentication depending on the type of user trying to access them. As an
ingest package developer, you will need a developer token to access files. The
developer token grants authorization to download any source data file.

To generate a token, go to https://kf-ui-data-tracker.kidsfirstdrc.org/tokens

Set Your Environment
====================

Now that you have your token, you're going to store it in your shell
environment so that the ingest app can read it and use it when fetching files
through the Study Creator API.

In the configuration for your local shell environment, add the following:

.. code-block:: bash

   export KF_STUDY_CREATOR_API_TOKEN=<YOUR_TOKEN>

Be careful with this token and make sure to keep it secret.

Update Your Extract Config to get the file from the Data Tracker
================================================================

The ``clinical.tsv`` and ``family_and_phenotype.tsv`` source data files have
already been uploaded to the ``SD_ME0WME0W`` study via the Data Tracker
service.

For example, the Data Tracker URL for the ``family_and_phenotypes.tsv``
file is:

https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_HH5PMCJX/version/FV_5H7MEHG2

Replace the value of ``source_data_url`` parameter in ``extract_config.py``,
with the Data Tracker URL above. Note that you will not be able to manually
download from that URL without the correct authorization headers.

.. code-block:: python

    # family_and_phenotype.tsv
    source_data_url = 'https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_HH5PMCJX/version/FV_5H7MEHG2'

Try It
======

If you try running ingest and the file was fetched successfully, you should see
something like this in your log::

    2019-04-24 11:19:31,719 - FileRetriever - INFO - Selected `token` authentication to fetch https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_HH5PMCJX/version/FV_5H7MEHG2
    2019-04-24 11:19:32,269 - kf_lib_data_ingest.network.utils - INFO - Successfully fetched https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_HH5PMCJX/version/FV_5H7MEHG2 with original file name "family_and_phenotype.tsv"

**Don't worry if your ingest package fails validation. You will learn how to
fix this in a later section.**
