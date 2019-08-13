.. _Tutorial-Study-Creator:

=================================================
Source Data stored by the Kids First Data Tracker
=================================================

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

Get the URL of the Source Data File
===================================

The ``family_and_phenotype.tsv`` file has already been uploaded to the
``SD_ME0WME0W`` study via the Data Tracker web app. Here is the file's URL:

https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_ND1PHHW4

You will not be able to download the file unless you log in.

Update Your Extract Config
==========================

Use this URL for the ``source_data_url parameter`` in your extract config.

.. code-block:: python

    # my_study/extract_configs/family_and_phenotype.py

    source_data_url = 'https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_ND1PHHW4'

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

If you recall from the :ref:`Tutorial-Ingest-App` section, in ``development``
mode, the ``kf_lib_data_ingest/app/settings/development.py`` app settings are
used. If you look at that file, you'll see that the ingest app is already
configured to use whatever is stored in an environment variable called
``KF_STUDY_CREATOR_API_TOKEN`` when accessing URLs that start with
``https://kf-study-creator.kidsfirstdrc.org/download/study``.

.. literalinclude:: ../../../kf_lib_data_ingest/app/settings/development.py
   :language: python
   :caption: kf_lib_data_ingest/app/settings/development.py

Therefore in your shell environment when testing you can use:

.. code-block:: bash

   export KF_STUDY_CREATOR_API_TOKEN=YOUR_TOKEN

Be careful with this token and make sure to keep it secret.

Try It
======

If you try running ingest and the file was fetched successfully, you should see
something like this in your log::

    2019-04-24 11:19:31,719 - FileRetriever - INFO - Selected `token` authentication to fetch https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_ND1PHHW4
    2019-04-24 11:19:32,269 - kf_lib_data_ingest.network.utils - INFO - Successfully fetched https://kf-study-creator.kidsfirstdrc.org/download/study/SD_ME0WME0W/file/SF_ND1PHHW4 with original file name "family_and_phenotype.tsv"
