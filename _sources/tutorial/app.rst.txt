.. _Tutorial-Ingest-App:

=====================
Kids First Ingest App
=====================

While the ingest library aims to be fairly generic, the built-in ingest app
is an example of how the library can be use used to support specific project(s)
or use cases. This section will explain what app settings are and how they help
us tailor usage of the library for specific projects.

If you will be developing ingest packages for Kids First study datasets,
then this section will also explain where source data files should live
(not on your computer!) and how to access them.

App Settings
============

App settings include configuration that applies to a group of ingest packages
which are all part of the same project.

Here's what an example app settings file looks like:

.. literalinclude:: ../../../kf_lib_data_ingest/app/settings/development.py
   :language: python
   :caption: kf_lib_data_ingest/app/settings/development.py

Each app settings file is a Python module and all app settings files live in
the ``kf_lib_data_ingest/app/settings`` directory. Common settings can be
imported from ``base.py``.

App level settings currently include but are not limited to:

- ``TARGET_API_CONFIG`` the path to the target API configuration module
  which specifies how to transform data into a form expected by the target
  service
- ``AUTH_CONFIGS`` dict, which specifies what authentication parameters to use
  when fetching remote source data files that require authentication.


CLI Option Override
-------------------

As an ingest package developer you will rarely need to worry about changing
app level settings. However, if you do need to modify settings you can do so by
supplying your own settings file via the ``--app_settings`` CLI option.
See ``kidsfirst ingest --help`` for details.


Ingest App Mode
===============

For Kids First projects, there are 2 settings files, each pertaining to a
different mode of operation. The mode of operation can be controlled by the
``KF_INGEST_APP_MODE`` environment variable.

- ``KF_INGEST_APP_MODE=production`` -> loads production.py settings
- ``KF_INGEST_APP_MODE=development`` -> loads development.py settings

Production Mode
---------------

This mode is used when the ingest/test commands are run as part of a
CI/CD process. It contains authentication configuration that is only used
in a CI/CD scenario.

As an ingest package developer you will never use this mode.

Development Mode
----------------

As a Kids First ingest package developer, you will want to make sure your
environment has the following set: ``KF_INGEST_APP_MODE=development``

This will instruct the ingest app to load the development settings,
which include authentication configuration for accessing files
via the Kids First Study Creator API, the single source of truth for source
data files within the Kids First ecosystem.

Developing for Kids First
=========================

If you are developing an ingest package for Kids First study data, then the
source data files for the study will be managed by the Kids First Study Creator
API. Read more about the study creator API at https://kids-first.github.io/kf-api-study-creator/

Upload Files
------------

Source data files must be uploaded to an existing Kids First study in the
study creator API. The easiest way to do this is via the
Kids First Data Tracker web app at https://kf-ui-data-tracker.kidsfirstdrc.org/

Currently, new studies must be created via the Kids First Dataservice API and
will then be mirrored/synced in the study creator API. This will likely change
in the near future so that studies may be created via the Data Tracker UI.

Download Files
--------------

Once files are uploaded into a study, they may be downloaded via the study
creator API's file endpoint with a developer token in the authorization header
of the request.

You will learn how to configure your ingest package to access these files
in the :ref:`Tutorial-Extract-Stage` section of the tutorial.
