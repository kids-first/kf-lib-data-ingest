.. _Tutorial-Ingest-App:

=====================
Kids First Ingest App
=====================

While the ingest library aims to be fairly generic, the built-in ingest app
is an example of how the library can be used to support specific project(s) or
scenarios. This section will explain what app settings are and how they help
us tailor usage of the library for specific projects.

If you will be developing ingest packages for Kids First study datasets,
then this section will also explain where source data files should live
(not on your computer!) and how to access them.

.. _Tutorial-App-Settings:

App Settings
============

App settings include configuration that applies to a group of ingest packages
which are all part of the same project.

Each app settings file is a Python module, and all app settings files live in
the ``kf_lib_data_ingest/app/settings`` directory. Common settings can be
imported from ``base.py``.

App level settings currently include but are not limited to:

- ``TARGET_API_CONFIG`` the path to the target API configuration module
  which specifies how to transform data into a form expected by the target
  service
- ``AUTH_CONFIGS`` dict, which specifies what authentication parameters to use
  when fetching remote source data files that require authentication.

Here's what an example app settings file looks like:

.. literalinclude:: ../../../kf_lib_data_ingest/app/settings/development.py
   :language: python
   :caption: kf_lib_data_ingest/app/settings/development.py

.. _Tutorial-Authorization-Configs:

Authorization Configs
---------------------

TODO

.. image:: /_static/images/construction_small.gif

Ingest App Deployment Mode
--------------------------

There are 2 app settings files, each pertaining to a different mode of
deployment. The deployment mode can be controlled by the ``KF_INGEST_APP_MODE``
environment variable.

- ``KF_INGEST_APP_MODE=production`` -> loads settings from ``production.py``
- ``KF_INGEST_APP_MODE=development`` -> loads settings from ``development.py``

Development Mode
~~~~~~~~~~~~~~~~

As a Kids First ingest package developer, you will want to make sure your
environment has the following set: ``KF_INGEST_APP_MODE=development``

This will instruct the ingest app to load the development settings, which
include authentication configuration for accessing files via the Kids First
Study Creator API, the single source of truth for source data files within the
Kids First ecosystem.

Production Mode
~~~~~~~~~~~~~~~

This mode is used when the ingest/test commands are run as part of a CI/CD
process. It contains authentication configuration that is only used in a CI/CD
scenario.

An ingest package developer should never use this mode.

CLI Option Override
~~~~~~~~~~~~~~~~~~~

As an ingest package developer you should never need to worry about changing
app level settings. However, if you do need to, you can supply your own
settings file via the ``--app_settings`` CLI option. See ``kidsfirst ingest
--help`` for details.
