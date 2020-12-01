.. _Warehousing:

==================
Remote Warehousing
==================

Sharing is caring, so storing ingest debugging data only on your own computer
is bad news bears. With minimal configuration, the ingest system can connect to
a remote warehousing database to centrally store your package's intermediate
Extract and Transform results as well as the KFID lookup cache for the study.
This makes it easier for others to assist in replicating, validating, and
modifying the results of an ingest.

Warehouse Server Requirements
=============================

The technical requirements for warehousing are:

1. A database server that supports namespace schemas (ask your group
   administrator if one has already been set up)

   * PostgreSQL will work
   * MySQL will **not** work

2. An account on the database server with sufficient access permissions to
   invoke ``CREATE DATABASE``, ``CREATE SCHEMA``, ``DROP SCHEMA``,
   ``CREATE TABLE``, ``DROP TABLE``, and ``INSERT``

3. The power of friendship

.. note::

    Connection details for the warehouse used for ingesting Kids First studies
    can be found at https://github.com/d3b-center/clinical-data-flow/tree/master/warehouse

Local Environment Configuration
===============================

In your local environment, set a variable named according to the definition of
``SECRETS.WAREHOUSE_DB_URL`` in the :ref:`application settings
configuration<Tutorial-App-Settings>` to equal a complete login URL, including
username and password if needed, that can be used for calling ``CREATE
DATABASE`` on the server.

As shown here from ``app/settings/base.py``, the variable is named
``KF_WAREHOUSE_DB_URL`` by default.

.. literalinclude:: ../../../kf_lib_data_ingest/app/settings/base.py
  :pyobject: SECRETS
  :emphasize-lines: 4

So we would set ``KF_WAREHOUSE_DB_URL`` in our shell environment::

    export KF_WAREHOUSE_DB_URL=postgresql://<username>:<password>@<address>:<port>/postgres

.. note::

    PostgreSQL requires all connections to go directly to a database regardless
    of what you're doing, so we use the default "postgres" database for this.

If this variable is set, the ingest system will use the designated server as a
central warehouse. If this environment variable is not set, then it will skip
warehousing.

.. _DisablingWarehousing:

Temporarily Disabling Warehousing
=================================

If for any reason you don't want to use the warehouse during a run (for example
if you're just testing), the ``--no_warehouse`` command line argument will skip
the warehousing steps.

Storage Layout
==============

After ingesting a study, the warehouse server should have a database named
after the study KFID.

.. image:: /_static/images/warehouse_db.png

Inside that database you should find schemas for each of the warehoused stages
prefixed by "Ingest:" and the name of the package. Each schema table will be
one of that stage's returned DataFrames.

.. image:: /_static/images/warehouse_inside_db.png

Lifecycle
=========

Whenever you ingest a package, that package's prior existing schemas will be
replaced.
