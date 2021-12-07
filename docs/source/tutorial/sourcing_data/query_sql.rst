======================================================
Source data stored in a PostgreSQL Database
======================================================

Scenario
========

You would like to extract data directly from a PostgreSQL Database.

.. note::
    Before deciding that you'd like to query a SQL database directly, instead,
    you may want to consider separately querying your database, saving the
    result of that query to a file, and then using that file as source for an
    extract config.

    By querying a database directly

        * It's no longer easy to audit the data used as the source for an
          ingest.
        * It's much harder, if not impossible, for non-technical users to know
          what data is used for an ingest
        * Using a database is inherently volatile e.g. rows may be added/
          deleted/ updated.

    Because of the above points, it's important to consider if your needs
    wouldn't be better served by using a static result of your query as
    source data for an ingest instead of directly querying the SQL database.


.sql File Solution
======================

A convenient way to load data from a postgres database is by saving your SQL
query in a file, loading that file as source data, then connecting to the
database and running your query inside of source_data_read_function.

``source_data_read_func`` will be used to parse the data from the
``source_data_url``.

As an example, say you save your query in the file at
``my-ingest-package/data/my_query.sql``:

.. code-block:: sql

    SELECT *
    FROM participant p
    WHERE p.study_id = 'SD_ME0WME0W'

Your extract config to pull data from your database could then look like:

.. code-block:: Python

    import pandas as pd
    import psycopg2

    source_data_url = "file://my-ingest-package/data/my_query.sql

    db_url = "postgresql://username:password@my-postgres-db-url.com:5432/postgres"

    source_data_read_params = {"connection_url": db_url}


    def source_data_read_func(filepath, connection_url, **kwargs):
        with open(filepath, "r") as f:
            query = f.read().replace("\n", " ")
        f.close
        conn = psycopg2.connect(connection_url)
        df = pd.read_sql(query, conn)
        return df

The resulting dataframe would then be the resulting data that comes from your
query. You can then continue on writing your extract config as you normally
would.
