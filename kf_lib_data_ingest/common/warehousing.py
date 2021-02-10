from urllib.parse import urlparse

import sqlalchemy
from sqlalchemy.schema import DropSchema


def make_prefix(ingest_package_name):
    return f"Ingest:{ingest_package_name}:"


def db_project_url(db_maintenance_url, project_id):
    return urlparse(db_maintenance_url)._replace(path=f"/{project_id}").geturl()


def init_project_db(db_maintenance_url, project_id, ingest_package_name):
    """
    Create and/or reset a warehouse database for the given project.

    :param db_maintenance_url: Connection URL with login/password for the warehouse db
    :type db_maintenance_url: str
    :param project_id: Unique identifier for the project
    :type project_id: str
    """
    # create the project database if needed
    # (this uses AUTOCOMMIT because CREATE DATABASE won't run in a transaction)
    eng = sqlalchemy.create_engine(
        db_maintenance_url,
        isolation_level="AUTOCOMMIT",
        connect_args={"connect_timeout": 5},
    )
    try:
        eng.execute(f'CREATE DATABASE "{project_id}"')
    except sqlalchemy.exc.ProgrammingError as e:
        if "already exists" not in str(e):
            raise
    eng.dispose()

    # link to the project database
    eng = sqlalchemy.create_engine(
        db_project_url(db_maintenance_url, project_id),
        connect_args={"connect_timeout": 5},
    )

    # drop prior package schemas
    for s in sqlalchemy.inspect(eng).get_schema_names():
        if s.startswith(make_prefix(ingest_package_name)):
            DropSchema(s, quote=True, cascade=True).execute(bind=eng)

    eng.dispose()


def persist_df_to_project_db(
    db_maintenance_url,
    project_id,
    stage_name,
    df,
    table_name,
    ingest_package_name,
):
    """Store the contents of a result dataframe in the warehouse database.

    :param db_maintenance_url: Connection URL with login/password for the warehouse db
    :type db_maintenance_url: str
    :param project_id: Unique identifier for the project
    :type project_id: str
    :param stage_name: Name of the current stage
    :type stage_name: str
    :param df: A pandas DataFrame
    :type df: DataFrame
    :param table_name: Name for the new table
    :type table_name: str
    """
    # link to the project database
    eng = sqlalchemy.create_engine(
        db_project_url(db_maintenance_url, project_id),
        connect_args={"connect_timeout": 5},
    )

    schema_name = f"{make_prefix(ingest_package_name)}{stage_name}"

    # create stage schema if needed
    if not eng.dialect.has_schema(eng, schema_name):
        eng.execute(sqlalchemy.schema.CreateSchema(schema_name))

    # submit records
    df.to_sql(
        table_name,
        eng,
        schema=schema_name,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10000,
    )

    eng.dispose()

    return schema_name
