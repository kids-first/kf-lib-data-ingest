from urllib.parse import urlparse

import sqlalchemy
from sqlalchemy.schema import DropSchema

SCHEMA_PREFIX = "Ingest:"


def db_study_url(db_maintenance_url, study_id):
    return urlparse(db_maintenance_url)._replace(path=f"/{study_id}").geturl()


def init_study_db(db_maintenance_url, study_id):
    """
    Create and/or reset a warehouse database for the given study.

    :param db_maintenance_url: Connection URL with login/password for the warehouse db
    :type db_maintenance_url: str
    :param study_id: Unique identifier for the study
    :type study_id: str
    """
    # create the study database if needed
    eng = sqlalchemy.create_engine(
        db_maintenance_url,
        isolation_level="AUTOCOMMIT",
        connect_args={"connect_timeout": 5},
    )
    try:
        eng.execute(f'CREATE DATABASE "{study_id}"')
    except sqlalchemy.exc.ProgrammingError as e:
        if "already exists" not in str(e):
            raise
    eng.dispose()

    # link to the study database
    eng = sqlalchemy.create_engine(
        db_study_url(db_maintenance_url, study_id),
        connect_args={"connect_timeout": 5},
    )

    # reset study database
    for s in sqlalchemy.inspect(eng).get_schema_names():
        if s.lower().startswith(SCHEMA_PREFIX.lower()):
            DropSchema(s, quote=True, cascade=True).execute(bind=eng)

    eng.dispose()


def persist_df_to_study_db(
    db_maintenance_url, study_id, stage_name, df, table_name
):
    """Store the contents of a result dataframe in the warehouse database.

    :param db_maintenance_url: Connection URL with login/password for the warehouse db
    :type db_maintenance_url: str
    :param study_id: Unique identifier for the study
    :type study_id: str
    :param stage_name: Name of the current stage
    :type stage_name: str
    :param df: A pandas DataFrame
    :type df: DataFrame
    :param table_name: Name for the new table
    :type table_name: str
    """
    # link to the study database
    eng = sqlalchemy.create_engine(
        db_study_url(db_maintenance_url, study_id),
        connect_args={"connect_timeout": 5},
    )

    schema_name = f"{SCHEMA_PREFIX}{stage_name}"

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
