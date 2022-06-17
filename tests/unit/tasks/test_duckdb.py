import os
from pickle import TRUE
import pytest

from viadot.tasks import DuckDBCreateTableFromParquet
from viadot.sources.duckdb import DuckDB

TABLE = "test_table"
SCHEMA = "test_schema"
DATABASE_PATH = "test_db_123.duckdb"


@pytest.fixture(scope="session")
def duckdb():
    duckdb = DuckDB(credentials=dict(database=DATABASE_PATH))
    yield duckdb
    os.remove(DATABASE_PATH)


def test_create_table_empty_file(duckdb):
    path = "empty.parquet"
    with open(path, "w"):
        pass
    duckdb_creds = {f"database": DATABASE_PATH}
    task = DuckDBCreateTableFromParquet(credentials=duckdb_creds)
    task.run(schema=SCHEMA, table=TABLE, path=path, if_empty="skip")

    assert duckdb._check_if_table_exists(TABLE, schema=SCHEMA) == False
    os.remove(path)
