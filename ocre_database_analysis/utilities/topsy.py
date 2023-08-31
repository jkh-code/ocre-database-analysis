from __future__ import annotations

import psycopg2 as pg2
from psycopg2 import sql
from os import environ
import sys
from pathlib import Path
from pathlib import PosixPath, WindowsPath

from typing import Union

import ocre_database_analysis.constants as c


# TODO: Document class with docstring (https://realpython.com/documenting-python-code/#class-docstrings)
# TODO: Replace print statements with logging
# TODO: Remove print/logging statements from Topsy and move to scripts that use Topsy


class Topsy:
    """Client for working with PostgreSQL databases."""

    def __init__(self, dbname: Union[None, str] = None) -> None:
        """Initialize client."""

        # Creating connection parameters
        if not dbname:
            dbname_ = environ["PGDS_DBNAME"]
        else:
            if type(dbname) != str:
                raise ValueError("VALUE ERROR:`dbname` must be a string.")
            dbname_ = dbname

        self.conn_parameters = {
            "dbname": dbname_,
            "username": environ["PGDS_USER"],
            "password": environ["PGDS_PASSWORD"],
            "host": environ["PGDS_HOST"],
            "port": environ["PGDS_PORT"],
        }

        self.conn = None
        self.cur = None
        self._open_connections()

        return None

    def _open_connections(self) -> None:
        self.conn = pg2.connect(
            dbname=self.conn_parameters["dbname"],
            user=self.conn_parameters["username"],
            password=self.conn_parameters["password"],
            host=self.conn_parameters["host"],
            port=self.conn_parameters["port"],
        )

        self.cur = self.conn.cursor()

        # Setting autocommit to avoid ActiveSqlTransaction error
        self.conn.autocommit = True

        return None

    def close_connection(self) -> None:
        """Close cursor and connection objects."""
        self.cur.close()
        self.conn.close()
        return None

    def create_new_database(self, db_names: list[str], switch: bool = False) -> None:
        """Create new database with option to switch to newly created
        database."""
        print("Creating new databases...")
        num_dbs = len(db_names)

        if type(db_names) != list:
            raise ValueError("VALUE ERROR:`db_names` argument must be a list.")
        if not all(type(item) == str for item in db_names):
            raise ValueError("VALUE ERROR: all items in `db_names` must be strings.")
        if num_dbs > 1 and switch == True:
            raise ValueError(
                "VALUE ERROR: `switch` cannot be `True` when multiple databases are created."
            )

        for name in db_names:
            print(f"Dropping existing database with the name `{name}`...")
            query_drop_db = "DROP DATABASE IF EXISTS {database_name};"
            query_drop_db = sql.SQL(query_drop_db).format(
                database_name=sql.Identifier(name)
            )
            self.cur.execute(query_drop_db)

            print(f"Creating new database with name `{name}`...")
            query_create_db = "CREATE DATABASE {database_name};"
            query_create_db = sql.SQL(query_create_db).format(
                database_name=sql.Identifier(name)
            )
            self.cur.execute(query_create_db)

        if switch:
            db_name = db_names[0]
            print(f"Switching to `{db_name}`...")
            self.close_connection()
            self.conn_parameters["dbname"] = db_name
            self._open_connections()
            print(f"Connected to `{self.conn.info.dbname}`...")

        return None

    def create_new_schema(self, schema_names: list[str]) -> None:
        """Create new schemas in active database."""
        print("Trying to create new schema(s)...")

        if (type(schema_names) != list) or (not schema_names):
            raise ValueError("VALUE ERROR: `schema_names` must be a non-empty list.")
        if not all(type(item) == str for item in schema_names):
            raise ValueError(
                "VALUE ERROR: all items in `schema_names` must be strings."
            )

        for name in schema_names:
            print(f"Creating schema `{name}` in `{self.conn.info.dbname}`...")
            query_create_schema = "CREATE SCHEMA IF NOT EXISTS {schema_name};"
            query_create_schema = sql.SQL(query_create_schema).format(
                schema_name=sql.Identifier(name)
            )
            self.cur.execute(query_create_schema)

        return None

    def create_new_table(self, file_path: Union[PosixPath, WindowsPath]) -> None:
        """Create new SQL table from file path."""
        print("Trying to create new table...")

        if type(file_path) not in (PosixPath, WindowsPath):
            raise ValueError(
                "VALUE ERROR: `file_path` must be a `PosixPath` or `WindowsPath`."
            )
        if not file_path.exists():
            raise ValueError("VALUE ERROR: `file_path` does not exists.")
        if file_path.suffix != ".sql":
            raise ValueError("VALUE ERROR: `file_path` must be a SQL file.")

        print(f"Creating new table defined in {file_path.name}...")
        with open(file_path, "r", encoding="UTF-8") as f:
            query = f.read()

        self.cur.execute(query)
        print(f"Table created...")

        return None

    def insert_data(
        self, file_path: Union[PosixPath, WindowsPath], data: list[dict]
    ) -> None:
        """Insert data into specified table."""
        if type(file_path) not in (PosixPath, WindowsPath):
            raise ValueError(
                "VALUE ERROR: `file_path` must be a `PosixPath` or `WindowsPath`."
            )
        if not file_path.exists():
            raise ValueError("VALUE ERROR: `file_path` does not exists.")
        if file_path.suffix != ".sql":
            raise ValueError("VALUE ERROR: `file_path` must be a SQL file.")
        if (type(data) != list) or (
            not all(type(item) == dict for item in data) or (not data)
        ):
            raise ValueError("VALUE ERROR: `data` must be a list of dicts.")

        with open(file_path, "r", encoding="UTF-8") as f:
            query = f.read()

        self.cur.executemany(query, data)
        return None

    def query_data(
        self, file_path: Union[PosixPath, WindowsPath], params: Union[None, dict] = None
    ) -> None:
        """Query data using specified file and save results to cursor object.
        Use the `params` argument when the query has parameters to pass to
        it."""
        print(
            f"Trying to query `{self.conn_parameters['dbname']}` using {file_path.name}..."
        )

        if type(file_path) not in (PosixPath, WindowsPath):
            raise ValueError(
                "VALUE ERROR: `file_path` must be a `PosixPath` or `WindowsPath`."
            )
        if not file_path.exists():
            raise ValueError("VALUE ERROR: `file_path` does not exists.")
        if file_path.suffix != ".sql":
            raise ValueError("VALUE ERROR: `file_path` must be a SQL file.")

        # Loading data from postgres
        with open(file_path, "r", encoding="UTF-8") as f:
            query = f.read()

        if params:
            self.cur.execute(query, params)
        else:
            self.cur.execute(query)
        print(f"Query complete...")

        return None

    @staticmethod
    def print_pg2_exception(err: Exception) -> None:
        """Print line number, error, SQLSTATE code, and PG message to
        console."""
        _, _, err_traceback = sys.exc_info()
        line_num = err_traceback.tb_lineno

        print(f"\npsycopg2 ERROR on line number: {line_num}")
        print(f"psycopg2 ERROR:\n{err}")

        print(f"psycopg2 ERROR SQLSTATE code: {err.pgcode}")
        print(f"psycopg2 ERROR PG MESSAGE:\n{err.pgerror}")

        return None

    @staticmethod
    def try_postgres_connection(db_name: str) -> Topsy:
        client = None  # Defined here to turn off false positive error from pylance
        try:
            client = Topsy(db_name)
        except ValueError as err:
            print(err)
            print(f"Unable to connect to `{client.conn_parameters['dbname']}`.")
            sys.exit(1)
        except pg2.OperationalError as err:
            Topsy.print_pg2_exception(err)
            sys.exit(1)

        return client


if __name__ == "__main__":
    pass
