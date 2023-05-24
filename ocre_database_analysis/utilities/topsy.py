from multiprocessing import Value
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
        self.open_connections()

        return None

    def open_connections(self) -> None:
        print(f"Connecting to database `{self.conn_parameters['dbname']}`...")
        self.conn = pg2.connect(
            dbname=self.conn_parameters["dbname"],
            user=self.conn_parameters["username"],
            password=self.conn_parameters["password"],
            host=self.conn_parameters["host"],
            port=self.conn_parameters["port"],
        )

        print(
            f"Creating cursor object in database `{self.conn_parameters['dbname']}`..."
        )
        self.cur = self.conn.cursor()

        # Setting autocommit to avoid ActiveSqlTransaction error
        self.conn.autocommit = True

        return None

    def close_connection(self) -> None:
        """Close cursor and connection objects."""
        print("\nClosing the cursor object...")
        self.cur.close()
        print(f"Closing the connecting to `{self.conn_parameters['dbname']}`...")
        self.conn.close()

        return None

    def create_new_database(self, db_names: list[str], switch: bool = False) -> None:
        """Create new database with option to switch to newly created
        database."""
        print("\nCreating new databases...")
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
            self.open_connections()
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
        print("\nTrying to create new table...")

        if type(file_path) not in (PosixPath, WindowsPath):
            raise ValueError(
                "VALUE ERROR: `file_path` must be a `PosixPath` or `WindowsPath`."
            )
        if not file_path.exists():
            raise ValueError("VALUE ERROR: `file_path` does not exists.")
        if file_path.suffix != ".sql":
            raise ValueError("VALUE ERROR: `file_path` must be a SQL file.")

        print(f"Creating new table defined in file {file_path}...")
        with open(file_path, "r", encoding="UTF-8") as f:
            print(f"Reading {file_path}...")
            query = f.read()

        print(f"Executing query...")
        self.cur.execute(query)
        print(f"Table created...")

        return None

    def insert_data(
        self, file_path: Union[PosixPath, WindowsPath], data=list[dict]
    ) -> None:
        """Insert data into specified table."""
        print("\nTrying to insert data...")

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

        print(f"Inserting data using file {file_path}...")
        with open(file_path, "r", encoding="UTF-8") as f:
            print(f"Reading {file_path}...")
            query = f.read()

        print(f"Executing query...")
        self.cur.executemany(query, data)
        print(f"Data inserted...")

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


if __name__ == "__main__":
    pass

    # Debug
    # -----

    # Connecting and closing connection
    try:
        temp = Topsy("ocre")
    except ValueError as err:
        print(err)
        print(f"Unable to connect to `{temp.conn_parameters['dbname']}`.")
        sys.exit(1)
    except pg2.OperationalError as err:
        Topsy.print_pg2_exception(err)
        sys.exit(1)

    # Creating new databases
    # try:
    #     # temp.create_new_database(["delme", "ocre", "nhl", "candy_land"])
    #     # temp.create_new_database(["delme", "ocre", "nhl", "candy_land"], switch=True)
    #     temp.create_new_database(["ocre"], switch=True)
    # except ValueError as err:
    #     print(err)
    #     temp.close_connection()
    #     sys.exit(1)

    # Creating new schema
    # try:
    #     # temp.create_new_schema("fail")
    #     # temp.create_new_schema([])
    #     # temp.create_new_schema(["success"])
    #     # temp.create_new_schema(["raw", "stg", "fnd", "rpt"])
    #     temp.create_new_schema(["raw_web_scrape"])
    # except ValueError as err:
    #     print(err)
    #     sys.exit(1)

    # Creating new table
    # try:
    #     # temp.create_new_table("/will/fail")
    #     path_file = c.SQL_FOLDER / "create" / "create_raw_browse_pages.sql"
    #     temp.create_new_table(path_file)
    # except ValueError as err:
    #     print(err)
    #     sys.exit(1)

    # Inserting data
    # data_test = [
    #     {
    #         "page_id_": 1,
    #         "page_url_": "start=0",
    #         "start_coin_id_": 1,
    #         "end_coin_id_": 20,
    #         "page_html_": "sample",
    #     },
    #     {
    #         "page_id_": 2,
    #         "page_url_": "start=20",
    #         "start_coin_id_": 21,
    #         "end_coin_id_": 40,
    #         "page_html_": "sample",
    #     },
    #     {
    #         "page_id_": 3,
    #         "page_url_": "start=40",
    #         "start_coin_id_": 41,
    #         "end_coin_id_": 60,
    #         "page_html_": "sample",
    #     },
    #     {
    #         "page_id_": 4,
    #         "page_url_": "start=60",
    #         "start_coin_id_": 61,
    #         "end_coin_id_": 80,
    #         "page_html_": "sample",
    #     },
    #     {
    #         "page_id_": 5,
    #         "page_url_": "start=80",
    #         "start_coin_id_": 81,
    #         "end_coin_id_": 100,
    #         "page_html_": "sample",
    #     },
    # ]
    data_test = [
        {
            "page_id_": 99,
            "page_url_": "start=99",
            "start_coin_id_": 99,
            "end_coin_id_": 99,
            "page_html_": "sample",
        }
    ]

    try:
        path_file = c.SQL_FOLDER / "insert" / "insert_raw_browse_pages.sql"
        temp.insert_data(path_file, data_test)
    except ValueError as err:
        print(err)
        sys.exit(1)

    temp.close_connection()
