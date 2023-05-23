from multiprocessing import Value
import psycopg2 as pg2
from psycopg2 import sql
from os import environ
import sys

from typing import Union, List


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

    def create_new_database(self, db_names: List[str], switch: bool = False) -> None:
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

    def create_new_schema(self, schema_names: List[str]) -> None:
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

    def create_new_table(self):
        """Create new table in specified schema."""
        pass

    def insert_data(self):
        """Insert data into specified table."""
        pass

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
        temp = Topsy("delme")
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
    #     temp.create_new_schema(["raw", "stg", "fnd", "rpt"])
    # except ValueError as err:
    #     print(err)
    #     sys.exit(1)

    temp.close_connection()
