import psycopg2 as pg2
from os import environ
import sys

from typing import Union


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
            dbname_ = dbname

        self.conn_parameters = {
            "dbname": dbname_,
            "username": environ["PGDS_USER"],
            "password": environ["PGDS_PASSWORD"],
            "host": environ["PGDS_HOST"],
            "port": environ["PGDS_PORT"],
        }

        print(f"Connecting to database `{self.conn_parameters['dbname']}`...")
        try:
            self.conn = pg2.connect(
                dbname=self.conn_parameters["dbname"],
                user=self.conn_parameters["username"],
                password=self.conn_parameters["password"],
                host=self.conn_parameters["host"],
                port=self.conn_parameters["port"],
            )
        except pg2.OperationalError as err:
            print(
                f"\nERROR: Unable to connect to "
                + f"`{self.conn_parameters['dbname']}`..."
            )
            Topsy._print_pg2_exception(err)
            sys.exit(1)

        print(
            f"Creating cursor object in database "
            + f"`{self.conn_parameters['dbname']}`..."
        )
        self.cur = self.conn.cursor()

        # Setting autocommit to avoid ActiveSqlTransaction error
        self.conn.autocommit = True

        return None

    @staticmethod
    def _print_pg2_exception(err: Exception) -> None:
        _, _, err_traceback = sys.exc_info()
        line_num = err_traceback.tb_lineno

        print(f"psycopg2 ERROR on line number: {line_num}")
        print(f"psycopg2 ERROR:\n{err}")

        print(f"psycopg2 ERROR SQLSTATE code: {err.pgcode}")
        print(f"psycopg2 ERROR PG MESSAGE:\n{err.pgerror}")

        return None

    def close_connection(self) -> None:
        """Close cursor and connection objects."""
        print("Closing the cursor object...")
        self.cur.close()
        print(f"Closing the connecting to `{self.conn_parameters['dbname']}`...")
        self.conn.close()

        return None

    def create_new_database(self):
        """Create new database with option to switch to newly created
        database."""
        pass

    def create_new_schema(self):
        """Create new schemas in active database."""
        pass

    def create_new_table(self):
        """Create new table in specified schema."""
        pass

    def insert_data(self):
        """Insert data into specified table."""
        pass


if __name__ == "__main__":
    pass

    # Debug
    # -----

    # Connecting and closing connection
    temp = Topsy()
    temp.close_connection()
