from argon2 import Type
import psycopg2 as pg2
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
        # TODO: Add error handling on `dbname` (check it is a string)
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
                f"\nERROR: Unable to connect to `{self.conn_parameters['dbname']}`..."
            )
            Topsy.print_pg2_exception(err)
            sys.exit(1)

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
            raise TypeError("TYPE ERROR:`db_names` argument must be a list.")
        if not all(type(item) == str for item in db_names):
            raise TypeError("TYPE ERROR: all items in `db_names` must be strings.")
        if num_dbs > 1 and switch == True:
            raise ValueError(
                "VALUE ERROR: `switch` cannot be `True` when multiple databases are created."
            )

        # TODO: Finish create_new_database() method

        return None

    def create_new_schema(self):
        """Create new schemas in active database."""
        pass

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

        print(f"psycopg2 ERROR on line number: {line_num}")
        print(f"psycopg2 ERROR:\n{err}")

        print(f"psycopg2 ERROR SQLSTATE code: {err.pgcode}")
        print(f"psycopg2 ERROR PG MESSAGE:\n{err.pgerror}")

        return None


if __name__ == "__main__":
    pass

    # Debug
    # -----

    # Connecting and closing connection
    temp = Topsy()

    try:
        temp.create_new_database(["hello"], switch=True)
    except TypeError as err:
        print(err)
        temp.close_connection()
        sys.exit(1)
    except ValueError as err:
        print(err)
        temp.close_connection()
        sys.exit(1)

    temp.close_connection()
