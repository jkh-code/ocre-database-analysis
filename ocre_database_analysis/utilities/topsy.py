import psycopg2 as pg2
from os import environ


## TODO: Document class with docstring (https://realpython.com/documenting-python-code/#class-docstrings)


class Topsy:
    """Client for working with PostgreSQL databases."""

    def __init__(self):
        """Initialize client."""
        pass

    def close_connection(self):
        """Close cursor and connection objects."""
        pass

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
