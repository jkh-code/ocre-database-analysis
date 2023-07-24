from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c

import psycopg2 as pg2
import sys


SCHEMA_NAMES = ["raw_web_scrape", "stg_web_scrape"]
TABLE_CREATION_ORDER = [
    "raw_browse_pages",
    "stg_coin_summaries",
    "raw_uri_pages",
    "stg_coins",
    "stg_examples",
    "stg_examples_images",
    "stg_uri_pages",
]


def create_ocre_database(db_to_create_name: str) -> None:
    # Initial log in is to `postgres` database to create database
    try:
        client = Topsy("postgres")
    except ValueError as err:
        print(err)
        print(f"Unable to connect to `{client.conn_parameters['dbname']}`.")
        sys.exit(1)
    except pg2.OperationalError as err:
        Topsy.print_pg2_exception(err)
        sys.exit(1)

    client.create_new_database([db_to_create_name], switch=True)
    client.create_new_schema(SCHEMA_NAMES)

    path_create = c.SQL_FOLDER / "create"
    for table in TABLE_CREATION_ORDER:
        path_create_file = path_create / (table + ".sql")
        client.create_new_table(path_create_file)

    client.close_connection()

    return None


if __name__ == "__main__":
    create_ocre_database("delme_ocre")
