from http import client
from os import path
import psycopg2 as pg2
import sys
from bs4 import BeautifulSoup
import requests

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c


# TODO: Replace print statements with logging


if __name__ == "__main__":
    # Connect to postgres database and create client
    try:
        client = Topsy("ocre")
    except ValueError as err:
        print(err)
        print(f"Unable to connect to `{client.conn_parameters['dbname']}`.")
        sys.exit(1)
    except pg2.OperationalError as err:
        Topsy.print_pg2_exception(err)
        sys.exit(1)

    # Scraping Browse tab results
    # Accessing HTML for first page
    try:
        page_url = c.OCRE_BROWSE_PAGE + "0"
        print(f"\nExtracting HTML from {page_url} ...")
        response = requests.get(page_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(
            f"\nREQUESTS ERROR: Encountered error trying to connect to {c.OCRE_BROWSE_PAGE}"
        )
        print(err)
        client.close_connection()
        sys.exit(1)

    # Converting HTML into soup
    print("Converting HTML into Soup...")
    soup = BeautifulSoup(response.text, "lxml")

    # Extract display records and pagination data
    data_display_records = (
        soup.find("div", class_="paging_div row").contents[0].text.strip().split()
    )
    data_display_records = [int(item) for item in data_display_records[2 : 6 + 1 : 2]]
    start_coin_id, end_coin_id, max_coin_id = data_display_records

    page_id = int(
        soup.find("div", class_="paging_div row")
        .find("div", class_="col-md-6 page-nos")
        .text.strip()
    )

    # Calculate num pages to scrape
    num_pages = (max_coin_id // 20) + int(max_coin_id % 20 != 0)

    # Save raw data to postgres database
    data = {
        "page_id_": page_id,
        "page_url_": page_url,
        "start_coin_id_": start_coin_id,
        "end_coin_id_": end_coin_id,
        "page_html_": str(soup),
    }
    client.insert_data(c.SQL_FOLDER / "insert" / "insert_raw_browse_pages.sql", [data])

    # repeat for additional pages
    # TODO: Rewrite code above to be more modular
    # TODO: Write code for this section

    # Disconnect from postgres database
    client.close_connection()
