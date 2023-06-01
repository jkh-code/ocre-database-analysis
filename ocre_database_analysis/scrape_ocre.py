import psycopg2 as pg2
import sys
from bs4 import BeautifulSoup
import requests

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c

from typing import Union


# TODO: Replace print statements with logging


class ScrapeOcre:
    """Scrape OCRE website and process HTML."""

    def __init__(self, db_name: str, pages_to_sample: Union[None, int] = None) -> None:
        """Construct instance of class."""
        self.db_name = db_name
        self.pages_to_sample = pages_to_sample
        self.client = None
        return None

    def connect_to_database(self) -> None:
        """Connect to database using Topsy instance."""
        self.client = Topsy(self.db_name)
        return None

    def disconnect_from_database(self) -> None:
        """Disconnect from database"""
        self.client.close_connection()
        return None

    def scrape_browse_results(self):
        """Scrape Browse results and save HTML to database."""
        curr_page_id = 1
        max_num_pages = None
        break_loop = False

        while not break_loop:
            url_start_idx = (curr_page_id - 1) * 20
            url_page = c.OCRE_BROWSE_PAGE + str(url_start_idx)

            # Scraping HTML
            print(f"\nWorking on page #{curr_page_id} ...")
            print(f"Extracting HTML from {url_page} ...")

            # TODO: Move try-except block to code below inem statement
            try:
                response = requests.get(url_page)
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                print(
                    f"\nREQUEST ERROR: Encountered error trying to connect to page #{curr_page_id}."
                )
                print(f"REQUESTS ERROR: Error on page at {url_page}")
                print(err)
                self.disconnect_from_database()
                sys.exit(1)

            # Convert HTML into soup
            print("Converting HTML into Soup...")
            soup = BeautifulSoup(response.text, "lxml")

            # Extract display records data
            print("Extracting displayed records data...")
            data_display_records = (
                soup.find("div", class_="paging_div row")
                .contents[0]
                .text.strip()
                .split()
            )
            data_display_records = [
                int(item) for item in data_display_records[2 : 6 + 1 : 2]
            ]
            if curr_page_id == 1:
                start_coin_id, end_coin_id, max_coin_id = data_display_records
                if not self.pages_to_sample:
                    max_num_pages = (max_coin_id // 20) + int(max_coin_id % 20 != 0)
                else:
                    max_num_pages = min(
                        (max_coin_id // 20) + int(max_coin_id % 20 != 0),
                        self.pages_to_sample,
                    )
            else:
                start_coin_id, end_coin_id, _ = data_display_records

            # Save raw data to postgres database
            print("Saving data to database...")
            data = {
                "page_id_": curr_page_id,
                "page_url_": url_page,
                "start_coin_id_": start_coin_id,
                "end_coin_id_": end_coin_id,
                "page_html_": str(soup),
            }
            path_insert_data_file = (
                c.SQL_FOLDER / "insert" / "insert_raw_browse_pages.sql"
            )
            self.client.insert_data(path_insert_data_file, [data])

            # Toggle break condition
            if curr_page_id == max_num_pages:
                break_loop = True

            # Increment page index
            curr_page_id += 1

        print("\nFinished scraping Browse results...")
        return None

    def process_browse_results(self):
        pass

    def scrape_canonical_uris(self):
        pass

    def process_canonical_uris(self):
        pass


if __name__ == "__main__":
    pipeline = ScrapeOcre("delme_ocre", pages_to_sample=20)

    try:
        pipeline.connect_to_database()
    except ValueError as err:
        print(err)
        print(f"Unable to connect to `{pipeline.db_name}`.")
        sys.exit(1)
    except pg2.OperationalError as err:
        Topsy.print_pg2_exception(err)
        sys.exit(1)

    pipeline.scrape_browse_results()

    pipeline.disconnect_from_database()
