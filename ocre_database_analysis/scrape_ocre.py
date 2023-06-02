from multiprocessing import process
import psycopg2 as pg2
import sys
from bs4 import BeautifulSoup
import requests

from pprint import pprint

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c

from typing import Union
from pathlib import PosixPath, WindowsPath


# TODO: Replace print statements with logging


class ScrapeOcre:
    """Scrape OCRE website and process HTML."""

    # Schemas without `ts` field
    SCHEMA_RAW_BROWSE_PAGES = {
        "page_id": None,
        "page_url": None,
        "start_coin_id": None,
        "end_coin_id": None,
        "page_html": None,
    }
    SCHEMA_STG_COIN_SUMMARY = {
        "coin_id": None,
        "page_id": None,
        "coin_name": None,
        "coin_canonical_uri": None,
        "coin_date_string": None,
        "denomination": None,
        "mint": None,
        "obverse_description": None,
        "reverse_description": None,
        "reference": None,
        "num_objects_found": None,
    }

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
            response = requests.get(url_page)
            response.raise_for_status()

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
            # TODO: Replace with OcreScrape schema
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
        """Process and save Browse results data."""
        # Query data
        # TODO: Change file names for all SQL files such that file below is:
        # c.SQL_FOLDER / "query" / "raw_browse_pages.sql"
        # TODO: Change all table names in database such that the table used below is:
        # raw_web_scrape.browse_pages
        print("Retrieving data from `raw_web_scrape` table...")
        path_query = c.SQL_FOLDER / "query" / "query_raw_browse_pages.sql"
        print(path_query)
        self.client.query_data(path_query)

        for row in self.client.cur:
            raw_browse_data = ScrapeOcre.SCHEMA_RAW_BROWSE_PAGES.copy()
            ScrapeOcre.populate_raw_browse_pages_schema(raw_browse_data, row)

            print(f"Processing data for `page_id` {raw_browse_data['page_id']}...")
            soup = BeautifulSoup(raw_browse_data["page_html"], "lxml")
            all_page_coins = soup.find_all("div", class_="row result-doc")
            all_coin_ids = range(
                raw_browse_data["start_coin_id"], raw_browse_data["end_coin_id"] + 1
            )
            for coin_id, coin in zip(all_coin_ids, all_page_coins):
                print(f"Processing data for `coin_id` {coin_id}...")
                processed_browse_data = ScrapeOcre.SCHEMA_STG_COIN_SUMMARY.copy()

                processed_browse_data["coin_id"] = coin_id
                processed_browse_data["page_id"] = raw_browse_data["page_id"]

                soup_title = coin.find("div", class_="col-md-12").find("a")
                processed_browse_data["coin_name"] = soup_title.text.strip()
                processed_browse_data["coin_canonical_uri"] = (
                    c.OCRE_HOME_PAGE + soup_title["href"]
                )

                left_data = coin.find("div", class_="col-md-7 col-lg-8").contents[0]
                all_dt = left_data.find_all("dt")
                all_dd = left_data.find_all("dd")
                for dt, dd in zip(all_dt, all_dd):
                    dt, dd = dt.text.strip(), dd.text.strip()
                    # Skip dt-dd pairs where the text in dt is blank
                    if dt:
                        dt = self._convert_dt(dt)
                        if dt in processed_browse_data.keys():
                            processed_browse_data[dt] = dd

                right_text = coin.find(
                    "div", class_="col-md-5 col-lg-4 pull-right"
                ).text.strip()
                if right_text:
                    right_text_mod = right_text.split(sep=";", maxsplit=1)[0]
                    if "object" in right_text_mod:
                        processed_browse_data["num_objects_found"] = int(
                            right_text_mod.split(maxsplit=1)[1]
                        )
                    else:
                        processed_browse_data["num_objects_found"] = 0
                else:
                    # When empty
                    processed_browse_data["num_objects_found"] = 0

                # Write processed data to database
                path_insert_query = (
                    c.SQL_FOLDER / "insert" / "insert_stg_coin_summaries.sql"
                )
                self._insert_using_secondary_client(
                    path_insert_query, [processed_browse_data]
                )

    def scrape_canonical_uris(self):
        pass

    def process_canonical_uris(self):
        pass

    def _convert_dt(self, tag: str) -> str:
        """Process "dt" tags from raw_browse_pages coins for use as
        keys in ScrapeOcre.SCHEMA_STG_COIN_SUMMARY dict."""
        tag_ = tag.lower()
        if tag_ in ("obverse", "reverse"):
            tag_ = tag_ + "_description"
        if tag_ == "date":
            tag_ = "coin_date_string"
        return tag_

    def _insert_using_secondary_client(
        self, path: Union[PosixPath, WindowsPath], data: list[dict]
    ) -> None:
        """Insert data into database using secondary client to preserve
        existing client."""
        client_temp = Topsy(self.db_name)
        client_temp.insert_data(path, data)
        client_temp.close_connection()
        return None

    @staticmethod
    def populate_raw_browse_pages_schema(
        data_dict: dict, row_of_data: Union[list, tuple]
    ) -> None:
        """Populate a raw_browse_pages schema in place with data from
        row_of_data, where the order of the items in row_of_data
        correspond to the ordinal position of columns from
        raw_browse_pages and the timestamp field (`ts`) is omitted."""
        data_dict["page_id"] = row_of_data[0]
        data_dict["page_url"] = row_of_data[1]
        data_dict["start_coin_id"] = row_of_data[2]
        data_dict["end_coin_id"] = row_of_data[3]
        data_dict["page_html"] = row_of_data[4]
        return None


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

    # try:
    #     pipeline.scrape_browse_results()
    # except requests.exceptions.RequestException as err:
    #     print(
    #         f"\nREQUEST ERROR: Encountered error trying to connect to webpage."
    #     )
    #     print(err)
    #     pipeline.disconnect_from_database()
    #     sys.exit(1)

    pipeline.process_browse_results()

    pipeline.disconnect_from_database()
