from __future__ import annotations

import psycopg2 as pg2
import sys
from bs4 import BeautifulSoup
import requests

from pprint import pprint

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c

from typing import Union, Callable
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
    SCHEMA_RAW_URI_PAGES = {
        "raw_uri_id": None,
        "coin_id": None,
        "has_examples": None,
        "has_examples_pagination": None,
        "examples_pagination_id": None,
        "examples_total_pagination": None,
        "examples_start_id": None,
        "examples_end_id": None,
        "examples_max_id": None,
        "page_html": None,
    }

    def __init__(
        self,
        db_name: str,
        pages_to_sample: Union[None, int] = None,
        only_found: bool = True,
    ) -> None:
        """Construct instance of class."""
        self.db_name = db_name
        self.pages_to_sample = pages_to_sample
        self.only_found = only_found
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

    def scrape_browse_results(self) -> None:
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
            data = ScrapeOcre.SCHEMA_RAW_BROWSE_PAGES.copy()
            scraped_values = (
                curr_page_id,
                url_page,
                start_coin_id,
                end_coin_id,
                str(soup),
            )
            ScrapeOcre.populate_raw_browse_pages_schema(data, scraped_values)
            path_insert_data_file = c.SQL_FOLDER / "insert" / "raw_browse_pages.sql"
            self.client.insert_data(path_insert_data_file, [data])

            # Toggle break condition
            if curr_page_id == max_num_pages:
                break_loop = True

            # Increment page index
            curr_page_id += 1

        print("\nFinished scraping Browse results...")
        return None

    def process_browse_results(self) -> None:
        """Process and save Browse results data."""
        # Query data
        print("Retrieving data from `raw_web_scrape` table...")
        path_query = c.SQL_FOLDER / "query" / "raw_browse_pages.sql"
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
                            if dt == "reference":
                                dd = dd.split(", ")
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
                path_insert_query = c.SQL_FOLDER / "insert" / "stg_coin_summaries.sql"
                self._insert_using_secondary_client(
                    path_insert_query, [processed_browse_data]
                )

    def scrape_canonical_uris(self) -> None:
        """Use processed `stg_coin_summaries` table data to scrape
        canonical URI pages and store raw data in `raw_uri_pages`
        table."""

        # Determine coin_id to start at
        print("Determining coin_id to start scraping URI pages at...")
        path_max_id = c.SQL_FOLDER / "query" / "raw_uri_pages_max_id.sql"
        self.client.query_data(path_max_id)
        query_params = {"start_coin_id": None}
        for row in self.client.cur:
            if row[0]:
                query_params["start_coin_id"] = row[0] + 1
            else:
                query_params["start_coin_id"] = 1
        print(
            f"Starting URI page scraping at coin_id #{query_params['start_coin_id']}..."
        )

        # Query data
        print("Retrieving data from `stg_coin_summaries` table...")
        if self.only_found:
            path_query = (
                c.SQL_FOLDER
                / "query"
                / "stg_coin_summaries_filter_coin_id_and_only_found.sql"
            )
        else:
            path_query = (
                c.SQL_FOLDER / "query" / "stg_coin_summaries_filter_coin_id.sql"
            )
        self.client.query_data(path_query, query_params)

        # Scrape canonical URIs and store raw data in raw_uri_pages
        for row in self.client.cur:
            data_query = ScrapeOcre.SCHEMA_STG_COIN_SUMMARY.copy()
            ScrapeOcre.populate_stg_coin_summaries_schema(
                data_query, row, all_fields=False
            )
            self._print_scrape_update_periodically(
                coin_id=data_query["coin_id"], interval=500
            )

            # Get HTML using requests library
            response = requests.get(data_query["coin_canonical_uri"])
            response.raise_for_status()

            # Convert HTML into soup
            soup = BeautifulSoup(response.text, "lxml")

            # Extract data from soup to later insert into database
            data_insert = ScrapeOcre.SCHEMA_RAW_URI_PAGES.copy()
            data_insert.pop("raw_uri_id")
            data_insert["coin_id"] = data_query["coin_id"]
            data_insert["page_html"] = str(soup)

            soup_examples = soup.find("div", class_="row", id="examples")
            if soup_examples:
                # If there is an examples section
                data_insert["has_examples"] = True
                soup_pagination = soup_examples.find_all("div", class_="col-md-12")
                if len(soup_pagination) > 1:
                    # If there is pagination in the examples section
                    data_insert["has_examples_pagination"] = True
                    soup_pagination_bar = soup_pagination[1]
                    data_examples, data_pagination = soup_pagination_bar.find_all(
                        "div", class_="col-md-6"
                    )

                    # Scraping examples IDs
                    data_examples_ids = data_examples.text.strip().split()[1::2]
                    (
                        data_insert["examples_start_id"],
                        data_insert["examples_end_id"],
                        data_insert["examples_max_id"],
                    ) = data_examples_ids

                    # Scraping pagination data
                    data_insert["examples_pagination_id"] = int(
                        data_pagination.find(
                            "button", class_="btn btn-default active"
                        ).text.strip()
                    )
                    data_insert["examples_total_pagination"] = int(
                        data_pagination.find(
                            "a", class_="btn btn-default", title="Last"
                        ).text.strip()
                    )
                else:
                    # If there is not pagination in the examples section
                    data_insert["has_examples_pagination"] = False

                    data_insert["examples_start_id"] = 1
                    num_coins_on_page = len(
                        soup_examples.find_all("div", class_="g_doc col-md-4")
                    )
                    data_insert["examples_end_id"] = num_coins_on_page
                    data_insert["examples_max_id"] = num_coins_on_page

                    data_insert["examples_pagination_id"] = None
                    data_insert["examples_total_pagination"] = None
            else:
                # If there is not an examples section
                data_insert["has_examples"] = False
                data_insert["has_examples_pagination"] = False

                data_insert["examples_start_id"] = None
                data_insert["examples_end_id"] = None
                data_insert["examples_max_id"] = None

                data_insert["examples_pagination_id"] = None
                data_insert["examples_total_pagination"] = None

            # Insert data into database
            path_insert = c.SQL_FOLDER / "insert" / "raw_uri_pages.sql"
            self._insert_using_secondary_client(path_insert, [data_insert])

        print("Finished scraping canonical URIs...")
        return None

    def scrape_uris_pagination(self) -> None:
        """Scrape URI pages with pagination in the examples section and
        store in raw_uri_pages table."""

        # Query URI pages with pagination
        print("Querying URI pages with pagination...")
        path_pagination_query = c.SQL_FOLDER / "query" / "raw_uri_pages_pagination.sql"
        self.client.query_data(path_pagination_query)
        num_coins_with_pagination = self.client.cur.rowcount
        print(f"There are {num_coins_with_pagination} coins with pagination...")

        # Scrape URI pages with pagination
        # TODO: develop section

        print("Finished scraping URIs with pagination...")
        return None

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
        client_temp = Topsy(self.db_name, silent=True)
        client_temp.insert_data(path, data)
        client_temp.close_connection()
        return None

    def _print_scrape_update_periodically(self, coin_id: int, interval: int) -> None:
        """Print to console scraping update message at an interval of
        number of records."""
        curr_row = self.client.cur.rownumber
        total_rows = self.client.cur.rowcount

        if curr_row == 1:
            print(
                f"Going into periodical updates, which are at every {interval} row..."
            )

        if (curr_row in (1, total_rows)) or (curr_row % interval == 0):
            print(f"Scraping coin #{coin_id} in row {curr_row} of {total_rows}...")
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

    @staticmethod
    def populate_stg_coin_summaries_schema(
        data_dict: dict, row_of_data: Union[list, tuple], all_fields: bool = False
    ) -> None:
        """Populate a stg_coin_summaries schema in place with data from
        row_of_data, where the order of the items in row_of_data
        corresponds to the ordinal position of columns from
        stg_coin_summaries and the timestamp field (`ts`) is omitted,
        for either all fields or a subset of fields in the table."""
        data_dict["coin_id"] = row_of_data[0]
        data_dict["page_id"] = row_of_data[1]
        data_dict["coin_name"] = row_of_data[2]
        data_dict["coin_canonical_uri"] = row_of_data[3]

        if all_fields:
            data_dict["coin_date_string"] = row_of_data[4]
            data_dict["denomination"] = row_of_data[5]
            data_dict["mint"] = row_of_data[6]
            data_dict["obverse_description"] = row_of_data[7]
            data_dict["reverse_description"] = row_of_data[8]
            data_dict["reference"] = row_of_data[9]
            data_dict["num_objects_found"] = row_of_data[10]
        else:
            # Convert data_dict.keys() into list to prevent dict
            # changed size during iteration error
            for key in list(data_dict.keys()):
                if key not in ("coin_id", "page_id", "coin_name", "coin_canonical_uri"):
                    data_dict.pop(key)

        return None

    @staticmethod
    def populate_raw_uri_pages_schema(
        data_dict: dict, row_of_data: Union[list, tuple]
    ) -> None:
        """Populate a raw_uri_pages schema in place with data from
        row_of_data, where the order of the items in row_of_data
        corresponds to the ordinal position of columns from
        raw_uri_pages and the timestamp field (`ts`) is omitted."""
        data_dict["raw_uri_id"] = row_of_data[0]
        data_dict["coin_id"] = row_of_data[1]
        data_dict["has_examples"] = row_of_data[2]
        data_dict["has_examples_pagination"] = row_of_data[3]
        data_dict["examples_pagination_id"] = row_of_data[4]
        data_dict["examples_total_pagination"] = row_of_data[5]
        data_dict["examples_start_id"] = row_of_data[6]
        data_dict["examples_end_id"] = row_of_data[7]
        data_dict["examples_max_id"] = row_of_data[8]
        data_dict["page_html"] = row_of_data[9]
        return None

    @staticmethod
    def try_except_with_retry(
        instance_method: Callable[[], None], retry_limit: int = 50
    ) -> None:
        """Run method from ScrapeOcre on an instance with try-except
        clauses and a retry limit."""
        num_retries = 0
        while num_retries <= retry_limit:
            try:
                instance_method()
                break
            except requests.exceptions.ConnectTimeout as err:
                print("\nREQUESTS CONNECTION TIMEOUT:")
                print(err)
            except requests.exceptions.ConnectionError as err:
                print("\nREQUESTS CONNECTION ERROR:")
                print(err)

            num_retries += 1
            print(f"Current retry count: {num_retries} / {retry_limit}")
            if num_retries <= retry_limit:
                print("Retrying scrape of Canonical URIs...")
            else:
                print("Ending scrape of Canonical URIs...")

        return None


if __name__ == "__main__":
    # pipeline = ScrapeOcre("delme_ocre", pages_to_sample=40, only_found=False)
    pipeline = ScrapeOcre("ocre", only_found=False)

    # Connect
    try:
        pipeline.connect_to_database()
    except ValueError as err:
        print(err)
        print(f"Unable to connect to `{pipeline.db_name}`.")
        sys.exit(1)
    except pg2.OperationalError as err:
        Topsy.print_pg2_exception(err)
        sys.exit(1)

    # Scrape Browse results pages
    # try:
    #     pipeline.scrape_browse_results()
    # except requests.exceptions.RequestException as err:
    #     print(f"\nREQUEST ERROR: Encountered error trying to connect to webpage.")
    #     print(err)
    #     pipeline.disconnect_from_database()
    #     sys.exit(1)

    # Process Browse results pages
    # pipeline.process_browse_results()

    # Scrape raw Canonical URI pages
    # TODO: modify script so that method below can be re-run for incomplete scrapes
    # TODO: replace code below with try_except_with_retry staticmethod
    # num_retries = 0
    # retry_limit = 50  # Arbitrary number
    # while num_retries <= retry_limit:
    #     try:
    #         pipeline.scrape_canonical_uris()
    #         break
    #     except requests.exceptions.ConnectTimeout as err:
    #         print("\nREQUESTS CONNECTION TIMEOUT:")
    #         print(err)
    #     except requests.exceptions.ConnectionError as err:
    #         print("\nREQUESTS CONNECTION ERROR:")
    #         print(err)

    #     num_retries += 1
    #     print(f"Current retry count: {num_retries} / {retry_limit}")
    #     if num_retries <= retry_limit:
    #         print("Retrying scrape of Canonical URIs...")
    #     else:
    #         print("Ending scrape of Canonical URIs...")

    # Scrape raw Canonical URI pages with pagination
    ScrapeOcre.try_except_with_retry(pipeline.scrape_uris_pagination)

    # Process Canonical URI pages
    # pipeline.process_canonical_uris()

    # Disconnect
    pipeline.disconnect_from_database()
