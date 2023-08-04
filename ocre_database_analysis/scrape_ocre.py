from __future__ import annotations
from numpy import insert

import psycopg2 as pg2
import sys
from bs4 import BeautifulSoup
import requests
import re

from pprint import pprint

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c

from typing import Union, Callable
from pathlib import PosixPath, WindowsPath


# TODO: Replace print statements with logging


class ScrapeOcre:
    """Scrape OCRE website and process HTML."""

    TYPOLOGICAL_FIELD_CONVERSION = {
        "date": "coin_date_range",
        "date_range": "coin_date_range",
        "denomination": "denomination",
        "manufacture": "manufacture",
        "material": "material",
        "object_type": "object_type",
        "authority_authority": "authority_name",
        "authority_issuer": "issuer_name",
        "authority_stated_authority": "stated_authority_name",
        "date_on_object_date": "coin_date_range",
        "geographic_mint": "mint",
        "geographic_region": "region",
        "obverse_controlmark": "obverse_controlmark",
        "obverse_deity": "obverse_deity",
        "obverse_legend": "obverse_legend",
        "obverse_portrait": "obverse_portrait",
        "obverse_state": "obverse_state",
        "obverse_type": "obverse_type",
        "reverse_control_marks": "reverse_control_marks",
        "reverse_deity": "reverse_deity",
        "reverse_dynasty": "reverse_dynasty",
        "reverse_legend": "reverse_legend",
        "reverse_mintmark": "reverse_mintmark",
        "reverse_monogram": "reverse_monogram",
        "reverse_officinamark": "reverse_officinamark",
        "reverse_portrait": "reverse_portrait",
        "reverse_state": "reverse_state",
        "reverse_symbol": "reverse_symbol",
        "reverse_type": "reverse_type",
    }
    EXAMPLES_FIELDS_CONVERSION = {
        "axis": "coin_axis",
        "collection": "collection_name",
        "diameter": "coin_diameter",
        "findspot": "findspot",
        "hoard": "hoard",
        "identifier": "identifier",
        "weight": "coin_weight",
    }
    TYPOLOGICAL_ARRAY_FIELDS = [
        "denomination",
        "material",
        "object_type",
        "authority_name",
        "issuer_name",
        "mint",
        "region",
        "obverse_deity",
        "obverse_portrait",
        "obverse_state",
        "reverse_deity",
        "reverse_officinamark",
        "reverse_portrait",
        "reverse_state",
        "reverse_symbol",
    ]
    STG_EXAMPLES_NUMERIC_FIELDS = ["coin_axis", "coin_diameter", "coin_weight"]
    COLLECTIONS_WITH_IIIF = [
        "American Numismatic Society",
        "Bibliothèque nationale de France",
        "British Museum",
        "Coin Cabinet of the Mainz City Archives",
        "Harvard Art Museums",
        "J. Paul Getty Museum",
        "Münzkabinett der Universität Göttingen",
        "Oldenburg Municipal Museum",
        "State Coin Collection of Munich",
        "State Museum of Prehistory Halle",
        "The Fralin Museum of Art",
        "Thuringian Museum for Pre- and Early History",
        "University of Graz",
    ]
    MAINZ_CITY_LIKE_LINKS = [
        "Coin Cabinet of the Mainz City Archives",
        "Münzkabinett der Universität Göttingen",
        "Oldenburg Municipal Museum",
        "State Coin Collection of Munich",
        "State Museum of Prehistory Halle",
        "Thuringian Museum for Pre- and Early History",
    ]

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
    SCHEMA_STG_COINS = {
        "coin_id": None,
        "coin_name": None,
        "has_typological": None,
        "has_examples": None,
        "has_examples_pagination": None,
        "has_analysis": None,
        "coin_date_range": None,
        "denomination": None,
        "manufacture": None,
        "material": None,
        "object_type": None,
        "authority_name": None,
        "issuer_name": None,
        "stated_authority_name": None,
        "mint": None,
        "region": None,
        "obverse_controlmark": None,
        "obverse_deity": None,
        "obverse_legend": None,
        "obverse_portrait": None,
        "obverse_state": None,
        "obverse_type": None,
        "reverse_control_marks": None,
        "reverse_deity": None,
        "reverse_dynasty": None,
        "reverse_legend": None,
        "reverse_mintmark": None,
        "reverse_monogram": None,
        "reverse_officinamark": None,
        "reverse_portrait": None,
        "reverse_state": None,
        "reverse_symbol": None,
        "reverse_type": None,
        "average_axis": None,
        "average_diameter": None,
        "average_weight": None,
    }
    SCHEMA_STG_EXAMPLES = {
        "examples_id": None,
        "coin_id": None,
        "uri_page_examples_id": None,
        "example_name": None,
        "has_fields_section": None,
        "has_links_section": None,
        "coin_axis": None,
        "collection_name": None,
        "coin_diameter": None,
        "findspot": None,
        "hoard": None,
        "identifier": None,
        "coin_weight": None,
    }
    SCHEMA_STG_EXAMPLES_IMAGES = {
        "examples_images_id": None,
        "stg_examples_id": None,
        "image_type": None,
        "link": None,
    }
    SCHEMA_STG_URI_PAGES = {
        "uri_page_id": None,
        "coin_id": None,
        "examples_pagination_id": None,
        "examples_total_pagination": None,
        "examples_start_id": None,
        "examples_end_id": None,
        "examples_max_id": None,
        "uri_link": None,
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
        print("Scraping URI pages with pagination in examples section...")

        # Query URI pages with pagination
        print("Querying URI pages with pagination...")
        path_pagination_query = c.SQL_FOLDER / "query" / "raw_uri_pages_pagination.sql"
        self.client.query_data(path_pagination_query)
        num_coins_with_pagination = self.client.cur.rowcount
        print(f"There are {num_coins_with_pagination} coins with pagination...")

        # Scrape URI pages with pagination
        for row in self.client.cur:
            pagination_coin_d = {
                "coin_id": row[0],
                "last_page_scraped": row[1],
                "total_pages": row[2],
                "coin_canonical_uri": row[3],
            }
            self._print_scrape_update_periodically(
                pagination_coin_d["coin_id"], interval=50
            )

            start_page_id = pagination_coin_d["last_page_scraped"] + 1
            end_page_id = pagination_coin_d["total_pages"] + 1
            for page_id in range(start_page_id, end_page_id, 1):

                path_uri = pagination_coin_d["coin_canonical_uri"] + f"?page={page_id}"
                response = requests.get(path_uri)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "lxml")

                data_insert = ScrapeOcre.SCHEMA_RAW_URI_PAGES.copy()
                data_insert.pop("raw_uri_id")
                data_insert["coin_id"] = pagination_coin_d["coin_id"]
                data_insert["page_html"] = str(soup)
                data_insert["has_examples"] = True
                data_insert["has_examples_pagination"] = True
                data_insert["examples_pagination_id"] = page_id
                data_insert["examples_total_pagination"] = pagination_coin_d[
                    "total_pages"
                ]

                data_examples_ids = (
                    soup.find("div", class_="row", id="examples")
                    .find_all("div", class_="col-md-12")[1]
                    .find("div", class_="col-md-6")
                    .text.strip()
                    .split()[1::2]
                )
                (
                    data_insert["examples_start_id"],
                    data_insert["examples_end_id"],
                    data_insert["examples_max_id"],
                ) = data_examples_ids

                # Insert data into database
                path_insert = c.SQL_FOLDER / "insert" / "raw_uri_pages.sql"
                self._insert_using_secondary_client(path_insert, [data_insert])

        print("Finished scraping URIs with pagination...")
        return None

    def process_canonical_uris(self) -> None:
        """Process raw_uri_pages records to populate records in
        stg_coins, stg_examples, stg_examples_images, and stg_uri_pages
        tables."""

        print("Start processing canonical URI data...")
        print("Querying raw_uri_pages table...")
        path_query = c.SQL_FOLDER / "query" / "raw_uri_pages_with_path.sql"
        self.client.query_data(path_query)

        num_rows = self.client.cur.rowcount
        print(f"Processing {num_rows:6,d} rows of data...")

        examples_id = int()  # For stg_examples table
        for row in self.client.cur:
            data_query = ScrapeOcre.SCHEMA_RAW_URI_PAGES.copy()
            ScrapeOcre.populate_raw_uri_pages_schema(data_query, row)
            data_query["path_uri"] = row[-1]
            self._print_scrape_update_periodically(
                coin_id=data_query["coin_id"], interval=1_000
            )

            # >>> DEBUG >>>
            # delme_data_query = data_query.copy()
            # delme_data_query.pop("page_html")
            # print("RAW_URI_PAGES DATA:")
            # pprint(delme_data_query)
            # <<< DEBUG <<<

            soup = BeautifulSoup(data_query["page_html"], "lxml")

            # >>> Populate stg_coins >>>
            if data_query["examples_pagination_id"] in (None, 1):
                data_coins = ScrapeOcre.SCHEMA_STG_COINS.copy()
                data_coins["coin_id"] = data_query["coin_id"]
                data_coins["has_examples"] = data_query["has_examples"]
                data_coins["has_examples_pagination"] = data_query[
                    "has_examples_pagination"
                ]
                data_coins["coin_name"] = re.sub(
                    " +",
                    " ",
                    soup.find("h1", id="object_title").text.strip().replace("\n", ""),
                )

                # Populate Typological data
                soup_typological = soup.find("div", class_="metadata_section")

                if soup_typological:
                    # There is a typological section
                    data_coins["has_typological"] = True

                    raw_typo_data = soup_typological.ul
                    raw_typo_data = [item for item in raw_typo_data if item.name]
                    for item in raw_typo_data:
                        all_item_tags = [i for i in item if i.name]

                        if all_item_tags[0].name == "b":
                            result_split = item.text.strip().split(": ", maxsplit=1)
                            if len(result_split) == 1:
                                field = result_split[0].replace(":", "")
                                value = None
                            else:
                                field, value = result_split
                                value = re.sub(" +", " ", value.replace("\n", " "))
                            field = field.lower().replace(" ", "_")

                            # Modify field name and save field and value to dict
                            self._add_field_value_to_dict(data_coins, field, value)

                            # >>> DEBUG >>>
                            # print(f"{field}\t\t{value}")
                            # <<< DEBUG <<<
                        elif all_item_tags[0].name == "h4":
                            section_name = (
                                all_item_tags[0].text.strip().lower().replace(" ", "_")
                            )
                            for li in item.find_all("li"):
                                if li.contents[0].name == "b":
                                    field = (
                                        li.contents[0]
                                        .text.strip()
                                        .lower()
                                        .replace(" ", "_")
                                        .replace(":", "")
                                    )
                                    field = section_name + "_" + field

                                    if "symbol" in field:
                                        value = re.sub(
                                            " +",
                                            " ",
                                            li.text.strip()
                                            .replace(" - ", " ")
                                            .replace(",", "", 1),
                                        )
                                        value = value.replace("Symbol: ", "")
                                    else:
                                        value = (
                                            re.sub(
                                                " +",
                                                " ",
                                                li.contents[1]
                                                .text.strip()
                                                .replace("\n", " "),
                                            )
                                            if len(li.contents) > 1
                                            else None
                                        )

                                    # Modify field name and save field
                                    # and value to dict
                                    self._add_field_value_to_dict(
                                        data_coins, field, value
                                    )

                                    # >>> DEBUG >>>
                                    # print(f"{field}\t\t{value}")
                                    # <<< DEBUG <<<
                else:
                    # There is not a typological section
                    # Remaining fields are None by default, therefore,
                    # they do not need to be updated in this clause.
                    data_coins["has_typological"] = False

                # Populate Analysis data
                soup_analysis = soup.find("div", class_="row", id="metrical")
                if soup_analysis:
                    # There is an analysis section
                    soup_analysis_data = soup_analysis.find(
                        "dl", class_="dl-horizontal"
                    )

                    if soup_analysis_data:
                        # Analysis section has fields
                        data_coins["has_analysis"] = True
                        all_dt = soup_analysis_data.find_all("dt")
                        all_dd = soup_analysis_data.find_all("dd")
                        for dt, dd in zip(all_dt, all_dd):
                            field = dt.text.strip().lower().replace(" ", "")
                            field = "average_" + field
                            value = float(dd.text.strip())
                            if field in data_coins.keys():
                                data_coins[field] = value
                            else:
                                # This clause should not trigger for the state
                                # of the OCRE database as of July 2023, but it
                                # is written in case the database changes in the
                                # future.
                                raise KeyError(
                                    f"KEY ERROR: Field `{field}` is not in `data_coins`"
                                    + f"dict for coin #{data_coins['coin_id']} with "
                                    + f"raw_uri_id #{data_query['raw_uri_id']} at URI "
                                    + f"{data_query['path_uri']}!"
                                )
                    else:
                        # Analysis section does not have fields
                        # Remaining fields are None by default, therefore,
                        # they do not need to be updated in this clause.
                        data_coins["has_analysis"] = False
                else:
                    # There is not an analysis section
                    # Remaining fields are None by default, therefore,
                    # they do not need to be updated in this clause.
                    data_coins["has_analysis"] = False

                # >>> DEBUG >>>
                # print(f"Coin #{data_query['coin_id']} at {data_query['path_uri']}:")
                # pprint(data_coins)
                # print()
                # <<< DEBUG <<<

                # Insert stg_coins data
                path_insert_coins = c.SQL_FOLDER / "insert" / "stg_coins.sql"
                # TODO: Uncomment
                # self._insert_using_secondary_client(path_insert_coins, [data_coins])
            # <<< Populate stg_coins <<<

            # Populate stg_examples and stg_examples_images
            if data_query["has_examples"] == True:
                soup_examples = soup.find("div", class_="row", id="examples").find_all(
                    "div", class_="g_doc col-md-4"
                )
                examples_ids = range(
                    data_query["examples_start_id"],
                    data_query["examples_end_id"] + 1,
                    1,
                )
                for idx, soup_example in zip(examples_ids, soup_examples):
                    data_examples = ScrapeOcre.SCHEMA_STG_EXAMPLES.copy()
                    examples_id += 1
                    data_examples["examples_id"] = examples_id
                    data_examples["coin_id"] = data_query["coin_id"]

                    coin_title = soup_example.find(
                        "span", class_="result_link"
                    ).text.strip()
                    data_examples["uri_page_examples_id"] = idx
                    data_examples["example_name"] = coin_title

                    soup_example_fields = soup_example.find(
                        "dl", class_="dl-horizontal"
                    )
                    if soup_example_fields:
                        # Example has fields
                        data_examples["has_fields_section"] = True

                        fields = soup_example_fields.find_all("dt")
                        values = soup_example_fields.find_all("dd")
                        for field, value in zip(fields, values):
                            f = field.text.strip().lower().replace(" ", "_")
                            v = value.text.strip().replace("\n", " ")
                            v = re.sub(" +", " ", v)
                            if f not in ScrapeOcre.EXAMPLES_FIELDS_CONVERSION.keys():
                                # This clause should not trigger for the state
                                # of the OCRE database as of July 2023, but it
                                # is written in case the database changes in the
                                # future.
                                raise KeyError(
                                    f"Field `{f}` not found in {coin_title} "
                                    + f"(Example ID#{idx}) on URI page "
                                    + f"{data_query['path_uri']}"
                                )
                            else:
                                f = ScrapeOcre.EXAMPLES_FIELDS_CONVERSION[f]
                                if f in ScrapeOcre.STG_EXAMPLES_NUMERIC_FIELDS:
                                    v = float(v)
                                data_examples[f] = v
                    else:
                        # Example does not have fields
                        # Remaining fields are None by default, therefore,
                        # they do not need to be updated in this clause.
                        data_examples["has_fields_section"] = False

                    soup_examples_images = soup_example.find(
                        "div", class_="gi_c"
                    ).find_all("a")
                    num_tags = len(soup_examples_images)
                    # >>> DEBUG >>>
                    print(f"Example #{idx:2,d} {coin_title}")
                    print(data_examples["collection_name"])
                    print(f"Number of links: {num_tags}")
                    print(
                        f"IIIF?: {data_examples['collection_name'] in ScrapeOcre.COLLECTIONS_WITH_IIIF}"
                    )
                    # <<< DEBUG <<<
                    data_images_list = list()
                    if soup_examples_images:
                        # Example may have links in the image section
                        # (there are exceptions depending on collection
                        # name)

                        # NOTES:
                        # For current example coin
                        # Collection name from data_examples["collection_name"]
                        # Example image links in soup_examples_images
                        collection_name = data_examples["collection_name"]
                        if collection_name not in ScrapeOcre.COLLECTIONS_WITH_IIIF:
                            # Not a collection that uses IIIF

                            if collection_name not in (
                                "Museu Arqueològic de Llíria",
                                "Museu de Prehistòria de València",
                            ):
                                # Not a Spanish museum that redirects to main page
                                data_examples["has_links_section"] = True

                                for tag in soup_examples_images:
                                    data_images = (
                                        ScrapeOcre.SCHEMA_STG_EXAMPLES_IMAGES.copy()
                                    )
                                    data_images.pop("examples_images_id")
                                    data_images["stg_examples_id"] = examples_id

                                    title = tag["title"].lower()
                                    if "obverse" in title and "reverse" in title:
                                        data_images["image_type"] = "both sides"
                                    elif "obverse" in title:
                                        data_images["image_type"] = "obverse"
                                    elif "reverse" in title:
                                        data_images["image_type"] = "reverse"
                                    else:
                                        data_images["image_type"] = "unknown"

                                    data_images["link"] = tag["href"]

                                    data_images_list.append(data_images)
                            else:
                                # A Spanish museum that redirects to main page
                                data_examples["has_links_section"] = False
                        else:
                            # A collection that may use IIIF

                            if collection_name == "Harvard Art Museums":
                                # Unable to scrape due to full image links
                                # not having a logical conversion from
                                # sample image links.
                                pass
                            elif collection_name == "American Numismatic Society":
                                pass
                            elif collection_name == "Bibliothèque nationale de France":
                                pass
                            elif collection_name == "British Museum":
                                # Some British Museum images us IIIF and
                                # others do not.
                                pass
                            elif collection_name in ScrapeOcre.MAINZ_CITY_LIKE_LINKS:
                                pass
                            elif collection_name == "J. Paul Getty Museum":
                                pass
                            elif collection_name == "The Fralin Museum of Art":
                                pass
                            elif collection_name == "University of Graz":
                                pass

                        # >>> OLD >>>
                        # # Example has links in the image section
                        # data_examples["has_links_section"] = True

                        # for tag in soup_examples_images:
                        #     # >>> DEBUG >>>
                        #     print(tag["title"])
                        #     # <<< DEBUG <<<

                        #     data_images = ScrapeOcre.SCHEMA_STG_EXAMPLES_IMAGES.copy()
                        #     data_images.pop("examples_images_id")
                        #     data_images["stg_examples_id"] = examples_id

                        #     if (
                        #         data_examples["collection_name"]
                        #         in ScrapeOcre.COLLECTIONS_WITH_IIIF
                        #     ):
                        #         # IIIF collection
                        #         pass
                        #     else:
                        #         # Non-IIIF collection
                        #         if data_examples["collection_name"] not in (
                        #             "Museu Arqueològic de Llíria",
                        #             "Museu de Prehistòria de València",
                        #         ):
                        #             title = tag["title"].lower()
                        #             if "obverse" in title and "reverse" in title:
                        #                 data_images["image_type"] = "both sides"
                        #             elif "obverse" in title:
                        #                 data_images["image_type"] = "obverse"
                        #             elif "reverse" in title:
                        #                 data_images["image_type"] = "reverse"
                        #             else:
                        #                 data_images["image_type"] = "unknown"

                        #             data_images["link"] = tag["href"]

                        #             data_images_list.append(data_images)
                        #         else:
                        #             # Update has_links_section field and
                        #             # do not write to stg_examples_images
                        #             # table
                        #             data_examples["has_links_section"] = False
                        #             del data_images
                        # <<< OLD <<<
                    else:
                        # Example does not have links in the image section
                        # Do not write to stg_examples_images table
                        data_examples["has_links_section"] = False

                    # >>> DEBUG >>>
                    # print(f"Example #{idx:2,d} {coin_title}")
                    # pprint(data_examples)
                    print(
                        f"Image has links section?: {data_examples['has_links_section']}"
                    )
                    # TODO: Check if data_images_list is empty before writing to database
                    pprint(data_images_list)
                    print()
                    # <<< DEBUG <<<

                    path_insert_examples = c.SQL_FOLDER / "insert" / "stg_examples.sql"
                    # TODO: uncommit line
                    # self._insert_using_secondary_client(
                    #     path_insert_examples, [data_examples]
                    # )

            # Populate stg_uri_pages
            data_pages = ScrapeOcre.SCHEMA_STG_URI_PAGES.copy()
            data_pages["uri_page_id"] = data_query["raw_uri_id"]
            data_pages["coin_id"] = data_query["coin_id"]
            data_pages["examples_pagination_id"] = data_query["examples_pagination_id"]
            data_pages["examples_total_pagination"] = data_query[
                "examples_total_pagination"
            ]
            data_pages["examples_start_id"] = data_query["examples_start_id"]
            data_pages["examples_end_id"] = data_query["examples_end_id"]
            data_pages["examples_max_id"] = data_query["examples_max_id"]
            data_pages["uri_link"] = data_query["path_uri"]

            path_insert_pages = c.SQL_FOLDER / "insert" / "stg_uri_pages.sql"
            # TODO: Uncomment line
            # self._insert_using_secondary_client(path_insert_pages, [data_pages])

            # >>> DEBUG >>>
            break
            # if self.client.cur.rownumber > 50:
            #     break
            # <<< DEBUG <<<

        print("Finished processing canonical URI data...")
        return None

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

    def _add_field_value_to_dict(
        self, data_dict: dict, field_name: str, field_value: str
    ) -> None:
        """Convert field name to schema field name and add field name
        and value to data_dict in place for typological fields."""
        mod_field = ScrapeOcre.TYPOLOGICAL_FIELD_CONVERSION[field_name]

        if mod_field in ScrapeOcre.TYPOLOGICAL_ARRAY_FIELDS:
            if data_dict[mod_field] == None:
                data_dict[mod_field] = [field_value]
            else:
                data_dict[mod_field].append(field_value)
        else:
            data_dict[mod_field] = field_value

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
    # ScrapeOcre.try_except_with_retry(pipeline.scrape_canonical_uris)

    # Scrape raw Canonical URI pages with pagination
    # ScrapeOcre.try_except_with_retry(pipeline.scrape_uris_pagination)

    # Process Canonical URI pages
    pipeline.process_canonical_uris()

    # Disconnect
    pipeline.disconnect_from_database()
