from bs4 import BeautifulSoup
import re
import sys

from pprint import pprint

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c
import ocre_database_analysis.utilities.helper_functions as hf
from ocre_database_analysis.scrape_ocre import ScrapeOcre


def connect_and_query(db_name: str, table_name: str) -> Topsy:
    """Connect to specified database, query specified table, and return
    the Topsy client, which contains the cursor and query results."""
    print("Start scraping of Browse results fields...")

    # Validate table names
    VALID_TABLE_NAMES = ("raw_browse_pages", "raw_uri_pages")
    if table_name not in VALID_TABLE_NAMES:
        raise ValueError(f"VALUE ERROR: `{table_name}` is not a valid table name!")

    # Try connection and return client
    client = Topsy.try_postgres_connection(db_name)

    # Loading data from postgres
    file_path = c.SQL_FOLDER / "query" / (table_name + ".sql")
    client.query_data(file_path)

    return client


def get_browse_fields(db_name: str) -> None:
    """Scrape browse fields and print to CSV file."""
    table_name = "raw_browse_pages"
    client = connect_and_query(db_name, table_name)

    # Determining unique Browse results fields
    unique_fields = dict()
    total_rows = client.cur.rowcount
    print("\nScraping fields from all results...")
    for row in client.cur:
        curr_row = client.cur.rownumber
        hf.print_update_periodically(curr_row, total_rows, 250)

        soup = BeautifulSoup(row[4], "lxml")
        all_page_coins = soup.find_all("div", class_="row result-doc")
        for coin in all_page_coins:
            left_data = coin.find("div", class_="col-md-7 col-lg-8").contents[0]
            all_dt = left_data.find_all("dt")
            for dt in all_dt:
                dt_ = dt.text.strip()
                if dt_ not in unique_fields.keys():
                    unique_fields[dt_] = curr_row

    # Saving unique fields to text file
    sorted_unique_fields = sorted(
        unique_fields.items(), reverse=False, key=lambda x: x[0].lower()
    )
    print(
        f"\nFound the following unique fields on the following pages: {sorted_unique_fields}"
    )
    path_save_results = c.DATA_FOLDER / "unique_browse_fields.csv"
    print(f"Writing results to file {path_save_results}")
    with open(path_save_results, "w", encoding="UTF-8") as f:
        f.write("item, first_page_seen, api_start_value\n")
        for field, page_num in sorted_unique_fields:
            api_start_value = (page_num - 1) * 20
            f.write(f'"{field}", {page_num}, {api_start_value}\n')

    client.close_connection()

    return None


def get_unique_object_counts(db_name: str) -> None:
    """Scrape object counts and print to text file."""
    table_name = "raw_browse_pages"
    client = connect_and_query(db_name, table_name)

    # Determining unique object count strings
    unique_text_d = dict()
    total_rows = client.cur.rowcount
    print("\nScraping object counts from all pages...")
    for row in client.cur:
        curr_row = client.cur.rownumber
        hf.print_update_periodically(curr_row, total_rows, 250)

        soup = BeautifulSoup(row[4], "lxml")
        all_page_coins = soup.find_all("div", class_="row result-doc")
        for coin in all_page_coins:
            right_text = coin.find(
                "div", class_="col-md-5 col-lg-4 pull-right"
            ).text.strip()
            if right_text:
                if right_text not in unique_text_d.keys():
                    unique_text_d[right_text] = curr_row

    # Saving data to file
    sorted_unique_fields = sorted(
        unique_text_d.items(), reverse=False, key=lambda x: x[0].lower()
    )
    print(f"\nFound {len(sorted_unique_fields)} unique object count text...")
    path_save_results = c.DATA_FOLDER / "unique_object_count_text.txt"
    print(f"Writing results to file {path_save_results}")
    with open(path_save_results, "w", encoding="UTF-8") as f:
        for text, page_num in sorted_unique_fields:
            api_start_value = (page_num - 1) * 20
            f.write(
                f'"{text}", first appears on page {page_num} at API start '
                + f"value of {api_start_value}\n"
            )

    client.close_connection()
    return None


def get_uri_header_sections(db_name: str) -> None:
    """Scrape the sections line from URI headers to determine all possible variations."""
    print("Scraping header section of URI pages...")
    table_name = "raw_uri_pages"
    client = connect_and_query(db_name, table_name)

    number_of_header_lines_d = dict()
    unique_section_text_d = dict()
    unique_sections_d = dict()
    total_rows = client.cur.rowcount
    for row in client.cur:
        curr_row = client.cur.rownumber
        curr_coin_id = row[0]
        hf.print_update_periodically(curr_row, total_rows, 1_000)

        soup = BeautifulSoup(row[1], "lxml")
        soup_header = soup.body.find_all("div", class_="col-md-12")[1].contents
        all_tags = [item for item in soup_header if item.name]
        path_uri = all_tags[1].find("a")["href"]

        # Check number of tags in header
        num_tags = len(all_tags)
        if num_tags not in number_of_header_lines_d.keys():
            number_of_header_lines_d[num_tags] = (curr_coin_id, path_uri)

        # Check section text
        section_text = all_tags[2].text.strip().replace("\n", "")
        section_text = re.sub(" +", " ", section_text)
        if section_text not in unique_section_text_d.keys():
            unique_section_text_d[section_text] = (curr_coin_id, path_uri)

        # Check section names
        section_names = [
            " ".join(item.split()) for item in all_tags[2].text.split(" | ")
        ]
        for section in section_names:
            if section not in unique_sections_d:
                unique_sections_d[section] = (curr_coin_id, path_uri)

    # Saving to files
    print("Writing data to files...")
    files = (
        "number_of_header_lines.txt",
        "unique_section_text.txt",
        "unique_sections.txt",
    )
    data_structures = (
        number_of_header_lines_d,
        unique_section_text_d,
        unique_sections_d,
    )
    for d, f in zip(data_structures, files):
        path_ = c.DATA_FOLDER / f
        sorted_ = sorted(d.items(), reverse=False, key=lambda x: str(x[0]).lower())
        with open(path_, "w", encoding="UTF-8") as f:
            for k, v in sorted_:
                f.write(f'"{k}" first appears on coin_id {v[0]} at URI {v[1]}\n')

    client.close_connection()
    return None


def integrate_field_value_pairs(
    field: str,
    value: str,
    row_field_counts_d: dict,
    unique_fields_d: dict,
    curr_coin_id: int,
    path_uri: str,
) -> None:
    """Integrating field and value pairs into the row_field_counts_d
    and unique_field_d dicts in place."""
    # Incrementing row_field_counts_d
    if field not in row_field_counts_d.keys():
        row_field_counts_d[field] = 1
    else:
        row_field_counts_d[field] += 1

    # Adding field to unique_fields_d
    if field not in unique_fields_d.keys():
        unique_fields_d[field] = (value, curr_coin_id, path_uri)
    else:
        if row_field_counts_d[field] > 1:
            key_idx = row_field_counts_d[field]
            unique_fields_d[field + f"{key_idx}"] = (
                value,
                curr_coin_id,
                path_uri,
            )

    return None


def get_uri_typological_fields(db_name: str) -> None:
    """Scrape the fields of the Typological Description section."""
    print("Scraping fields of the Typological Description section...")
    table_name = "raw_uri_pages"
    client = connect_and_query(db_name, table_name)

    # Determining unique typological fields
    unique_fields_d = dict()
    total_rows = client.cur.rowcount
    print("Scraping fields from all results...")
    for row in client.cur:
        curr_row = client.cur.rownumber
        curr_coin_id = row[0]
        curr_coin_html = row[1]
        hf.print_update_periodically(curr_row, total_rows, 1_000)

        soup = BeautifulSoup(curr_coin_html, "lxml")
        path_uri = hf.retrieve_uri_path(soup)

        soup_typological = soup.find("div", class_="metadata_section")
        data_raw = soup_typological.ul
        data_raw = [item for item in data_raw if item.name]

        curr_subsection = str()
        row_field_counts_d = dict()
        for item in data_raw:
            all_item_tags = [i for i in item if i.name]

            if all_item_tags[0].name == "b":
                result_split = item.text.strip().split(": ", maxsplit=1)
                if len(result_split) == 1:
                    field = result_split[0].replace(":", "")
                    value = None
                else:
                    field, value = result_split
                    value = re.sub(" +", " ", value.replace("\n", " "))
                field = "_" + field.lower().replace(" ", "_")

                integrate_field_value_pairs(
                    field,
                    value,
                    row_field_counts_d,
                    unique_fields_d,
                    curr_coin_id,
                    path_uri,
                )
            elif all_item_tags[0].name == "h4":
                section_name = all_item_tags[0].text.strip().lower().replace(" ", "_")
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
                                li.text.strip().replace(" - ", " ").replace(",", "", 1),
                            )
                            value = value.replace("Symbol: ", "")
                        else:
                            value = (
                                re.sub(
                                    " +",
                                    " ",
                                    li.contents[1].text.strip().replace("\n", " "),
                                )
                                if len(li.contents) > 1
                                else None
                            )

                        integrate_field_value_pairs(
                            field,
                            value,
                            row_field_counts_d,
                            unique_fields_d,
                            curr_coin_id,
                            path_uri,
                        )

    # Saving to file
    print("Saving unique fields to file...")
    path_save = c.DATA_FOLDER / "unique_typological_fields.txt"
    sorted_keys = sorted(
        unique_fields_d.items(), reverse=False, key=lambda x: str(x[0]).lower()
    )
    with open(path_save, "w", encoding="UTF-8") as f:
        for k, v in sorted_keys:
            f.write(
                f'Key "{k}" first appears on coin_id {v[1]} with value "{v[0]}" at URI {v[2]}.\n'
            )

    client.close_connection()
    return None


def get_uri_analysis_fields(db_name: str) -> None:
    """Scrape fields in the Quantitative Analysis section of URI pages."""
    print("Scraping fields of the Quantitative Analysis section...")
    table_name = "raw_uri_pages"
    client = connect_and_query(db_name, table_name)

    unique_fields_d = dict()
    total_rows = client.cur.rowcount
    print("Scraping fields from all results...")
    for row in client.cur:
        curr_row = client.cur.rownumber
        curr_coin_id = row[0]
        curr_coin_html = row[1]
        hf.print_update_periodically(curr_row, total_rows, 1_000)

        soup = BeautifulSoup(curr_coin_html, "lxml")
        path_uri = hf.retrieve_uri_path(soup)

        soup_analysis = soup.find("div", class_="row", id="metrical")
        if not soup_analysis:
            # When there is not a Quantitative Analysis section
            continue
        soup_data = soup_analysis.find("dl", class_="dl-horizontal")
        if not soup_data:
            # When there is not data in the Quantitative Analysis section
            continue

        all_dt = soup_data.find_all("dt")
        all_dd = soup_data.find_all("dd")
        row_field_counts_d = dict()
        for dt, dd in zip(all_dt, all_dd):
            field = dt.text.strip().lower().replace(" ", "_")
            field = "average_" + field
            value = float(dd.text.strip())

            integrate_field_value_pairs(
                field,
                value,
                row_field_counts_d,
                unique_fields_d,
                curr_coin_id,
                path_uri,
            )

    # Saving to file
    print("Saving unique fields to file...")
    path_save = c.DATA_FOLDER / "unique_analysis_fields.txt"
    sorted_keys = sorted(
        unique_fields_d.items(), reverse=False, key=lambda x: str(x[0]).lower()
    )
    with open(path_save, "w", encoding="UTF-8") as f:
        for k, v in sorted_keys:
            f.write(
                f'Key "{k}" first appears on coin ID #{v[1]} with value "{v[0]}" at URI {v[2]}.\n'
            )

    client.close_connection()
    return None


def get_uri_examples_fields(db_name: str) -> None:
    """Scrape fields in the example section of URI pages."""
    print("Scraping data from URI examples sections...")
    # Using specific raw_uri_pages query file name as table name
    query_file_name = "raw_uri_pages_has_examples"
    client = Topsy.try_postgres_connection(db_name)
    file_path = c.SQL_FOLDER / "query" / (query_file_name + ".sql")
    client.query_data(file_path)

    unique_fields_d = dict()
    total_rows = client.cur.rowcount
    for row in client.cur:
        query_data = ScrapeOcre.SCHEMA_RAW_URI_PAGES.copy()
        ScrapeOcre.populate_raw_uri_pages_schema(query_data, row)
        query_data["path_uri"] = row[10]

        curr_row = client.cur.rownumber
        hf.print_update_periodically(curr_row, total_rows, 1_000)

        soup = BeautifulSoup(query_data["page_html"], "lxml")
        soup_examples_section = soup.find("div", class_="row", id="examples")
        soup_examples = soup_examples_section.find_all("div", class_="g_doc col-md-4")

        for soup_example in soup_examples:
            soup_example_title = soup_example.find("span", class_="result_link")
            example_title = soup_example_title.text.strip()

            soup_fields = soup_example.find("dl", class_="dl-horizontal")
            fields, values = soup_fields.find_all("dt"), soup_fields.find_all("dd")

            example_field_count_d = dict()
            for field, value in zip(fields, values):
                f = field.text.strip().lower().replace(" ", "_")
                v = value.text.strip()

                if f not in example_field_count_d.keys():
                    example_field_count_d[f] = 1
                else:
                    example_field_count_d[f] += 1
                if f not in unique_fields_d.keys():
                    unique_fields_d[f] = (v, example_title, query_data["path_uri"])
                else:
                    if example_field_count_d[f] > 1:
                        key_idx = example_field_count_d[f]
                        unique_fields_d[f + f"{key_idx}"] = (
                            v,
                            example_title,
                            query_data["path_uri"],
                        )

        pprint(unique_fields_d)
        # >>> debug >>>
        break
        # <<< debug <<<

    # Saving to file
    print("Saving unique_fields to file...")
    path_save = c.DATA_FOLDER / "unique_examples_fields.txt"
    sorted_keys = sorted(
        unique_fields_d.items(), reverse=False, key=lambda x: str(x[0]).lower()
    )
    with open(path_save, "w", encoding="UTF-8") as f:
        for k, v in sorted_keys:
            f.write(
                f'Key "{k}" first appears on example "{v[1]}" with value "{v[0]}" at URI {v[2]}.\n'
            )

    client.close_connection()
    return None


if __name__ == "__main__":
    database_name = "ocre"

    # get_browse_fields(database_name)
    # get_unique_object_counts(database_name)

    # get_uri_header_sections(database_name)
    # get_uri_typological_fields(database_name)
    # get_uri_analysis_fields(database_name)
    get_uri_examples_fields(database_name)
