from bs4 import BeautifulSoup

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c


def connect_and_query(db_name: str, table_name: str) -> Topsy:
    """Connect to specified database, query specified table, and return
    the Topsy client, which contains the cursor and query results."""
    print("Start scraping of Browse results fields...")

    # Validate table names
    # TODO: Clean up valid_table_names tables
    valid_table_names = ("raw_browse_pages", "stg_coin_summaries")
    if table_name not in valid_table_names:
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

        if (curr_row in (1, total_rows)) or (curr_row % 250 == 0):
            print(f"Scraping page {curr_row} of {total_rows}...")

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

        if (curr_row in (1, total_rows)) or (curr_row % 250 == 0):
            print(f"Scraping page {curr_row} of {total_rows}...")

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
    # TODO: Correct table_name
    table_name = "stg_coin_summaries"
    client = connect_and_query(db_name, table_name)

    client.close_connection()
    return None


if __name__ == "__main__":
    database_name = "ocre"

    # get_browse_fields(database_name)
    # get_unique_object_counts(database_name)

    get_uri_header_sections(database_name)
