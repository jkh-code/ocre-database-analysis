import psycopg2 as pg2
from bs4 import BeautifulSoup
import sys

from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c


# TODO: Convert scrape_browse_results_fields() into scrape_browse_results(). This function will run the same code through "Loading data from postgres" section
# TODO: Move the contents from the "Determining unique Browse results fields" section to the end of the existing scrape_browse_results_fields() function into its own function, named get_browse_fields()
# TODO: Create a new function, named get_unique_object_count(), to return a unique list of the number of objects text from the browse pages
# TODO: write the scrape_browse_results() function to pass the cursor to the get_browse_fields() and get_unique_object_count() functions to perform their own tasks. --THIS WILL NOT WORK-- Once the cursor is exhausted it cannot be replenished. Then have two function "get" functions have scrape_browse_results() function in it.


def scrape_browse_results(db_name: str) -> Topsy:
    print("Start scraping of Browse results fields...")

    # Try connection and return client
    client = Topsy.try_postgres_connection(db_name)

    # Loading data from postgres
    file_path = c.SQL_FOLDER / "query" / "query_raw_browse_pages.sql"
    print(f"\nQuerying `{db_name}` using file {file_path}")
    with open(file_path, "r", encoding="UTF-8") as f:
        print(f"Reading file...")
        query = f.read()

    print("Querying database...")
    client.cur.execute(query)

    return client


def get_browse_fields(db_name: str) -> None:
    client = scrape_browse_results(db_name)

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
    pass


if __name__ == "__main__":
    get_browse_fields("ocre")
