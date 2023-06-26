from bs4 import BeautifulSoup


def print_update_periodically(curr_row: int, total_rows: int, interval: int) -> None:
    """Print to console scraping update message at an interval of
    number of records."""
    if (curr_row in (1, total_rows)) or (curr_row % interval == 0):
        print(f"Scraping page {curr_row} of {total_rows}...")
    return None


def retrieve_uri_path(soup: BeautifulSoup) -> str:
    """Retrieve the canonical URI path from the HTML of a raw URI page."""
    soup_header = soup.body.find_all("div", class_="col-md-12")[1].contents
    all_header_tags = [item for item in soup_header if item.name]
    return all_header_tags[1].find("a")["href"]


if __name__ == "__main__":
    pass
