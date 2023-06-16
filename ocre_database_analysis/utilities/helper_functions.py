def print_update_periodically(curr_row: int, total_rows: int, interval: int) -> None:
    """Print to console scraping update message at an interval of
    number of records."""
    if (curr_row in (1, total_rows)) or (curr_row % interval == 0):
        print(f"Scraping page {curr_row} of {total_rows}...")
    return None


if __name__ == "__main__":
    pass
