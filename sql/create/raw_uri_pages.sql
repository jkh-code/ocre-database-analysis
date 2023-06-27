DROP TABLE IF EXISTS raw_web_scrape.raw_uri_pages;
CREATE TABLE raw_web_scrape.raw_uri_pages (
    raw_uri_id SERIAL NOT NULL,
    coin_id INT NOT NULL,
    has_examples BOOLEAN NOT NULL,
    has_examples_pagination BOOLEAN NOT NULL,
    examples_pagination_id INT NULL,
    examples_total_pagination INT NULL,
    examples_start_id INT NULL,
    examples_end_id INT NULL,
    examples_max_id INT NULL,
    page_html TEXT NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (raw_uri_id)
);
