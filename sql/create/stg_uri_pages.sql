DROP TABLE IF EXISTS stg_web_scrape.stg_uri_pages;
CREATE TABLE stg_web_scrape.stg_uri_pages (
    uri_page_id INTEGER NOT NULL,
    coin_id INTEGER NOT NULL,
    examples_pagination_id INTEGER NULL,
    examples_total_pagination INTEGER NULL,
    examples_start_id INTEGER NULL,
    examples_end_id INTEGER NULL,
    examples_max_id INTEGER NULL,
    uri_link VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (uri_page_id)
);
