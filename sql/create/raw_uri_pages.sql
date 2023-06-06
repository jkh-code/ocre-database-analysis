DROP TABLE IF EXISTS raw_web_scrape.raw_uri_pages;
CREATE TABLE raw_web_scrape.raw_uri_pages (
    coin_id SERIAL NOT NULL,
    page_html TEXT NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (coin_id)
);
