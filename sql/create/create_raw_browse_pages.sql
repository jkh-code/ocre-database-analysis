DROP TABLE IF EXISTS raw_web_scrape.raw_browse_pages;
CREATE TABLE raw_web_scrape.raw_browse_pages (
    page_id SERIAL,
    page_url VARCHAR NOT NULL,
    start_coin_id INTEGER NOT NULL,
    end_coin_id INTEGER NOT NULL,
    page_html TEXT NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (page_id)
);
