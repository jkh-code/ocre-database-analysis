DROP TABLE IF EXISTS ocre.raw_web_scrape.raw_browse_pages;
CREATE TABLE ocre.raw_web_scrape.raw_browse_pages (
    page_id SERIAL,
    page_url VARCHAR,
    start_coin_id INTEGER,
    end_coin_id INTEGER,
    page_html TEXT,
    ts TIMESTAMP,
    PRIMARY KEY (page_id)
);
