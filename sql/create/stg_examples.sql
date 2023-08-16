DROP TABLE IF EXISTS stg_web_scrape.stg_examples;
CREATE TABLE stg_web_scrape.stg_examples (
    examples_id SERIAL NOT NULL,
    coin_id INTEGER NOT NULL,
    uri_page_examples_id INTEGER NOT NULL,
    example_name VARCHAR NOT NULL,
    has_fields_section BOOLEAN NOT NULL,
    has_links_section BOOLEAN NOT NULL,
    coin_axis NUMERIC NULL,
    collection_name VARCHAR NULL,
    coin_diameter NUMERIC NULL,
    findspot VARCHAR NULL,
    hoard VARCHAR NULL,
    identifier VARCHAR NULL,
    coin_weight NUMERIC NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (examples_id)
);
