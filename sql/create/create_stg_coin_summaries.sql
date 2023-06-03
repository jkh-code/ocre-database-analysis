DROP TABLE IF EXISTS stg_web_scrape.stg_coin_summaries;
CREATE TABLE stg_web_scrape.stg_coin_summaries (
    coin_id SERIAL NOT NULL,
    page_id INT NOT NULL,
    coin_name VARCHAR NOT NULL,
    coin_canonical_uri VARCHAR NULL,
    coin_date_string VARCHAR NULL,
    denomination VARCHAR NULL,
    mint VARCHAR NULL,
    obverse_description TEXT NULL,
    reverse_description TEXT NULL,
    reference VARCHAR[] NULL,
    num_objects_found INT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (coin_id)
);
