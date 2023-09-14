DROP TABLE IF EXISTS fnd_web_scrape.fnd_denominations;
CREATE TABLE fnd_web_scrape.fnd_denominations AS
SELECT
    coin_id
    , coin_name
    , denomination
    , is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP as ts
FROM stg_web_scrape.stg_processed_denominations;
