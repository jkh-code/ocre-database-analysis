DROP TABLE IF EXISTS fnd_web_scrape.fnd_mints;
CREATE TABLE fnd_web_scrape.fnd_mints AS
SELECT
    coin_id
    , coin_name
    , mint
    , is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM stg_web_scrape.stg_processed_mints;
