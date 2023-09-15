DROP TABLE IF EXISTS fnd_web_scrape.fnd_regions;
CREATE TABLE fnd_web_scrape.fnd_regions AS
SELECT
    coin_id
    , coin_name
    , region
    , is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM stg_web_scrape.stg_processed_regions;
