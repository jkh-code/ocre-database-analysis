DROP TABLE IF EXISTS fnd_web_scrape.fnd_authority_names;
CREATE TABLE fnd_web_scrape.fnd_authority_names AS
SELECT
    coin_id
    , coin_name
    , authority_name
    , is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM stg_web_scrape.stg_processed_authority_names;
