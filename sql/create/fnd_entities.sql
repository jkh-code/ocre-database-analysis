DROP TABLE IF EXISTS fnd_web_scrape.fnd_entities;
CREATE TABLE fnd_web_scrape.fnd_entities AS
SELECT
    coin_id
    , coin_name
    , face
    , entity_type
    , entity_name
    , is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM stg_web_scrape.stg_processed_entities;
