DROP TABLE IF EXISTS fnd_web_scrape.fnd_materials;
CREATE TABLE fnd_web_scrape.fnd_materials AS
SELECT
    coin_id
    , coin_name
    , material
    , is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM stg_web_scrape.stg_processed_materials;
