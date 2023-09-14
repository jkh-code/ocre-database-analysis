DROP TABLE IF EXISTS fnd_web_scrape.fnd_examples_images;
CREATE TABLE fnd_web_scrape.fnd_examples_images AS
SELECT
    examples_images_id
    , coin_id
    , coin_name
    , coin_uri_link
    , examples_id
    , example_name
    , example_uri_link
    , collection_name
    , identifier
    , image_type
    , image_link
    , CURRENT_TIMESTAMP AS ts
FROM stg_web_scrape.stg_processed_examples_images;
