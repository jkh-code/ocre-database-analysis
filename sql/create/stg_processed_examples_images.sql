DROP TABLE IF EXISTS stg_web_scrape.stg_processed_examples_images;
CREATE TABLE stg_web_scrape.stg_processed_examples_images AS
SELECT
    e.coin_id
    , e.coin_name
    , e.coin_uri_link
    , e.examples_id
    , e.example_name
    , e.example_uri_link
    , e.collection_name
    , e.identifier
    , i.examples_images_id
    , i.image_type
    , i.link AS image_link
    , CURRENT_TIMESTAMP AS ts
FROM
    stg_web_scrape.stg_processed_examples AS e
    INNER JOIN stg_web_scrape.stg_examples_images AS i
    ON e.examples_id = i.stg_examples_id;
