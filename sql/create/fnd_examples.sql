-- DROP TABLE IF EXISTS fnd_web_scrape.fnd_examples;
-- CREATE TABLE fnd_web_scrape.fnd_examples AS
WITH images_data AS (
    SELECT
        examples_id
        , COUNT(examples_id) AS num_images
    FROM stg_web_scrape.stg_processed_examples_images
    GROUP BY examples_id
)
SELECT
    e.examples_id
    , e.coin_id
    , e.coin_name
    , e.coin_uri_link
    , e.uri_page_examples_id
    , e.example_name
    , e.example_uri_link
    , e.has_fields_section
    , e.has_links_section
    , e.coin_axis
    , e.collection_name
    , e.coin_diameter
    , e.identifier
    , e.coin_weight
    , COALESCE(i.num_images, 0) AS num_images
    , CURRENT_TIMESTAMP AS ts
FROM
    stg_web_scrape.stg_processed_examples AS e
    LEFT JOIN images_data AS i
    ON e.examples_id = i.examples_id;
