DROP TABLE IF EXISTS stg_web_scrape.stg_processed_examples;
CREATE TABLE stg_web_scrape.stg_processed_examples AS
SELECT
    c.coin_id
    , c.coin_name
    , REPLACE(u.uri_link, '?page=1', '') AS coin_uri_link
    , e.examples_id
    , e.uri_page_examples_id
    , e.example_name
    , u.uri_link AS example_uri_link
    , e.has_fields_section
    , e.has_links_section
    , e.coin_axis
    , e.collection_name
    , e.coin_diameter
    , e.identifier
    , e.coin_weight
FROM
    stg_web_scrape.stg_coins AS c
    INNER JOIN stg_web_scrape.stg_examples AS e
    ON c.coin_id = e.coin_id
    LEFT JOIN stg_web_scrape.stg_uri_pages AS u
    ON c.coin_id = u.coin_id
    AND e.uri_page_examples_id BETWEEN u.examples_start_id AND u.examples_end_id
