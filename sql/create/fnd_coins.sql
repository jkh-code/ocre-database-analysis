-- DROP TABLE IF EXISTS fnd_web_scrape.fnd_coins;
-- CREATE TABLE fnd_web_scrape.fnd_coins AS
SELECT
    c.coin_id
    , c.coin_name
    , c.has_typological
    , c.has_examples
    , c.has_examples_pagination
    , c.has_analysis
    , c.coin_date_range
    , REPLACE(u.uri_link, '?page=1', '') AS coin_uri_link
FROM
    stg_web_scrape.stg_coins AS c
    LEFT JOIN stg_web_scrape.stg_uri_pages AS u
    ON c.coin_id = u.coin_id
    AND (u.examples_pagination_id = 1 OR u.examples_pagination_id IS NULL);
