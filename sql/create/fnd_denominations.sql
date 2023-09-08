-- DROP TABLE IF EXISTS fnd_web_scrape.fnd_denomination;
-- CREATE TABLE fnd_web_scrape.fnd_denomination AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , UNNEST(c.denomination) AS denomination
        , REPLACE(u.uri_link, '?page=1', '') AS coin_uri_link
    FROM
        stg_web_scrape.stg_coins AS c
        LEFT JOIN stg_web_scrape.stg_uri_pages AS u
        ON c.coin_id = u.coin_id
        AND (u.examples_pagination_id = 1 OR u.examples_pagination_id IS NULL)
)
SELECT DISTINCT denomination
FROM raw_stg_coins_data
ORDER BY denomination;
