DROP TABLE IF EXISTS fnd_web_scrape.fnd_denominations;
CREATE TABLE fnd_web_scrape.fnd_denominations AS
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
SELECT
    coin_id
    , coin_name
    , CASE
        WHEN denomination IS NULL THEN 'Uncertain'
        WHEN denomination = 'Uncertain value' THEN 'Uncertain'
        ELSE denomination
    END AS denomination
    , CASE
        WHEN denomination IS NULL THEN TRUE
        WHEN denomination = 'Uncertain value' THEN TRUE
        WHEN denomination LIKE '%uncertain%' THEN TRUE
        ELSE FALSE
    END AS is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM raw_stg_coins_data
WHERE denomination != 'Medallion';
