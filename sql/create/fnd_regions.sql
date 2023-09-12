-- DROP TABLE IF EXISTS fnd_web_scrape.fnd_regions;
-- CREATE TABLE fnd_web_scrape.fnd_regions AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , c.region
        , REPLACE(u.uri_link, '?page=1', '') AS coin_uri_link
    FROM
        stg_web_scrape.stg_coins AS c
        LEFT JOIN stg_web_scrape.stg_uri_pages AS u
        ON c.coin_id = u.coin_id
        AND (u.examples_pagination_id = 1 OR u.examples_pagination_id IS NULL)
),
combined_data AS (
    SELECT
        coin_id
        , coin_name
        , CAST(region AS VARCHAR) AS region
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE region IS NULL
    UNION ALL
        SELECT
        coin_id
        , coin_name
        , UNNEST(region) AS region
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE region IS NOT NULL
)
SELECT
    coin_id
    , coin_name
    , CASE
        WHEN region IS NULL THEN 'Uncertain'
        WHEN region LIKE '%Uncertain%' THEN 'Uncertain'
        ELSE region
    END AS region
    , CASE
        WHEN region IS NULL THEN TRUE
        WHEN region LIKE '%Uncertain%' THEN TRUE
        ELSE FALSE
    END AS is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM combined_data;
