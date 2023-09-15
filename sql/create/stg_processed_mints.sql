DROP TABLE IF EXISTS stg_web_scrape.stg_processed_mints;
CREATE TABLE stg_web_scrape.stg_processed_mints AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , c.mint
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
        , CAST(mint AS VARCHAR) AS mint
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE mint IS NULL
    UNION ALL
        SELECT
        coin_id
        , coin_name
        , UNNEST(mint) AS mint
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE mint IS NOT NULL
)
SELECT
    coin_id
    , coin_name
    , CASE
        WHEN mint IS NULL THEN 'Uncertain'
        WHEN mint LIKE '%Uncertain%' THEN 'Uncertain'
        ELSE mint
    END AS mint
    , CASE
        WHEN mint IS NULL THEN TRUE
        WHEN mint LIKE '%Uncertain%' THEN TRUE
        ELSE FALSE
    END AS is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM combined_data;
