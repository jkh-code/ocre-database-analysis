DROP TABLE IF EXISTS stg_web_scrape.stg_processed_issuer_names;
CREATE TABLE stg_web_scrape.stg_processed_issuer_names AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , c.issuer_name
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
        , CAST(issuer_name AS VARCHAR) AS issuer_name
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE issuer_name IS NULL
    UNION ALL
        SELECT
        coin_id
        , coin_name
        , UNNEST(issuer_name) AS issuer_name
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE issuer_name IS NOT NULL
)
SELECT
    coin_id
    , coin_name
    , CASE
        WHEN issuer_name IS NULL THEN 'Uncertain'
        ELSE issuer_name
    END AS issuer_name
    , CASE
        WHEN issuer_name IS NULL THEN TRUE
        ELSE FALSE
    END AS is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM combined_data;
