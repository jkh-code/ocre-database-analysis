DROP TABLE IF EXISTS stg_web_scrape.stg_processed_denominations;
CREATE TABLE stg_web_scrape.stg_processed_denominations AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , c.denomination
        , REPLACE(u.uri_link, '?page=1', '') AS coin_uri_link
    FROM
        stg_web_scrape.stg_coins AS c
        LEFT JOIN stg_web_scrape.stg_uri_pages AS u
        ON c.coin_id = u.coin_id
        AND (u.examples_pagination_id = 1 OR u.examples_pagination_id IS NULL)
),
combined_data AS (
    /* Unnesting does not retain null rows, therefore, unioning null
    rows to unnested rows to retain all records from
    raw_stg_coins_data. */
    SELECT
        coin_id
        , coin_name
        , CAST(denomination AS VARCHAR) AS denomination
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE denomination IS NULL
    UNION ALL
    SELECT
        coin_id
        , coin_name
        , UNNEST(denomination) AS denomination
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE denomination IS NOT NULL
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
FROM combined_data
WHERE denomination != 'Medallion' OR denomination IS NULL;
