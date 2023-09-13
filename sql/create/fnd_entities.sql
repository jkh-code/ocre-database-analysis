DROP TABLE IF EXISTS stg_web_scrape.stg_processed_entities;
CREATE TABLE stg_web_scrape.stg_processed_entities AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , c.obverse_deity
        , c.obverse_portrait
        , c.reverse_deity
        , c.reverse_portrait
        , REPLACE(u.uri_link, '?page=1', '') AS coin_uri_link
    FROM
        stg_web_scrape.stg_coins AS c
        LEFT JOIN stg_web_scrape.stg_uri_pages AS u
        ON c.coin_id = u.coin_id
        AND (u.examples_pagination_id = 1 OR u.examples_pagination_id IS NULL)
),
raw_obverse_deity_data AS (
    SELECT
        coin_id
        , CAST(obverse_deity AS VARCHAR) AS obverse_deity
    FROM raw_stg_coins_data
    WHERE obverse_deity IS NULL
    UNION ALL
    SELECT
        coin_id
        , UNNEST(obverse_deity) AS obverse_deity
    FROM raw_stg_coins_data
    WHERE obverse_deity IS NOT NULL
),
processed_obverse_deity_data AS (
    SELECT
        coin_id
        , 'obverse' AS face
        , 'deity' AS entity_type
        , CASE
            WHEN obverse_deity LIKE '%Uncertain%' THEN 'Uncertain'
            ELSE obverse_deity
        END AS entity_name
        , CASE
            WHEN obverse_deity LIKE '%Uncertain%' THEN TRUE
            ELSE FALSE
        END AS is_uncertain
    FROM raw_obverse_deity_data
    WHERE obverse_deity IS NOT NULL
),
raw_obverse_portrait_data AS (
    SELECT
        coin_id
        , CAST(obverse_portrait AS VARCHAR) AS obverse_portrait
    FROM raw_stg_coins_data
    WHERE obverse_portrait IS NULL
    UNION ALL
    SELECT
        coin_id
        , UNNEST(obverse_portrait) AS obverse_portrait
    FROM raw_stg_coins_data
    WHERE obverse_portrait IS NOT NULL
),
processed_obverse_portrait_data AS (
    SELECT
        coin_id
        , 'obverse' AS face
        , 'portrait' AS entity_type
        , CASE
            WHEN obverse_portrait LIKE '%Uncertain%' THEN 'Uncertain'
            ELSE obverse_portrait
        END AS entity_name
        , CASE
            WHEN obverse_portrait LIKE '%Uncertain%' THEN TRUE
            ELSE FALSE
        END AS is_uncertain
    FROM raw_obverse_portrait_data
    WHERE obverse_portrait IS NOT NULL
),
raw_reverse_deity_data AS (
    SELECT
        coin_id
        , CAST(reverse_deity AS VARCHAR) AS reverse_deity
    FROM raw_stg_coins_data
    WHERE reverse_deity IS NULL
    UNION ALL
    SELECT
        coin_id
        , UNNEST(reverse_deity) AS reverse_deity
    FROM raw_stg_coins_data
    WHERE reverse_deity IS NOT NULL
),
processed_reverse_deity_data AS (
    SELECT
        coin_id
        , 'reverse' AS face
        , 'deity' AS entity_type
        , CASE
            WHEN reverse_deity LIKE '%Uncertain%' THEN 'Uncertain'
            ELSE reverse_deity
        END AS entity_name
        , CASE
            WHEN reverse_deity LIKE '%Uncertain%' THEN TRUE
            ELSE FALSE
        END AS is_uncertain
    FROM raw_reverse_deity_data
    WHERE reverse_deity IS NOT NULL
),
raw_reverse_portrait_data AS (
    SELECT
        coin_id
        , CAST(reverse_portrait AS VARCHAR) AS reverse_portrait
    FROM raw_stg_coins_data
    WHERE reverse_portrait IS NULL
    UNION ALL
    SELECT
        coin_id
        , UNNEST(reverse_portrait) AS reverse_portrait
    FROM raw_stg_coins_data
    WHERE reverse_portrait IS NOT NULL
),
processed_reverse_portrait_data AS (
    SELECT
        coin_id
        , 'reverse' AS face
        , 'portrait' AS entity_type
        , CASE
            WHEN reverse_portrait LIKE '%Uncertain%' THEN 'Uncertain'
            ELSE reverse_portrait
        END AS entity_name
        , CASE
            WHEN reverse_portrait LIKE '%Uncertain%' THEN TRUE
            ELSE FALSE
        END AS is_uncertain
    FROM raw_reverse_portrait_data
    WHERE reverse_portrait IS NOT NULL
),
combined_data AS (
    SELECT
        coin_id
        , face
        , entity_type
        , entity_name
        , is_uncertain
    FROM processed_obverse_deity_data
    UNION ALL
    SELECT
        coin_id
        , face
        , entity_type
        , entity_name
        , is_uncertain
    FROM processed_obverse_portrait_data
    UNION ALL
    SELECT
        coin_id
        , face
        , entity_type
        , entity_name
        , is_uncertain
    FROM processed_reverse_deity_data
    UNION ALL
    SELECT
        coin_id
        , face
        , entity_type
        , entity_name
        , is_uncertain
    FROM processed_reverse_portrait_data
)
SELECT
    coin.coin_id
    , coin.coin_name
    , comb.face
    , comb.entity_type
    , comb.entity_name
    , comb.is_uncertain
    , coin.coin_uri_link
FROM
    raw_stg_coins_data AS coin
    INNER JOIN combined_data AS comb
    ON coin.coin_id = comb.coin_id;
