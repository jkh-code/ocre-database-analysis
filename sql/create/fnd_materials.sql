DROP TABLE IF EXISTS fnd_web_scrape.fnd_materials;
CREATE TABLE fnd_web_scrape.fnd_materials AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , c.material
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
        , CAST(material AS VARCHAR) AS material
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE material IS NULL
    UNION ALL
        SELECT
        coin_id
        , coin_name
        , UNNEST(material) AS material
        , coin_uri_link
    FROM raw_stg_coins_data
)
SELECT
    coin_id
    , coin_name
    , CASE
        WHEN material IS NULL THEN 'Uncertain'
        ELSE material
    END AS material
    , CASE
        WHEN material IS NULL THEN TRUE
        WHEN material LIKE '%uncertain%' THEN TRUE
        ELSE FALSE
    END AS is_uncertain
    , coin_uri_link
FROM combined_data;
