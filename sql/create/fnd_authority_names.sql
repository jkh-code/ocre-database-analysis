DROP TABLE IF EXISTS fnd_web_scrape.fnd_authority_names;
CREATE TABLE fnd_web_scrape.fnd_authority_names AS
WITH raw_stg_coins_data AS (
    SELECT
        c.coin_id
        , c.coin_name
        , c.authority_name
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
        , CAST(authority_name AS VARCHAR) AS authority_name
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE authority_name IS NULL
    UNION ALL
        SELECT
        coin_id
        , coin_name
        , UNNEST(authority_name) AS authority_name
        , coin_uri_link
    FROM raw_stg_coins_data
    WHERE authority_name IS NOT NULL
)
SELECT
    coin_id
    , coin_name
    , CASE
        WHEN authority_name IN ('Uncertain value', 'Anonymous') THEN 'Uncertain'
        ELSE authority_name
    END AS authority_name
    , CASE
        WHEN authority_name IN ('Uncertain value', 'Anonymous') THEN TRUE
        ELSE FALSE
    END AS is_uncertain
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM combined_data;
