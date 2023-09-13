DROP TABLE IF EXISTS stg_web_scrape.stg_processed_coins;
CREATE TABLE stg_web_scrape.stg_processed_coins AS
WITH raw_stg_coins_data AS (
    /* `object_type` does not have any null values, therefore, zero rows
    are dropped when unnesting occurs. */
    SELECT
        /* Not importing the following fields into fnd_coins:
            - manufacture
            - stated_authority_name
            - obverse_controlmark
            - obverse_state
            - reverse_control_marks
            - reverse_dynasty
            - reverse_mintmark
            - reverse_monogram
            - reverse_officinamark
            - reverse_state
            - reverse_symbol */
        c.coin_id
        , c.coin_name
        , c.has_typological
        , c.has_examples
        , c.has_examples_pagination
        , c.has_analysis
        , c.coin_date_range
        , UNNEST(c.object_type) AS object_type
        , c.obverse_legend
        , c.obverse_type
        , c.reverse_legend
        , c.reverse_type
        , c.average_axis
        , c.average_diameter
        , c.average_weight
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
    , has_typological
    , has_examples
    , has_examples_pagination
    , has_analysis
    , coin_date_range
    , object_type
    , obverse_legend
    , obverse_type
    , reverse_legend
    , reverse_type
    , average_axis
    , average_diameter
    , average_weight
    , coin_uri_link
    , CURRENT_TIMESTAMP AS ts
FROM raw_stg_coins_data
WHERE
    object_type NOT LIKE '%Tessera%'
    AND object_type != 'Medallion';
