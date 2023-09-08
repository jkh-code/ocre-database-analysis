-- DROP TABLE IF EXISTS fnd_web_scrape.fnd_coins;
-- CREATE TABLE fnd_web_scrape.fnd_coins AS
WITH raw_stg_coins_data AS (
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
        , CURRENT_TIMESTAMP AS ts
    FROM
        stg_web_scrape.stg_coins AS c
        LEFT JOIN stg_web_scrape.stg_uri_pages AS u
        ON c.coin_id = u.coin_id
        AND (u.examples_pagination_id = 1 OR u.examples_pagination_id IS NULL)
)
SELECT *
FROM raw_stg_coins_data
WHERE
    object_type NOT LIKE '%Tessera%'
    AND object_type != 'Medallion';
