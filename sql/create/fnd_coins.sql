-- DROP TABLE IF EXISTS fnd_web_scrape.fnd_coins;
-- CREATE TABLE fnd_web_scrape.fnd_coins AS
WITH raw_stg_coins_data AS (
    SELECT
        /* Not importing the following fields:
            - manufacture
            - stated_authority_name
            - obverse_controlmark
            - obverse_state
            - reverse_control_marks
            - reverse_dynasty
            - reverse_mintmark
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
        , c.denomination
        , c.material
        , c.object_type
        , c.authority_name
        , c.issuer_name
        , c.mint
        , c.region
        , c.obverse_deity
        , c.obverse_legend
        , c.obverse_portrait
        , c.obverse_type
        , c.reverse_deity
        , c.reverse_legend
        , c.reverse_portrait
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
SELECT *
FROM raw_stg_coins_data;
