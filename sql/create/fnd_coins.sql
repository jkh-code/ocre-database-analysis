-- DROP TABLE IF EXISTS fnd_web_scrape.fnd_coins;
-- CREATE TABLE fnd_web_scrape.fnd_coins AS
WITH denomination_data AS (
    SELECT
        coin_id
        , COUNT(coin_id) AS num_denominations
    FROM stg_web_scrape.stg_processed_denominations
    WHERE denomination != 'Uncertain'
    GROUP BY coin_id
),
materials_data AS (
    SELECT
        coin_id
        , COUNT(coin_id) AS num_materials
    FROM stg_web_scrape.stg_processed_materials
    WHERE material != 'Uncertain'
    GROUP BY coin_id
),
authority_names_data AS (
    SELECT
        coin_id
        , COUNT(coin_id) AS num_authority_names
    FROM stg_web_scrape.stg_processed_authority_names
    WHERE authority_name != 'Uncertain'
    GROUP BY coin_id
),
issuer_names_data AS (
    SELECT
        coin_id
        , COUNT(coin_id) AS num_issuer_names
    FROM stg_web_scrape.stg_processed_issuer_names
    WHERE issuer_name != 'Uncertain'
    GROUP BY coin_id
),
mints_data AS (
    SELECT
        coin_id
        , COUNT(coin_id) AS num_mints
    FROM stg_web_scrape.stg_processed_mints
    WHERE mint != 'Uncertain'
    GROUP BY coin_id
),
regions_data AS (
    SELECT
        coin_id
        , COUNT(coin_id) AS num_regions
    FROM stg_web_scrape.stg_processed_regions
    WHERE region != 'Uncertain'
    GROUP BY coin_id
)
SELECT
    coin.coin_id
    , coin.coin_name
    , coin.has_typological
    , coin.has_examples
    , coin.has_examples_pagination
    , coin.has_analysis
    , coin.coin_date_range
    , coin.object_type
    , coin.obverse_legend
    , coin.obverse_type
    , coin.reverse_legend
    , coin.reverse_type
    , coin.average_axis
    , coin.average_diameter
    , coin.average_weight
    , coin.coin_uri_link
    , COALESCE(den.num_denominations, 0) AS num_denominations
    , COALESCE(mat.num_materials, 0) AS num_materials
    , COALESCE(auth.num_authority_names, 0) AS num_authority_names
    , COALESCE(iss.num_issuer_names, 0) AS num_issuer_names
    , COALESCE(min.num_mints, 0) AS num_mints
    , COALESCE(reg.num_regions, 0) AS num_regions
    , CURRENT_TIMESTAMP AS ts
FROM
    stg_web_scrape.stg_processed_coins AS coin
    LEFT JOIN denomination_data AS den
    ON coin.coin_id = den.coin_id
    LEFT JOIN materials_data AS mat
    ON coin.coin_id = mat.coin_id
    LEFT JOIN authority_names_data AS auth
    ON coin.coin_id = auth.coin_id
    LEFT JOIN issuer_names_data AS iss
    ON coin.coin_id = iss.coin_id
    LEFT JOIN mints_data AS min
    ON coin.coin_id = min.coin_id
    LEFT JOIN regions_data AS reg
    ON coin.coin_id = reg.coin_id;
-- SELECT * FROM denomination_data;
