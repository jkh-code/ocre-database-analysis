WITH found_coins_data AS (
    SELECT coin_id
    FROM stg_web_scrape.stg_coin_summaries
    WHERE num_objects_found > 0
)
SELECT
    coin_id
    , page_html
FROM raw_web_scrape.raw_uri_pages
WHERE coin_id IN (TABLE found_coins_data)
ORDER BY coin_id ASC;
