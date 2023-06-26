SELECT
    coin_id
    , page_id
    , coin_name
    , coin_canonical_uri
FROM stg_web_scrape.stg_coin_summaries
WHERE
    coin_id >= %(start_coin_id)s
    AND num_objects_found > 0
ORDER BY coin_id ASC;
