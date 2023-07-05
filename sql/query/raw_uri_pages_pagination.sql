SELECT
    r.raw_uri_id
    , r.coin_id
    , r.has_examples
    , r.has_examples_pagination
    , r.examples_pagination_id
    , r.examples_total_pagination
    , r.examples_start_id
    , r.examples_end_id
    , r.examples_max_id
    -- , r.page_html
    , s.coin_canonical_uri
FROM
    raw_web_scrape.raw_uri_pages AS r
    LEFT JOIN stg_web_scrape.stg_coin_summaries AS s
    ON r.coin_id = s.coin_id
WHERE r.has_examples_pagination = TRUE
ORDER BY r.coin_id;
