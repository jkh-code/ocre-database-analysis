WITH coins_with_pagination_data AS (
    SELECT
        coin_id
        , MAX(examples_pagination_id) AS last_page_scraped
        , MAX(examples_total_pagination) AS total_pages
    FROM raw_web_scrape.raw_uri_pages
    WHERE has_examples_pagination = TRUE
    GROUP BY coin_id
    HAVING MAX(examples_pagination_id) < MAX(examples_total_pagination)
)
SELECT
    p.coin_id
    , p.last_page_scraped
    , p.total_pages
    , s.coin_canonical_uri
FROM
    coins_with_pagination_data AS p
    LEFT JOIN stg_web_scrape.stg_coin_summaries AS s
    ON p.coin_id = s.coin_id
ORDER BY p.coin_id;
