SELECT
    uri.raw_uri_id
    , uri.coin_id
    , uri.has_examples
    , uri.has_examples_pagination
    , uri.examples_pagination_id
    , uri.examples_total_pagination
    , uri.examples_start_id
    , uri.examples_end_id
    , uri.examples_max_id
    , uri.page_html
    , CASE
        WHEN uri.has_examples_pagination = FALSE THEN link.coin_canonical_uri
        ELSE CONCAT(link.coin_canonical_uri, '?page=', uri.examples_pagination_id)
    END AS path_uri
FROM
    raw_web_scrape.raw_uri_pages AS uri
    LEFT JOIN stg_web_scrape.stg_coin_summaries AS link
    ON uri.coin_id = link.coin_id
WHERE uri.has_examples = TRUE
ORDER BY uri.coin_id ASC, uri.examples_pagination_id ASC;
