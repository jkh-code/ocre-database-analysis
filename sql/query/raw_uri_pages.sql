SELECT
    raw_uri_id
    , coin_id
    , has_examples
    , has_examples_pagination
    , examples_pagination_id
    , examples_total_pagination
    , examples_start_id
    , examples_end_id
    , examples_max_id
    , page_html
FROM raw_web_scrape.raw_uri_pages
ORDER BY coin_id ASC, examples_pagination_id ASC;
