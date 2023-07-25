INSERT INTO stg_web_scrape.stg_uri_pages (
    uri_page_id,
    coin_id,
    examples_pagination_id,
    examples_total_pagination,
    examples_start_id,
    examples_end_id,
    examples_max_id,
    uri_link
)
VALUES (
    %(uri_page_id),
    %(coin_id),
    %(examples_pagination_id),
    %(examples_total_pagination),
    %(examples_start_id),
    %(examples_end_id),
    %(examples_max_id),
    %(uri_link)
);
