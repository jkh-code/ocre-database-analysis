INSERT INTO raw_web_scrape.raw_uri_pages (
    coin_id, has_examples, has_examples_pagination, examples_pagination_id,
    examples_total_pagination, examples_start_id, examples_end_id, examples_max_id,
    page_html
)
VALUES (
    %(coin_id)s, %(has_examples)s, %(has_examples_pagination)s,
    %(examples_pagination_id)s, %(examples_total_pagination)s,
    %(examples_start_id)s, %(examples_end_id)s, %(examples_max_id)s, %(page_html)s
);
