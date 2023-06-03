INSERT INTO raw_web_scrape.raw_browse_pages (
    page_id, page_url, start_coin_id, end_coin_id, page_html
)
VALUES (
    %(page_id)s, %(page_url)s, %(start_coin_id)s, %(end_coin_id)s, %(page_html)s
);
