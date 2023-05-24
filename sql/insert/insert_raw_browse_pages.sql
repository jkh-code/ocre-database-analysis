INSERT INTO ocre.raw_web_scrape.raw_browse_pages (
    page_id, page_url, start_coin_id, end_coin_id, page_html
)
VALUES (
    %(page_id_)s, %(page_url_)s, %(start_coin_id_)s, %(end_coin_id_)s, %(page_html_)s
);
