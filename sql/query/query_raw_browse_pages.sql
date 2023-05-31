SELECT
    page_id
    , page_url
    , start_coin_id
    , end_coin_id
    , page_html
FROM ocre.raw_web_scrape.raw_browse_pages
ORDER BY page_id ASC;
