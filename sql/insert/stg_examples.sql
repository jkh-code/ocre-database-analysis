INSERT INTO stg_web_scrape.stg_examples (
    coin_id,
    uri_page_examples_id,
    coin_axis,
    collection_name,
    coin_diameter,
    findspot,
    hoard,
    identifier,
    coin_weight
)
VALUES (
    %(coin_id)s,
    %(uri_page_examples_id)s,
    %(coin_axis)s,
    %(collection_name)s,
    %(coin_diameter)s,
    %(findspot)s,
    %(hoard)s,
    %(identifier)s,
    %(coin_weight)s
);
