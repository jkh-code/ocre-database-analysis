INSERT INTO stg_web_scrape.stg_coin_summaries (
    coin_id, page_id, coin_name, coin_canonical_uri, coin_date_string,
    denomination, mint, obverse_description, reverse_description,
    reference, num_objects_found
)
VALUES
    (
        %(coin_id)s, %(page_id)s, %(coin_name)s, %(coin_canonical_uri)s,
        %(coin_date_string)s, %(denomination)s, %(mint)s, %(obverse_description)s,
        %(reverse_description)s, %(reference)s, %(num_objects_found)s
    );
