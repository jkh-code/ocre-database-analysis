INSERT INTO stg_web_scrape.stg_examples_images (
    examples_images_id,
    stg_examples_id,
    image_type,
    link
)
VALUES (
    %(examples_images_id)s,
    %(stg_examples_id)s,
    %(image_type)s,
    %(link)s
);
