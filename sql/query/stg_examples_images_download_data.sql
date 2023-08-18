SELECT
    e.coin_id
    , i.stg_examples_id
    , i.examples_images_id
    , i.image_type
    , i.link
    , i.tried_downloading
    , i.is_downloaded
    , i.image_dimensions
    , i.file_path
FROM
    stg_web_scrape.stg_examples AS e
    INNER JOIN stg_web_scrape.stg_examples_images AS i
    ON e.examples_id = i.stg_examples_id
ORDER BY e.coin_id ASC, i.stg_examples_id ASC, i.examples_images_id ASC;
