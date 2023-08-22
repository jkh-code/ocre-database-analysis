SELECT
    MAX(e.coin_id) AS max_coin_id
    , MAX(i.stg_examples_id) AS max_stg_examples_id
    , MAX(i.examples_images_id) AS max_examples_images_id
FROM
    stg_web_scrape.stg_examples AS e
    INNER JOIN stg_web_scrape.stg_examples_images AS i
    ON e.examples_id = i.stg_examples_id;
