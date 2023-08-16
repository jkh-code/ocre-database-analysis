DROP TYPE IF EXISTS stg_web_scrape.image_type_values;
CREATE TYPE stg_web_scrape.image_type_values AS ENUM (
    'obverse', 'reverse', 'both sides', 'unknown'
);

DROP TABLE IF EXISTS stg_web_scrape.stg_examples_images;
CREATE TABLE stg_web_scrape.stg_examples_images (
    examples_images_id SERIAL NOT NULL,
    stg_examples_id INTEGER NOT NULL,
    image_type stg_web_scrape.image_type_values NOT NULL,
    link VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (examples_images_id)
);
