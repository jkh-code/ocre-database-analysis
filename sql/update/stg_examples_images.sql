UPDATE stg_examples_images
SET
    tried_downloading = %(tried_downloading)s
    , is_downloaded = %(is_downloaded)s
    , image_height = %(image_height)s
    , image_width = %(image_width)s
    , file_path = %(file_path)s
WHERE examples_images_id = %(examples_images_id)s;
