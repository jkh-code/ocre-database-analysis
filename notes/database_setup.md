# Database Set Up Notes

## Add to PYTHONPATH

```sh
export PYTHONPATH=$PYTHONPATH:/path/to/my/ocre-database-analysis/ocre_database_analysis/
```



## Connection Parameters
Add to environmental variables.

```sh
# pgds connection parameters
export PGDS_DBNAME="MY DB NAME"
export PGDS_USER="MY USER"
export PGDS_PASSWORD="MY PASSWORD"
export PGDS_HOST="MY LOCALHOST"
export PGDS_PORT="MY PORT NO."
```



## Docker
- Internal volume at `/var/lib/postgresql/data`.
- Is external access to volume required?

```sh
docker run --name pgds -d -p MY_PORT:5432 -e POSTGRES_PASSWORD='MY PASSWORD' postgres
```



## Creating database
Running `create_ocre_database.py` script will create the database structure.

- Create `ocre` database
- Create schemas
    - `raw_web_scrape`
    - `stg_web_scrape`
- Create tables
    - `ocre.raw_web_scrape.raw_browse_pages`
    - `ocre.stg_web_scrape.stg_browse_pages`
    - `ocre.raw_web_scrape.raw_canonical_uris`



## Issues When Creating New Database
- When scraping URI HTML, there are a lot of pages to scrape. I have had several failures due to connection errors that are resolved the next time I run the script.
    - Users will need to we warned about this potential.
    - Redesigning scrape script so that it can be run multiple times from this failure point.
- Another issue with scraping URI HTML: scraping may take over 11 hours when the full database is scraped.
    - I may want to limit to coins with found objects.
    - About 11 hours when full database is scraped
    - About 7-8 hours when only coins with `num_objects_found > 0`.
