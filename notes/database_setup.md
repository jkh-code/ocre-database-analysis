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
