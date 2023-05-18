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
docker run --name pgds -d -p 5432:5432 -e POSTGRES_PASSWORD='MY PASSWORD' postgres
```
