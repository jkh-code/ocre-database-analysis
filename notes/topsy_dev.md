# Topsy Dev Notes

## Debugging Code
```python
# Debug
# -----

# Connecting and closing connection
try:
    temp = Topsy("ocre")
except ValueError as err:
    print(err)
    print(f"Unable to connect to `{temp.conn_parameters['dbname']}`.")
    sys.exit(1)
except pg2.OperationalError as err:
    Topsy.print_pg2_exception(err)
    sys.exit(1)

# Creating new databases
# try:
#     # temp.create_new_database(["delme", "ocre", "nhl", "candy_land"])
#     # temp.create_new_database(["delme", "ocre", "nhl", "candy_land"], switch=True)
#     temp.create_new_database(["ocre"], switch=True)
# except ValueError as err:
#     print(err)
#     temp.close_connection()
#     sys.exit(1)

# Creating new schema
# try:
#     # temp.create_new_schema("fail")
#     # temp.create_new_schema([])
#     # temp.create_new_schema(["success"])
#     # temp.create_new_schema(["raw", "stg", "fnd", "rpt"])
#     temp.create_new_schema(["raw_web_scrape"])
# except ValueError as err:
#     print(err)
#     sys.exit(1)

# Creating new table
# try:
#     # temp.create_new_table("/will/fail")
#     path_file = c.SQL_FOLDER / "create" / "raw_browse_pages.sql"
#     temp.create_new_table(path_file)
# except ValueError as err:
#     print(err)
#     sys.exit(1)

# Inserting data
# data_test = [
#     {
#         "page_id_": 1,
#         "page_url_": "start=0",
#         "start_coin_id_": 1,
#         "end_coin_id_": 20,
#         "page_html_": "sample",
#     },
#     {
#         "page_id_": 2,
#         "page_url_": "start=20",
#         "start_coin_id_": 21,
#         "end_coin_id_": 40,
#         "page_html_": "sample",
#     },
#     {
#         "page_id_": 3,
#         "page_url_": "start=40",
#         "start_coin_id_": 41,
#         "end_coin_id_": 60,
#         "page_html_": "sample",
#     },
#     {
#         "page_id_": 4,
#         "page_url_": "start=60",
#         "start_coin_id_": 61,
#         "end_coin_id_": 80,
#         "page_html_": "sample",
#     },
#     {
#         "page_id_": 5,
#         "page_url_": "start=80",
#         "start_coin_id_": 81,
#         "end_coin_id_": 100,
#         "page_html_": "sample",
#     },
# ]
data_test = [
    {
        "page_id_": 99,
        "page_url_": "start=99",
        "start_coin_id_": 99,
        "end_coin_id_": 99,
        "page_html_": "sample",
    }
]

try:
    path_file = c.SQL_FOLDER / "insert" / "raw_browse_pages.sql"
    temp.insert_data(path_file, data_test)
except ValueError as err:
    print(err)
    sys.exit(1)

temp.close_connection()
```
