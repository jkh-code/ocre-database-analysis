# Shell Notes

## Explicitly run a z-shell script
```sh
zsh /path/to/file.ext
```



## Break line up
When you have an operator like `&&` that expects commands to the left and right, the break can be after the operator without a `\` break operator because another command is expected. See the second example below. However, if you break before the  operator, you'll need to input a `\` break operator. See the first example below.

```sh
# Without `&&` operator at end (preferred method)
python ocre_database_analysis/create_ocre_database.py \
    && python ocre_database_analysis/scrape_ocre.py

# With `&&` operator before line break
python ocre_database_analysis/create_ocre_database.py &&
    python ocre_database_analysis/scrape_ocre.py
```
