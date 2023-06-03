#!/bin/zsh

python ocre_database_analysis/create_ocre_database.py \
    && python ocre_database_analysis/scrape_ocre.py
