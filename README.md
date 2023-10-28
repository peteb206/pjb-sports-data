# pjb-sports-data
Repository to interact with pjb-sports-data BigQuery project

[![daily](https://github.com/peteb206/pjb-sports-data/actions/workflows/daily.yml/badge.svg)](https://github.com/peteb206/pjb-sports-data/actions/workflows/daily.yml)

This project allows me to ...
- pull data from public sources on a daily basis
    - this prevents me from having to repitively make calls to these sources and overload their servers
- add the data to my own database
    - easier for me to preview the data, perform data quality checks, etc.
- query only the data I need
    - no longer having to load tons of external data sources to memory and then filter to what I need

Why BigQuery?
- 10 GB free storage each month
- 1 TB free queries each month
- Google Cloud is a valuable skill in the data engineering world
- Easy connection to Python
- Easy setup compared to Oracle, for example