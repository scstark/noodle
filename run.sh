#!/bin/bash

# this is a master script for the EMR cluster

# download and uncompress data file

wget ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2017.csv.gz .

gunzip 2017.csv.gz

# import CSV file into HDFS and create Hive table

hadoop fs -mkdir /user/hadoop/noodle/raw/

hadoop fs -put 2017.csv /user/hadoop/noodle/raw/2017.csv

hive -f create_weather_raw.sql

# verify Hive table works by selecting a count

a=$(hive -e 'select count(*) from weatherraw;')

echo $a

# run spark script
spark-submit stark_challenge.py
