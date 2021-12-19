#!/usr/bin/env python3

import csv
import os
import sys
import mysql.connector
import configparser
import re

DB_CONFIG_PATH=sys.argv[1]
deleted_samples=sys.argv[1]
DB_CONFIG_SECTION='db_login'
config = configparser.ConfigParser()
config.read(DB_CONFIG_PATH)
user=config[DB_CONFIG_SECTION]['user']
password=config[DB_CONFIG_SECTION]['password']
database=config[DB_CONFIG_SECTION]['db']
host=config[DB_CONFIG_SECTION]['host']
port=config[DB_CONFIG_SECTION]['port']

cnx = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)
cursor = cnx.cursor()
xfile = "sample_db_archive.tsv"

# prepare sql statement
query = ("SELECT ena_sample_accession FROM Sample")
cursor.execute(query)

with open(xfile, "w+") as out_file, open(deleted_samples) as del_file:
  for row in cursor:
    final = re.search(r'\'(.*?)\'', str(row)).group(1)
    print(final, file=out_file)
  for line in del_file.readlines():
    print(line.strip(), file=out_file)

cursor.close()
cnx.close()