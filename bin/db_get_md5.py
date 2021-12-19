#!/usr/bin/env python3

import csv
import os
import sys
import mysql.connector
import configparser

DB_CONFIG_PATH=sys.argv[1]
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
xfile = "md5_archive.tsv"

query = ("SELECT original_reads_file_1_md5, original_reads_file_2_md5 FROM Seqrep")
cursor.execute(query)

with open(xfile, "w+") as out_file:
  for row in cursor:
    print(row[0], file=out_file)
    print(row[1], file=out_file)

cursor.close()
cnx.close()