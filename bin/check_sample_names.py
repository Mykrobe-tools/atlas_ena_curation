#!/usr/bin/env python3
#-*- coding: latin-1 -*-

import csv
import re
import sys

with open(sys.argv[1]) as archive_file :
    smpl_set = set()
    archive = archive_file.readlines()
    for line in archive :
        sample = line.strip()
        smpl_set.add(sample)

with open(sys.argv[2]) as input, open("study.sample.runs.temp2.tsv", "w+") as output :
    reader = csv.DictReader(input, dialect='excel-tab')
    fieldnames = ['Study', 'Sample', 'Runs', 'md5s']
    writer = csv.DictWriter(output, fieldnames=fieldnames, dialect='excel-tab')
    writer.writeheader()

    for row in reader :
        sample_name = row["Sample"]
        if sample_name not in smpl_set :
            writer.writerow(row)

print('[DEBUG] Sample name check successful.')