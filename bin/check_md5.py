#!/usr/bin/env python3

import csv
import os
import sys
import re

dl_set = set()
dico_dl = {}
archive_set = set()
notok_set =  set()

with open(sys.argv[1]) as samples_file :
    reader = csv.DictReader(samples_file, dialect='excel-tab')
    for row in reader :
        sample = row["Sample"]
        md5s = re.split(r';|,', row["md5s"])
        dl_set.update(md5s)

        dico_dl[sample] = {}
        dico_dl[sample]['Sample'] = sample
        dico_dl[sample]['Study'] = row["Study"]
        dico_dl[sample]['Runs'] = row["Runs"]
        dico_dl[sample]['md5s'] = row["md5s"]

with open(sys.argv[2]) as archive_file :
    archive = archive_file.readlines()
    for line in archive :
        archive_set.add(line.strip())

for md5 in dl_set :
    if md5 in archive_set :
        notok_set.add(md5)

with open("study.sample.runs.tsv", "w+") as output :
    fieldnames = ['Study', 'Sample', 'Runs', 'md5s']
    writer = csv.DictWriter(output, fieldnames=fieldnames, dialect='excel-tab')
    writer.writeheader()

    for sample in dico_dl :
        md5s = re.split(r';|,', dico_dl[sample]['md5s'])
        if any(md5 in md5s for x in notok_set) :
            print(f'Removed sample {sample}. One or more md5 are already in the database.', file=sys.stderr)
        else :
            writer.writerow(dico_dl[sample])
        
print('[DEBUG] Primary md5 check successful.')