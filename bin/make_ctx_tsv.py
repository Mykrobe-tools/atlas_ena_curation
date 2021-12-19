#!/usr/bin/env python3

import csv
import sys
import re

##########################################################

new_samples_info = sys.argv[1]
ctx_datafile = sys.argv[2]

ctx_set = set()

with open(ctx_datafile) as list_ctx:
    for line in list_ctx.readlines() :
        ctx_set.add(line.strip())

##########################################################

with open(new_samples_info) as data_file, open("new_samples_ctx.tsv", "w+") as outfile, open("cobs_input.tsv", "w+") as cobs_input:
    reader = csv.DictReader(data_file, dialect='excel-tab')
    header = "sample_id\tisolate_id\tena_sample_accession\tctx_file"
    print(header, file=outfile)

    for row in reader:
        sample_id = int(row['sample_id'])
        isolate_id = row['isolate_id']
        ena_sample_accession = row['ena_sample_accession']

        if sample_id < 10 :
            sample_dir = "00/00/00/0%d" % sample_id
        elif sample_id < 100 :
            sample_dir = "00/00/00/%d" % sample_id
        elif sample_id < 1000 :
            sample_dir = "00/00/0%s/%s" % (str(sample_id)[0], str(sample_id)[1:3])
        elif sample_id < 10000 :
            sample_dir = "00/00/%s/%s" % (str(sample_id)[0:2], str(sample_id)[2:4])
        elif sample_id < 100000 :
            sample_dir = "00/0%s/%s/%s" % (str(sample_id)[0], str(sample_id)[1:3], str(sample_id)[3:5])
            

        for ctx in ctx_set :
            try:
                ctx_path = re.search(r"(^/.*?%s/%s/.*?$)" % (sample_dir, isolate_id), ctx).group(1)
                final_tsv = "%s\t%s\t%s\t%s" % (sample_id, isolate_id, ena_sample_accession, ctx_path)
                print(final_tsv, file=outfile)
                final_cobs = "%s\t%s" % (ena_sample_accession, ctx_path)
                print(final_cobs, file=cobs_input)
                break
            except :
                pass


##########################################################

print('[DEBUG] Ctx tsv created.')