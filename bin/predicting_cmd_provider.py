#!/usr/bin/env python3
#-*- coding: latin-1 -*-

import sys
import csv
import os

#######################################################################################################

reads_dict = {}

dataset_sp = sys.argv[1]
data_path = sys.argv[2]
dataset_name = sys.argv[3]
date = sys.argv[4]
pred_dir = sys.argv[5]
mykrobe_img = sys.argv[6]

count_files = 0
sub_dir = 1

#######################################################################################################

with open(dataset_sp) as reads_file :
    reader = csv.DictReader(reads_file, dialect='excel-tab')
    for row in reader:
        
        sample = row['subject_id']
        r1 = os.path.join(data_path, row['reads_file_1'])
        r2 = os.path.join(data_path, row['reads_file_2'])

        reads_dict[sample] = []
        reads_dict[sample].append(r1)
        reads_dict[sample].append(r2)

with open("./myk_predictor.sh", "w+") as cmd_file, open("./sample.list.tsv", "w+") as out_file, open("./sample.list", "w+") as out_file2 :
    header = "sample\tpath"
    print(header, file=out_file)
    for sample in reads_dict :
        reads = "--seq %s" % (' '.join(reads_dict[sample]))

        if count_files == 2000 :
                sub_dir += 1
                count_files = 0

        output_dir = os.path.join(pred_dir, dataset_name, date, str(sub_dir))

        if os.path.exists(output_dir):
            print("Warning:", output_dir, "already exists.", file=sys.stderr)
        else :
            os.makedirs(output_dir)

        cmd = "singularity exec %s mykrobe predict --sample %s --species tb %s --out %s/pred_%s.json --format json" % (mykrobe_img, sample, reads, output_dir, sample)
        print (cmd, file=cmd_file)

        tmp = "%s\t%s/pred_%s.json" % (sample, output_dir, sample)
        print (tmp, file=out_file)

        print (sample, file=out_file2)
        count_files += 1
        
#######################################################################################################