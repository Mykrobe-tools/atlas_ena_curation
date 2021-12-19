#!/usr/bin/env python3

import csv
import os
import sys

data_by_study = {}
max_samples_per_study = 5000

with open(sys.argv[1]) as f:
    dict_reader = csv.DictReader(f, delimiter='\t')
    for row in dict_reader:
        if row['Study'] not in data_by_study:
            data_by_study[row['Study']] = []

        data_by_study[row['Study']].append((row['Sample'], row['Runs']))


print('Number of studies:', len(data_by_study))

groups = []

for study, sample_list in data_by_study.items():
    if len(groups) == 0:
        groups.append(sample_list)
    elif len(groups[-1]) + len(sample_list) < max_samples_per_study:
        groups[-1].extend(sample_list)
    else:
        groups.append(sample_list)

for i, sample_list in enumerate(groups):
    filename = os.path.join(f'{i+1}.tsv')
    with open(filename, 'w') as f:
        for l in sample_list:
            print(*l, sep='\t', file=f)

