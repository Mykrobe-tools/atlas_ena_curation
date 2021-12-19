#!/usr/bin/env python3

import csv
import os
import requests
import sys
import re

sample_list = sys.argv[1]

def get_sample_data(sample_id):
    fields = ['study_accession', 'sample_accession', 'run_accession', 'fastq_md5', 'study_title', 'sample_title']
    url = 'http://www.ebi.ac.uk/ena/data/warehouse/filereport?'
    data = {
        'accession': sample_id,
        'result': 'read_run',
        'fields': ','.join(fields)}

    try:
        r = requests.get(url, data)
    except:
        raise Exception('Error querying ENA to get sample from run \n' + r.url)
    
    if r.status_code != requests.codes['ok']:
        raise Exception('Error requesting data. Error code: ' + str(r.status_code) + '\n' + r.url)

    lines = r.text.rstrip().split('\n')

    if len(lines) < 2 or lines[0] != '\t'.join(fields):
        raise Exception('Unexpected format from ENA request ' + r.url + '\nGot:' + str(lines))

    got_fields = [x.split('\t') for x in lines[1:]]

    studies = set([x[0] for x in got_fields])
    if len(studies) > 1:
        raise Exception('More than one study found ', str(lines))

    samples = set([x[1] for x in got_fields])
    if len(samples) > 1:
        raise Exception('More than one sample found ', str(lines))

    runs = set([x[2] for x in got_fields])

    md5s = set([x[3] for x in got_fields])

    return {
        'study': got_fields[0][0],
        'sample': got_fields[0][1],
        'runs': ','.join(sorted(list(runs))),
        'md5s': ','.join(sorted(list(md5s))),
        'study_title': got_fields[0][4],
        'sample_title': got_fields[0][5]
    }

exclusion_criteria = [
    'mutant',
    'reference',
    'rep1',
    'rep2',
    'rep3',
    'RNASeq',
    'H37Rv',
    'EQA'
]

with open(sample_list) as f:
    dict_reader = csv.DictReader(f, delimiter='\t')
    samples = [row['accession'] for row in dict_reader]

print(len(samples), 'for which to get data')

tsv_out = 'study.sample.runs.temp.tsv'
old_tsv_out = tsv_out + '.old'
got_samples = {}

if os.path.exists(tsv_out):
    os.rename(tsv_out, old_tsv_out)
    with open(old_tsv_out) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            got_samples[row['Sample']] = (row['Study'], row['Sample'], row['Runs'], row['md5s'])


with open(tsv_out, 'w') as f:
    print('Study\tSample\tRuns\tmd5s', file=f)

    for i, sample in enumerate(samples):
        if i % 100 == 0:
            print(i, 'samples of', len(samples), 'done')
            f.flush()
            os.fsync(f)

        if sample in got_samples:
            print(*got_samples[sample], sep='\t', file=f)
        else:
            try:
                d = get_sample_data(sample)
            except:
                print(f'Error getting data for sample {sample}', file=sys.stderr)
                continue

            if d['sample'].startswith('SAM'):
                list_str1 = re.split(r'\.|;|:|,|_|\(|\)|"|\s|\n' , str(d['study_title']))
                list_str2 = re.split(r'\.|;|:|,|_|\(|\)|"|\s|\n' , str(d['sample_title']))
                if any(x in list_str1 for x in exclusion_criteria) or any(x in list_str2 for x in exclusion_criteria):
                    print(f'Removed sample {sample}. Study or sample title contained an exclusion criterion. Study title : %s / Sample title : %s' % (str(d['study_title']).rstrip('\n'), str(d['sample_title']).rstrip('\n')), file=sys.stderr)
                else :
                    print(d['study'], d['sample'], d['runs'], d['md5s'], sep='\t', file=f)
            else:
                print(f'Error getting sample {sample}. Got {d["sample"]}, which does not start with SAM. {d}', file=sys.stderr)