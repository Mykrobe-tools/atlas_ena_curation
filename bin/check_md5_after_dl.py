#!/usr/bin/env python3

import csv
import os
import sys
import re

with open(sys.argv[1]) as dl_file :
    set_dl = set()
    dico_dl = {}
    cmpt=0
    dico_name = {}

    dl = csv.DictReader(dl_file, dialect='excel-tab')
    for row in dl :
        cmpt+=1

        sample_name = row["ena_sample_accession"]
        run_name = row["ena_run_accession"]

        dico_name[sample_name] = run_name
        
        dico_dl[cmpt] = {}
        dico_dl[cmpt]["subject_id"] = row["subject_id"] #1
        dico_dl[cmpt]["site_id"] = row["site_id"] #2
        dico_dl[cmpt]["lab_id"] = row["lab_id"] #3
        dico_dl[cmpt]["isolate_number"] = row["isolate_number"] #4
        dico_dl[cmpt]["sequence_replicate_number"] = row["sequence_replicate_number"] #5
        dico_dl[cmpt]["submission_date"] = row["submission_date"] #6
        dico_dl[cmpt]["reads_file_1"] = row["reads_file_1"] #7
        dico_dl[cmpt]["reads_file_1_md5"] = "" #8 init
        dico_dl[cmpt]["reads_file_2"] = row["reads_file_2"] #9
        dico_dl[cmpt]["reads_file_2_md5"] = "" #10 init
        dico_dl[cmpt]["dataset_name"] = row["dataset_name"] #11
        dico_dl[cmpt]["instrument_model"] = row["instrument_model"] #12
        dico_dl[cmpt]["ena_center_name"] = row["ena_center_name"] #13
        dico_dl[cmpt]["submit_to_ena"] = row["submit_to_ena"] #14
        dico_dl[cmpt]["ena_on_hold"] = row["ena_on_hold"] #15
        dico_dl[cmpt]["ena_run_accession"] = run_name #16
        dico_dl[cmpt]["ena_sample_accession"] = sample_name #17

        file1 = "%s/%s.md5" % (sys.argv[3], row["reads_file_1"])
        file2 = "%s/%s.md5" % (sys.argv[3], row["reads_file_2"])

        with open(file1) as md5 :
            md5_file = md5.readlines()
            for line in md5_file :
                md5_str = re.search(r'^(.*?) ', line).group(1)
                set_dl.add(md5_str)
                dico_dl[cmpt]["reads_file_1_md5"] = md5_str
        with open(file2) as md5 :
            md5_file = md5.readlines()
            for line in md5_file :
                md5_str = re.search(r'^(.*?) ', line).group(1)
                set_dl.add(md5_str)
                dico_dl[cmpt]["reads_file_2_md5"] = md5_str

with open(sys.argv[2]) as archive_file :
    set_archive = set()
    archive = archive_file.readlines()
    for line in archive :
        set_archive.add(line.strip())

set_ok = set()
for md5 in set_dl :
    if md5 not in set_archive :
        set_ok.add(md5)


with open("import.tsv", "w+") as output :
    fieldnames = ['subject_id', 'site_id', 'lab_id', 'isolate_number', 'sequence_replicate_number', 'submission_date', 'reads_file_1', 'reads_file_1_md5', 'reads_file_2', 'reads_file_2_md5', 'dataset_name', 'instrument_model', 'ena_center_name', 'submit_to_ena', 'ena_on_hold', 'ena_run_accession', 'ena_sample_accession']
    writer = csv.DictWriter(output, fieldnames=fieldnames, dialect='excel-tab')
    writer.writeheader()
    set_delete = set()
    cnt = 0

    for sample in dico_dl :
        md5_1 = dico_dl[sample]["reads_file_1_md5"]
        md5_2 = dico_dl[sample]["reads_file_2_md5"]
        if md5_1 in set_ok and md5_2 in set_ok :
            writer.writerow(dico_dl[sample])
            cnt += 1
        else :
            set_delete.add(dico_dl[sample]["subject_id"])

    if cnt == 0 :
        print ("[ABORTING] No new sample to process.")
        exit()
        

with open("delete_reads.sh", "w+") as delete_script :
    cmpt = 0
    for sample in set_delete :
        run = dico_name[sample]
        line = "rm %s_*.fastq.gz %s_*.fastq.gz.md5" % (run.strip(), run.strip())
        print(line, file=delete_script)
        cmpt+=1
    result = "echo \"[DEBUG] Deleted %s sample(s) (both reads), and associated md5 files.\"" % cmpt
    print(result, file=delete_script)
    del_imp = "rm %s/import_temp.tsv" % (sys.argv[3])
    print(del_imp, file=delete_script)
        
print('[DEBUG] Secondary md5 check successful.')