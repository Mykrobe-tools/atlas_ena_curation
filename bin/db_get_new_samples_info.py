#!/usr/bin/env python3

import csv
import os
import sys
import mysql.connector
import configparser
import re

##########################################################

DB_CONFIG_PATH=sys.argv[1]
DB_CONFIG_SECTION='db_login'
config = configparser.ConfigParser()
config.read(DB_CONFIG_PATH)
user=config[DB_CONFIG_SECTION]['user']
password=config[DB_CONFIG_SECTION]['password']
database=config[DB_CONFIG_SECTION]['db']
host=config[DB_CONFIG_SECTION]['host']
port=config[DB_CONFIG_SECTION]['port']

query_isolate = ("SELECT isolate_id, sample_id FROM Isolate")
query_sample = ("SELECT sample_id, subject_id, ena_sample_accession FROM Sample")
query_seqrep = ("SELECT isolate_id, seqrep_id, ena_run_accession FROM Seqrep")
query_qc = ("SELECT seqrep_id, samtools_bases_mapped_cigar, het_snp_het_calls FROM QC")

run_ids = set()
samples_set = set()

##########################################################

def _connect_db() :
  cnx = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)
  return cnx

def _get_samples_dict(cnx) :
  cursor_sample = cnx.cursor(dictionary=True)
  cursor_sample.execute(query_sample)

  samples_dict = {}
  for row in cursor_sample:
    sample_id = row['sample_id']
    samples_dict[sample_id] = {}
    samples_dict[sample_id]['subject_id'] = row['subject_id']
    samples_dict[sample_id]['ena_sample_accession'] = row['ena_sample_accession']

  cursor_sample.close()
  return samples_dict


def _get_isolates_dict(cnx, samples_dict) :
  cursor_isolate = cnx.cursor(dictionary=True)
  cursor_isolate.execute(query_isolate)

  isolates_dict = {}
  for row in cursor_isolate:
    isolate_id = row['isolate_id']
    sample_id = row['sample_id']
    isolates_dict[isolate_id] = {}
    isolates_dict[isolate_id]['sample_id'] = sample_id
    isolates_dict[isolate_id]['ena_sample_accession'] = samples_dict[sample_id]['ena_sample_accession']
    isolates_dict[isolate_id]['subject_id'] = samples_dict[sample_id]['subject_id']

  cursor_isolate.close()
  return isolates_dict


def _get_db_dict(cnx, isolates_dict) :
  cursor_seqrep = cnx.cursor(dictionary=True)
  cursor_seqrep.execute(query_seqrep)

  db_dict = {}
  count = 0
  for row in cursor_seqrep:
    ena_run_accession = row['ena_run_accession']
    if ena_run_accession is None :
      seqrep_id = row['seqrep_id']
      isolate_id = row['isolate_id']
      db_dict[seqrep_id] = {}
      db_dict[seqrep_id]['isolate_id'] = isolate_id
      db_dict[seqrep_id]['sample_id'] = isolates_dict[isolate_id]['sample_id']
      db_dict[seqrep_id]['subject_id'] = isolates_dict[isolate_id]['subject_id']
      db_dict[seqrep_id]['ena_run_accession'] = 'NULL'
      db_dict[seqrep_id]['ena_sample_accession'] = 'NULL'
    elif ena_run_accession is not None and ena_run_accession not in run_ids :
      run_ids.add(ena_run_accession)
      seqrep_id = row['seqrep_id']
      isolate_id = row['isolate_id']
      db_dict[seqrep_id] = {}
      db_dict[seqrep_id]['isolate_id'] = isolate_id
      db_dict[seqrep_id]['sample_id'] = isolates_dict[isolate_id]['sample_id']
      db_dict[seqrep_id]['ena_sample_accession'] = isolates_dict[isolate_id]['ena_sample_accession']
      db_dict[seqrep_id]['subject_id'] = isolates_dict[isolate_id]['subject_id']
      db_dict[seqrep_id]['ena_run_accession'] = ena_run_accession
    else :
      count += 1
  print ("%d runs were duplicates." % count)


  cursor_seqrep.close()
  cursor_qc = cnx.cursor(dictionary=True)
  cursor_qc.execute(query_qc)

  for row in cursor_qc:
    seqrep_id = row['seqrep_id']
    if seqrep_id in db_dict :
      try :
        db_dict[seqrep_id]['samtools_bases_mapped_cigar'] = row['samtools_bases_mapped_cigar']
        db_dict[seqrep_id]['het_snp_het_calls'] = row['het_snp_het_calls']
      except :
        del db_dict[seqrep_id]

  cursor_qc.close()
  return db_dict
  
##########################################################

# connect to database
with _connect_db() as cnx_ena_tb :
  samples_dict = _get_samples_dict(cnx_ena_tb)
  isolates_dict = _get_isolates_dict(cnx_ena_tb, samples_dict)
  dict_ena_tb = _get_db_dict(cnx_ena_tb, isolates_dict)


##########################################################

# write to TSV
with open("new_samples_info.tsv", "w+") as out_file, open("qc_discarded_samples", "w+") as err_file:
  header = "sample_id\tisolate_id\tena_sample_accession\tsubject_id"
  print(header, file=out_file)

  for seqrep_id in dict_ena_tb :
    sample_id = dict_ena_tb[seqrep_id]['sample_id']
    if sample_id not in samples_set :
      try :
        samtools_bases_mapped_cigar = dict_ena_tb[seqrep_id]['samtools_bases_mapped_cigar']
        het_snp_het_calls = dict_ena_tb[seqrep_id]['het_snp_het_calls']
        if (samtools_bases_mapped_cigar / float(4411532) <= 15) :
          print("seqrep %s discarded : bad coverage (%s)." % (str(seqrep_id), str(samtools_bases_mapped_cigar)), file=err_file)
        elif (het_snp_het_calls > 100000):
          print("seqrep %s discarded : too many het calls (%s)." % (str(seqrep_id), str(het_snp_het_calls)), file=err_file)
        else :
          samples_set.add(sample_id)
          isolate_id = dict_ena_tb[seqrep_id]['isolate_id']
          ena_sample_accession = dict_ena_tb[seqrep_id]['ena_sample_accession']
          subject_id = dict_ena_tb[seqrep_id]['subject_id']
          ena_run_accession = dict_ena_tb[seqrep_id]['ena_run_accession']
          row = "%s\t%s\t%s\t%s" % (sample_id, isolate_id, ena_sample_accession, subject_id)
          print(row, file=out_file)
          del samtools_bases_mapped_cigar
          del het_snp_het_calls
      except :
        print("seqrep %s discarded : qc info lacking." % (str(seqrep_id)), file=err_file)


 