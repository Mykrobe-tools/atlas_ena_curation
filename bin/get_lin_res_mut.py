#!/usr/bin/env python3
#-*- coding: latin-1 -*-
import sys
import json
import csv

#######################################################################################################

path_file = sys.argv[1]
file_date = str(sys.argv[2])
summary_file = "ena_myk_analysis_%s.tsv" % (file_date)
sample_dict = {}

#######################################################################################################

with open(path_file) as data_file:
    reader = csv.DictReader(data_file, dialect='excel-tab')
    for row in reader:
        sample = row["sample"]
        sample_dict[sample] = {}
        sample_dict[sample]["path"] = row["path"]

#######################################################################################################

with open(summary_file, "w+") as file_out :
    header = "sample_name\tspecies_or_lineage\tOfloxacin\tOfloxacin_pr\tMoxifloxacin\tMoxifloxacin_pr\tIsoniazid\tIsoniazid_pr\tKanamycin\tKanamycin_pr\tEthambutol\tEthambutol_pr\tStreptomycin\tStreptomycin_pr\tCiprofloxacin\tCiprofloxacin_pr\tPyrazinamide\tPyrazinamide_pr\tRifampicin\tRifampicin_pr\tAmikacin\tAmikacin_pr\tCapreomycin\tCapreomycin_pr"
    print(header,file=file_out)

    for sample_name in sample_dict :
        try :
            sample_json = sample_dict[sample_name]["path"]
            with open(sample_json) as json_file :
                data = json.load(json_file)
                for sample in data :

                    #LINEAGE
                    if "lineage" in data[sample]['phylogenetics']['lineage'] :
                        lineage = "/".join(data[sample]['phylogenetics']['lineage']['lineage'])
                    else :
                        lineage = "unknown"
                    if lineage == "unknown" and 'species' in data[sample]['phylogenetics'] :
                        lineage = "/".join(data[sample]['phylogenetics']['species'])
                    
                    #SUSCEPTIBILITY
                    if "Ofloxacin" in data[sample]['susceptibility'] :
                        Ofloxacin = data[sample]['susceptibility']["Ofloxacin"]["predict"]
                        if data[sample]['susceptibility']["Ofloxacin"]["predict"] != 'S':
                            Ofloxacin_pr = str("/".join(data[sample]['susceptibility']["Ofloxacin"]["called_by"]))
                        else :
                            Ofloxacin_pr = "None"
                    else :
                        Ofloxacin = "unknown"
                    
                    if "Moxifloxacin" in data[sample]['susceptibility'] :
                        Moxifloxacin = data[sample]['susceptibility']["Moxifloxacin"]["predict"]
                        if data[sample]['susceptibility']["Moxifloxacin"]["predict"] != 'S':
                            Moxifloxacin_pr = str("/".join(data[sample]['susceptibility']["Moxifloxacin"]["called_by"]))
                        else :
                            Moxifloxacin_pr = "None"
                    else :
                        Moxifloxacin = "unknown"
                    
                    if "Isoniazid" in data[sample]['susceptibility'] :
                        Isoniazid = data[sample]['susceptibility']["Isoniazid"]["predict"]
                        if data[sample]['susceptibility']["Isoniazid"]["predict"] != 'S':
                            Isoniazid_pr = str("/".join(data[sample]['susceptibility']["Isoniazid"]["called_by"]))
                        else :
                            Isoniazid_pr = "None"
                    else :
                        Isoniazid = "unknown"
                    
                    if "Kanamycin" in data[sample]['susceptibility'] :
                        Kanamycin = data[sample]['susceptibility']["Kanamycin"]["predict"]
                        if data[sample]['susceptibility']["Kanamycin"]["predict"] != 'S':
                            Kanamycin_pr = str("/".join(data[sample]['susceptibility']["Kanamycin"]["called_by"]))
                        else :
                            Kanamycin_pr = "None"
                    else :
                        Kanamycin = "unknown"
                    
                    if "Ethambutol" in data[sample]['susceptibility'] :
                        Ethambutol = data[sample]['susceptibility']["Ethambutol"]["predict"]
                        if data[sample]['susceptibility']["Ethambutol"]["predict"] != 'S':
                            Ethambutol_pr = str("/".join(data[sample]['susceptibility']["Ethambutol"]["called_by"]))
                        else :
                            Ethambutol_pr = "None"
                    else :
                        Ethambutol = "unknown"
                    
                    if "Streptomycin" in data[sample]['susceptibility'] :
                        Streptomycin = data[sample]['susceptibility']["Streptomycin"]["predict"]
                        if data[sample]['susceptibility']["Streptomycin"]["predict"] != 'S':
                            Streptomycin_pr = str("/".join(data[sample]['susceptibility']["Streptomycin"]["called_by"]))
                        else :
                            Streptomycin_pr = "None"
                    else :
                        Streptomycin = "unknown"
                    
                    if "Ciprofloxacin" in data[sample]['susceptibility'] :
                        Ciprofloxacin = data[sample]['susceptibility']["Ciprofloxacin"]["predict"]
                        if data[sample]['susceptibility']["Ciprofloxacin"]["predict"] != 'S':
                            Ciprofloxacin_pr = str("/".join(data[sample]['susceptibility']["Ciprofloxacin"]["called_by"]))
                        else :
                            Ciprofloxacin_pr = "None"
                    else :
                        Ciprofloxacin = "unknown"
                    
                    if "Pyrazinamide" in data[sample]['susceptibility'] :
                        Pyrazinamide = data[sample]['susceptibility']["Pyrazinamide"]["predict"]
                        if data[sample]['susceptibility']["Pyrazinamide"]["predict"] != 'S':
                            Pyrazinamide_pr = str("/".join(data[sample]['susceptibility']["Pyrazinamide"]["called_by"]))
                        else :
                            Pyrazinamide_pr = "None"
                    else :
                        Pyrazinamide = "unknown"
                    
                    if "Rifampicin" in data[sample]['susceptibility'] :
                        Rifampicin = data[sample]['susceptibility']["Rifampicin"]["predict"]
                        if data[sample]['susceptibility']["Rifampicin"]["predict"] != 'S':
                            Rifampicin_pr = str("/".join(data[sample]['susceptibility']["Rifampicin"]["called_by"]))
                        else :
                            Rifampicin_pr = "None"
                    else :
                        Rifampicin = "unknown"
                    
                    if "Amikacin" in data[sample]['susceptibility'] :
                        Amikacin = data[sample]['susceptibility']["Amikacin"]["predict"]
                        if data[sample]['susceptibility']["Amikacin"]["predict"] != 'S':
                            Amikacin_pr = str("/".join(data[sample]['susceptibility']["Amikacin"]["called_by"]))
                        else :
                            Amikacin_pr = "None"
                    else :
                        Amikacin = "unknown"
                    
                    if "Capreomycin" in data[sample]['susceptibility'] :
                        Capreomycin = data[sample]['susceptibility']["Capreomycin"]["predict"]
                        if data[sample]['susceptibility']["Capreomycin"]["predict"] != 'S':
                            Capreomycin_pr = str("/".join(data[sample]['susceptibility']["Capreomycin"]["called_by"]))
                        else :
                            Capreomycin_pr = "None"
                    else :
                        Capreomycin = "unknown"  

                line = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (sample, lineage, Ofloxacin, Ofloxacin_pr, Moxifloxacin, Moxifloxacin_pr, Isoniazid, Isoniazid_pr, Kanamycin, Kanamycin_pr, Ethambutol, Ethambutol_pr, Streptomycin, Streptomycin_pr, Ciprofloxacin, Ciprofloxacin_pr, Pyrazinamide, Pyrazinamide_pr, Rifampicin, Rifampicin_pr, Amikacin, Amikacin_pr, Capreomycin, Capreomycin_pr)
                print(line, file=file_out)

        except :
            lineage = "file_missing"