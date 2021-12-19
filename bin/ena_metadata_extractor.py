#!/usr/bin/env python3
#-*- coding: latin-1 -*-

import re
import csv
import json
import requests
import sys


#######################################################################################################

dataset = set()

dataset_file = sys.argv[1]
file_date = str(sys.argv[2])
metadata_file = "./ena_metadata_%s.tsv" % (file_date)

#######################################################################################################

with open(dataset_file) as data_file :
    reader = csv.DictReader(data_file, dialect='excel-tab')
    for row in reader:
        sample = row['subject_id']
        dataset.add(sample)

with open(metadata_file, "w+") as out_file :
    header = 'sample_name\tgeo_loc\tcenter\ttitle\tcollection_date\trelease_date\tisolate_source\tsample_type\thost\thost_health_state\thost_age\thost_disease_outcome\thost_hiv_status\thost_hiv_status_diagnosis_postmortem\thost_anti-retroviral_status\thost_sex\thost_location_sampled'
    print(header, file=out_file)

    for sample in dataset :
        target_url = "https://www.ebi.ac.uk/biosamples/samples/%s.json" % (sample)
        
        try:
            response = requests.get(target_url)
            if response.status_code != requests.codes['ok']:
                raise Exception('Error requesting data. Error code: ' + str(response.status_code) + '\n' + target_url)
            # parsed_json = json.loads(requests.get(target_url).text)
            parsed_json = response.json()
        except:
            raise Exception('Error accessing BioSample\'s json for sample ' + sample)


#######################################################################################################

#GEOLOCATION AND CENTER

        try : 
            loc_dict = parsed_json["characteristics"]["geographicLocation"]
            geo_loc = loc_dict[0]["text"]
        except :
            geo_loc = "unknown"
        try :
            if geo_loc == 'unknown' :
                loc_dict = parsed_json["characteristics"]["geographicLocationCountryAndOrSea"]
                geo_loc = loc_dict[0]["text"]
        except :
            geo_loc = "unknown"
        try :
            if geo_loc == 'unknown' :
                loc_dict = parsed_json["characteristics"]["geo loc name"]
                geo_loc = loc_dict[0]["text"]
        except :
            geo_loc = "unknown"
        try:
            if geo_loc == 'unknown' :
                loc_dict = parsed_json["characteristics"]["geographic location (region and locality)"]
                geo_loc = loc_dict[0]["text"]
        except :
            geo_loc = "unknown"
        try:
            if geo_loc == 'unknown' :
                loc_dict = parsed_json["characteristics"]["geographic location (country and/or sea region)"]
                geo_loc = loc_dict[0]["text"]
        except :
            geo_loc = "unknown"
        try:
            if geo_loc == 'unknown' :
                loc_dict = parsed_json["characteristics"]["geographic location (country and/or sea)"]
                geo_loc = loc_dict[0]["text"]
        except :
            geo_loc = "unknown"
        try:
            if geo_loc == 'unknown' :
                loc_dict = parsed_json["characteristics"]["geographic location (countryand/orsea,region)"]
                geo_loc = loc_dict[0]["text"]
        except :
            geo_loc = "unknown"

        try:
            center_dict = parsed_json["characteristics"]["INSDC center name"]
            center = center_dict[0]["text"]
        except :
            center = 'unknown'
        try:
            if center == 'unknown' :
                center_dict = parsed_json["characteristics"]["collected by"]
                center = center_dict[0]["text"]
        except :
            center = 'unknown'

        try:
            title_dict = parsed_json["characteristics"]["title"]
            title = title_dict[0]["text"]
        except :
            title = 'unknown'

#######################################################################################################

#HOST AND ISOLATE SOURCE

        try:
            iso_s_dict = parsed_json["characteristics"]["isolation-source"]
            iso_s = iso_s_dict[0]["text"]
        except :
            iso_s = 'unknown'
        try:
            if iso_s == 'unknown' :
                iso_s_dict = parsed_json["characteristics"]["isolation source"]
                iso_s = iso_s_dict[0]["text"]
        except :
            iso_s = 'unknown'
        try:
            if iso_s == 'unknown' :
                iso_s_dict = parsed_json["characteristics"]["isolation_source"]
                iso_s = iso_s_dict[0]["text"]
        except :
            iso_s = 'unknown'
        try:
            stype_dict = parsed_json["characteristics"]["sample type"]
            stype = stype_dict[0]["text"]
        except :
            stype = 'unknown'

        try:
            host_dict = parsed_json["characteristics"]["host"]
            host = host_dict[0]["text"]
        except :
            host = 'unknown'
        try:
            if host == 'unknown' :
                host_dict = parsed_json["characteristics"]["specific host"]
                host = host_dict[0]["text"]
        except :
            host = 'unknown'
        try:
            if host == 'unknown' :
                host_dict = parsed_json["characteristics"]["host scientific name"]
                host = host_dict[0]["text"]
        except :
            host = 'unknown'
        try:
            host_hs_dict = parsed_json["characteristics"]["host health state"]
            host_hs = host_hs_dict[0]["text"]
        except :
            host_hs = 'unknown'

#######################################################################################################          

#PATIENT INFO

        try:
            h_age_dict = parsed_json["characteristics"]["host age"]
            h_age = h_age_dict[0]["text"]
        except :
            h_age = 'unknown'
        try:
            h_do_dict = parsed_json["characteristics"]["host_disease_outcome"]
            h_do = h_do_dict[0]["text"]
        except :
            h_do = 'unknown'
        try:
            h_hiv_dict = parsed_json["characteristics"]["host_hiv_status"]
            h_hiv = h_hiv_dict[0]["text"]
        except :
            h_hiv = 'unknown'
        try:
            h_sdp_dict = parsed_json["characteristics"]["host_hiv_status_diagnosis_postmortem"]
            h_sdp = h_sdp_dict[0]["text"]
        except :
            h_sdp = 'unknown'
        try:
            h_ars_dict = parsed_json["characteristics"]["host_anti-retroviral_status"]
            h_ars = h_ars_dict[0]["text"]
        except :
            h_ars = 'unknown'
        try:
            h_sex_dict = parsed_json["characteristics"]["host sex"]
            h_sex = h_sex_dict[0]["text"]
        except :
            h_sex = 'unknown'
        try:
            h_loc_dict = parsed_json["characteristics"]["host_location_sampled"]
            h_loc = h_loc_dict[0]["text"]
        except :
            h_loc = 'unknown'


######################################################################################################

#DATE

        try :
            release = parsed_json["releaseDate"]
        except :
            release = 'unknown'
        try:
            coll_dict = parsed_json["characteristics"]["collection date"]
            coll = coll_dict[0]["text"]
        except :
            coll = 'unknown'

#######################################################################################################

        line = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (sample, geo_loc, center, title, coll, release, iso_s, stype, host, host_hs, h_age, h_do, h_hiv, h_sdp, h_ars, h_sex, h_loc)
        print (line, file=out_file)

print("[DEBUG] Metadata extraction successful.")
