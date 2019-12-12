#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import gzip
import json
import math
import os
import sys
import csv
import re

r_phone = re.compile("^\D?(\d{3})\D?\D?(\d{3})\D?(\d{4})$")


METADATA_DIR = './metadata/'

KEEP_RATIO = 0.3


from names_dataset import NameDataset
name_dataset = NameDataset()

def is_middle_name(item):
    return len(item) == 1 and item[0] >='A' and item[0] <='Z'

def detect_person_name(item):
    return name_dataset.search_first_name(item) or name_dataset.search_last_name(item) or is_middle_name(item)


def load_business_names():
    # https://github.com/9b/heavy_pint/blob/master/lists/business-names.txt
    with open(METADATA_DIR + 'business-names.txt',encoding = "ISO-8859-1") as fin:
        business_names = set(line.strip().lower() for line in fin)
    business_names |= set(b.split()[0] for b in business_names)
    return business_names

#business_names_dataset = load_business_names()

def detect_business_name(item):
    suffix=["corporation","corp","corp.","inc","inc.","llc","llc.","l.l.c","company","incorporated","ltd","ltd.","limited","service"]
    for s in suffix:
        if item.lower().endswith(s):
            return True
    return False


def detect_phone_number(item):
    if r_phone.match(item):
        return True
    else:
        return False
    


def detect_address(item):
    suffix = ['street', 'road', 'avenue','st','ave','rd','pl','ct','blvd','dr']
    item = item.lower()
    if not item.split()[0].replace('-', '').isdigit():
        return False
    for s in suffix:
        if item.endswith(s):
            return True
    return False


def detect_street_name(item):
    suffix = ['street', 'road', 'avenue','st','ave','rd','pl','ct','blvd','dr']
    item = item.lower()
    if item.split()[0].replace('-', '').isdigit():
        return False
    for s in suffix:
        if item.endswith(s):
            return True
    return False



def load_cities():
    # https://github.com/datasets/world-cities
    cities = set()
    with open(METADATA_DIR + 'world-cities.txt', encoding='utf8') as fin:
        for i, cells in enumerate(csv.reader(fin)):
            if i == 0:
                continue
            
            c1, _, c2, _ = cells
            
            
            cities.add(c1.lower())
            cities.add(c2.lower())

    return cities

city_dataset = load_cities()

def detect_city(item):

    return item.lower() in city_dataset
     


def load_neighborhood():
    areas_of_study = set()
    with open(METADATA_DIR + 'neighborhood.txt') as fin:
        line = fin.readline()
        while line:
            areas_of_study.add(line.strip().lower())
            line = fin.readline()
    return areas_of_study
    # https://en.wikipedia.org/wiki/Neighborhoods_in_New_York_City
    
    return areas_of_study

neighborhood_dataset = load_neighborhood()

def detect_neighborhood(item):
    return item.lower() in neighborhood_dataset


def detect_lat_lon(item):
    if not (item[0] == '(' and item[-1] == ')'):
        return False
    xs = item.split(',')
    if len(xs) != 2:
        return False
    lat, lon = xs
    lat = lat.strip()[1:]
    lon = lon.strip()[:-1]
    try:
        float(lat)
        float(lon)
    except:
        return False
    return True


def detect_zip_code(item):
    return len(item) == 5 and item.isdigit()


def detect_borough(item):
    # https://en.wikipedia.org/wiki/Boroughs_of_New_York_City
    boroughs = {'BROOKLYN', 'MANHATTAN', 'BRONX', 'QUEENS', 'STATEN ISLAND',"BK","BX","MN","QN","SI",'K', 'M', 'Q', 'R', 'X'}
    return item.upper() in boroughs


def detect_school_name(item):
    keywords = [' hs',' sc','hs ','sc ','acad','i.s.','p.s.','h.s.','m.s.','school']
    item = item.lower()
    for k in keywords:
        if k in item:
            return True
    return False


def load_colors():
    # https://github.com/meodai/color-names
    colors = set()
    with open(METADATA_DIR + 'colornames.txt', encoding='utf8') as fin:
        for i, cells in enumerate(csv.reader(fin)):
            if i == 0:
                continue
            c1, _ = cells
            colors.add(c1.lower())
    return colors

color_dataset = load_colors()

def detect_color(item):
    return item.lower() in color_dataset


def load_car_make():
    # https://gist.github.com/pcgeek86/78f4cad29dd16961ceeeee654127a0db
    with open(METADATA_DIR + 'Car_Manufacturers.txt') as fin:
        car_make = set(line.strip().lower() for line in fin)
    car_make |= set(cm[:5] for cm in car_make)
    return car_make

car_make_dataset = load_car_make()

def detect_car_make(item):
    return item.lower() in car_make_dataset
agencies = {'DFTA', 'DOB', 'DCP', 'DCAS', 'DCWP', 'DOC', 'NYCD', 'DCLA', 'DDC', 'DOE',
        'NYCEM', 'DEP', 'DOF', 'FDNY', 'DOHMH', 'DHS', 'HPD', 'HRA', 'DSS', 'DOITT', 'DOI',
        'LAW', 'PARKS', 'NYPD', 'DOP', 'DORIS', 'DSNY', 'SBS', 'DOT', 'DYCD', 'OMB', 'MOFTB',
        'WKDEV', 'WDB', 'OCB', 'OATH', 'BSA', 'BIC', 'CFB', 'IBO', 'LMEC', 'TLC', 'PPB', 'FCRC',
        'RGB', 'CCRB', 'BOC', 'CCHR', 'COIB', 'CSC', 'LPC'}
agencies2 = {'THE DEPARTMENT FOR THE AGING', 'THE DEPARTMENT OF BUILDINGS', 'THE DEPARTMENT OF CITY PLANNING', 'THE DEPARTMENT OF CITYWIDE ADMINISTRATIVE SERVICES', 'THE DEPARTMENT OF CONSUMER AND WORKER PROTECTION', 'THE DEPARTMENT OF CONSUMER AFFAIRS', 'THE DEPARTMENT OF CORRECTION', 'THE DEPARTMENT OF CULTURAL AFFAIRS', 'THE DEPARTMENT OF DESIGN & CONSTRUCTION', 'THE DEPARTMENT OF EDUCATION', 'THE NEW YORK CITY EMERGENCY MANAGEMENT', 'NYCEM', 'THE DEPARTMENT OF ENVIRONMENTAL PROTECTION', 'THE DEPARTMENT OF FINANCE', "THE SHERIFF'S OFFICE", 'SHERIFF', 'THE FIRE DEPARTMENT', 'THE DEPARTMENT OF HEALTH & MENTAL HYGIENE', 'DOHMH', 'THE DEPARTMENT OF HOMELESS SERVICES', 'THE DEPARTMENT OF HOUSING PRESERVATION & DEVELOPMENT', 'THE HUMAN RESOURCES ADMINISTRATION', 'DEPARTMENT OF SOCIAL SERVICES', 'THE DEPARTMENT OF INFORMATION TECHNOLOGY & TELECOMMUNICATIONS', 'DOITT', 'THE DEPARTMENT OF INVESTIGATION', "THE LAW DEPARTMENT (LAW) IS RESPONSIBLE FOR MOST OF THE CITY'S LEGAL AFFAIRS.", 'THE DEPARTMENT OF PARKS & RECREATION', 'PARKS', 'THE POLICE DEPARTMENT', 'THE DEPARTMENT OF PROBATION', 'THE DEPARTMENT OF RECORDS & INFORMATION SERVICES', 'DORIS', 'THE DEPARTMENT OF SANITATION', 'THE DEPARTMENT OF SMALL BUSINESS SERVICES', 'THE DEPARTMENT OF TRANSPORTATION', 'THE DEPARTMENT OF YOUTH & COMMUNITY DEVELOPMENT', 'THE OFFICE OF MANAGEMENT AND BUDGET', 'THE NEW YORK CITY OFFICE OF WORKFORCE DEVELOPMENT', 'WKDEV', 'THE NEW YORK CITY WORKFORCE DEVELOPMENT BOARD', 'THE NEW YORK CITY OFFICE OF COLLECTIVE BARGAINING', 'THE OFFICE OF ADMINISTRATIVE TRIALS AND HEARINGS', 'THE NEW YORK CITY BOARD OF STANDARDS AND APPEALS', 'THE BUSINESS INTEGRITY COMMISSION', 'THE CAMPAIGN FINANCE BOARD', 'THE INDEPENDENT BUDGET OFFICE', 'THE LATIN MEDIA & ENTERTAINMENT COMMISSION', 'THE NEW YORK CITY PROCUREMENT POLICY BOARD', 'THE NEW YORK CITY FRANCHISE AND CONCESSION REVIEW COMMITTEE', 'THE NEW YORK CITY TAX APPEALS TRIBUNAL', 'THE NEW YORK CITY TAX COMMISSION', 'THE NEW YORK CITY BANKING COMMISSION', 'THE NEW YORK CITY LOFT BOARD', 'THE NEW YORK CITY RENT GUIDELINES BOARD', 'THE NEW YORK CITY CIVILIAN COMPLAINT REVIEW BOARD', 'THE NEW YORK CITY BOARD OF CORRECTION', 'THE NEW YORK CITY COMMISSION ON HUMAN RIGHTS', 'NEW YORK CITY COMMUNITY ASSISTANCE UNIT', 'THE NEW YORK CITY CLERK', 'THE NEW YORK CITY MARRIAGE BUREAU', 'THE NEW YORK CITY CONFLICTS OF INTEREST BOARD', 'THE NEW YORK CITY PUBLIC DESIGN COMMISSION', 'ART COMMISSION', 'THE NEW YORK CITY CIVIL SERVICE COMMISSION', 'THE NEW YORK CITY LANDMARKS PRESERVATION COMMISSION', 'THE NEW YORK CITY IN REM FORECLOSURE RELEASE BOARD', 'NEW YORK CITY VOTER ASSISTANCE COMMISSION', 'OFFICE OF CHIEF MEDICAL EXAMINER OF THE CITY OF NEW YORK', 'CHILD WELFARE BOARD', 'THE NEW YORK CITY OFFICE OF THE ACTUARY', 'CITY UNIVERSITY OF NEW YORK', 'NEW YORK CITY BOARD OF EDUCATION', 'NEW YORK CITY ECONOMIC DEVELOPMENT CORPORATION', 'NEW YORK CITY HEALTH AND HOSPITALS CORPORATION', 'NEW YORK CITY HOUSING AUTHORITY', 'NEW YORK CITY SCHOOL CONSTRUCTION AUTHORITY', 'NEW YORK CITY SOIL AND WATER CONSERVATION DISTRICT'}

def detect_city_agency(item):
    
    # https://en.wikipedia.org/wiki/List_of_New_York_City_agencies


    return item.upper() in agencies or item.upper() in agencies2


def load_subject():
    # https://github.com/fivethirtyeight/data/blob/master/college-majors/majors-list.csv
    areas_of_study = set()
    with open(METADATA_DIR + 'subject.txt') as fin:
        line = fin.readline()
        while line:
            areas_of_study.add(line.strip().lower())
            line = fin.readline()
    return areas_of_study
def load_areas_of_study():
    
    areas_of_study = set()
    with open(METADATA_DIR + 'area_study.txt') as fin:
        line = fin.readline()
        while line:
            areas_of_study.add(line.strip().lower())
            line = fin.readline()
    return areas_of_study

areas_of_study_dataset = load_areas_of_study()
subject_dataset = load_subject()

def detect_areas_of_study(item):
    return item.lower() in areas_of_study_dataset


def detect_subject(item):
    
    return  item.lower() in subject_dataset


def detect_school_levels(item):
    school_levels = {
        'elementary', 'elementary school', 'high school', 'middle', 'high school transfer',
        'transfer school', 'yabc', 'd75'}
    item = item.lower()
    if item in school_levels:
        return True
    if item.startswith('k-') and item.replace('k-', '').isdigit():
        return True
    return False


def detect_university(item):
    keywords = ['university', 'college']
    item = item.lower()
    for k in keywords:
        if k in item:
            return True
    return False

import validators

def detect_websites(item):
    if not item.startswith('http'):
        item = 'http://' + item
    if validators.url(item) == True:
        return True
    return False


def detect_building_classification(item):
    keywords = ['-elevator', '-walk-up']
    item = item.lower()
    for k in keywords:
        if item.endswith(k):
            return True
    return False

def load_vehicle_type():
    
    areas_of_study = set()
    with open(METADATA_DIR + 'vehicle.txt') as fin:
        line = fin.readline()
        while line:
            areas_of_study.add(line.strip().lower())
            line = fin.readline()
    return areas_of_study
vehicle_dataset = load_vehicle_type()
def detect_vehicle_type(item):
    item = item.lower()
  
    return item in vehicle_dataset


def detect_type_of_location(item):
    keywords = ['SUBWAY', 'BUILDING', 'CLUB', 'HOSPITAL', 'FACILITY', 'HOUSE', 'TERMINAL', 'STORE', 'HOUSING', 'GARAGE', 'HOTEL']
    item = item.upper()
    for k in keywords:
        if k in item:
            return True
    return False


def detect_parks_playgrounds(item):
    item = item.lower()
    return 'park' in item or 'playground' in item


DETECTORS = {
    'person_name': detect_person_name,
    'business_name': detect_business_name,
    'phone_number': detect_phone_number,
    'address': detect_address,
    'street_name': detect_street_name,
    'city': detect_city,
    'neighborhood': detect_neighborhood,
    'lat_lon_cord': detect_lat_lon,
    'zip_code': detect_zip_code,
    'borough': detect_borough,
    'school_name': detect_school_name,
    'color': detect_color,
    'car_make': detect_car_make,
    'city_agency': detect_city_agency,
    'area_of_study': detect_areas_of_study,
    'subject_in_school': detect_subject,
    'school_level': detect_school_levels,
    'college_name': detect_university,
    'website': detect_websites,
    'building_classification': detect_building_classification,
    'vehicle_type': detect_vehicle_type,
    'location_type': detect_type_of_location,
    'park_playground': detect_parks_playgrounds,
}


def main(input_columns_file, input_dir, out_columns_dir):
    os.system('mkdir -p %s' % out_columns_dir)
    fpredit = open('columns_type_predicted.txt', 'w')
    na_array=["na","none","unspecified","n/a","-","other"]
    #file = open("columns.txt","r")
    #filelist = []
    #line = file.readline()
    #while line:
     #   filelist.append(line.strip()[0:-3])
      #  line = file.readline()
    #file.close()

    for i, line in enumerate(open(input_columns_file)):
    #for input_file in filelist:
        filename = line.strip().split('/')[-1]
        print('%d: %s' % (i+1, filename))
        input_file = '%s/%s' % (input_dir, filename.replace('.gz', ''))
        dataset_name_prefix, column_name, _, _ = filename.split('.')
        dataset_name = '%s.tsv.gz' % dataset_name_prefix
        
        type2cnt = {}
        count_sum = 0.
        with open(input_file, encoding='utf8') as fin:
            for _line in fin:
                xs = _line.rstrip().split('\t')
                item = '\t'.join(xs[:-1]).strip()
                count = int(xs[-1])
                if len(item) == 0 or item.lower() in na_array:
                    continue
                count_sum += count
              
                for semantic_type, detector in DETECTORS.items():
                    if detector(item):
                        type2cnt[semantic_type] = type2cnt.get(semantic_type, 0) + count
        #print(type2cnt)
        
        if 'color' in type2cnt and type2cnt['color']/count_sum > 0.2 and ('borough' in type2cnt and type2cnt['borough']/count_sum <0.9):
            type2cnt = {k:v for k,v in type2cnt.items() if k=='color'}
        else:
            type2cnt = {k:v for k,v in type2cnt.items() if v / count_sum > KEEP_RATIO}

            if 'person_name' in type2cnt and 'borough' in type2cnt:
                del type2cnt['person_name']
            if 'website' in type2cnt and 'school_name' in type2cnt:
                del type2cnt['school_name']
            if 'school_name' in type2cnt and 'school_level' in type2cnt:
                del type2cnt['school_name']
        

        
  
        
        
        column = {'column_name': column_name, 'semantic_types': []}
        semantic_count = 0
        for semantic_type, count in type2cnt.items():
            column['semantic_types'].append({
                'semantic_type': semantic_type,
               })
            semantic_count += count

        if semantic_count < count_sum:
            column['semantic_types'].append({
                'semantic_type': 'other',
                'count': int(count_sum - semantic_count)
                })
       # print(type2cnt)
        
        
        output = column
        output_file = '%s/%s.%s.json' % (out_columns_dir, dataset_name_prefix, column_name)
        with open(output_file, 'w') as fout:
            fout.write(json.dumps(output, indent=4))
        fpredit.write('%s\t%s\n' % (','.join(list(type2cnt.keys())), input_file+".gz"))
    fpredit.close()


if __name__ == '__main__':
    input_columns_file = sys.argv[1]
    input_dir = sys.argv[2]
    out_columns_dir = sys.argv[3]
    main(input_columns_file, input_dir, out_columns_dir)

