import sys
import string
import json
import os

### cluster
fileLst = []
with open('./cluster1.txt', 'r') as f:
    contentStr = f.read()
    fileLst = contentStr.replace('[',"").replace(']',"").replace("'","").replace("\n","").split(', ')
with open('./task2-manual-labels.json', 'w') as manF:
    actTDct = {}
    actTDct['actual_types'] = ['address', 'area_of_study', 'borough', 'building_classification', 'business_name', 'car_make', 'city', 'city_agency', 'college_name', 'color', 'lat_lon_cord', 'location_type', 'neighborhood', 'other', 'park_playground', 'person_name', 'phone_number', 'school_level', 'school_name', 'street_name', 'subject_in_school', 'vehicle_type', 'website', 'zip_code']
    manF.write((json.dumps(actTDct,indent=1)) + "\n")
    with open('./labellist.txt') as f:
        labels = f.readlines()
        for i in range(len(fileLst)):
            fstr = fileLst[i].split('.')
            datasetName = fstr[0]
            colName = fstr[1]
            label = labels[i]
            label = label.replace("\n","")
            llst = label.split(" ")[1].split(",")
            pdct = {}
            pdct['dataset_name'] = datasetName
            pdct['column_name'] = colName
            pdct['manual_labels'] = []
            for label in llst:
                ldct = {}
                ldct['semantic_type'] = label
                pdct['manual_labels'].append(ldct)
            manF.write((json.dumps(pdct,indent=1))+"\n")