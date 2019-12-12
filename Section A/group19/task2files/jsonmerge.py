# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 20:51:53 2019

@author: johny
"""

import glob
import json

predicted_types = {}

read_files = glob.glob("*.json")

for f in read_files[:219]:
    with open(f, 'r', encoding='utf') as g:
        test = json.load(g)
        test = test[1]
        out = test['semantic_types']
        types = []
        for dictionary in out:
            if dictionary['semantic_type'] == None:
                dictionary['semantic_type'] = 'other'
            types.append(dictionary['semantic_type'])
        predicted_types[f] = types

for f in read_files[220:]:
    with open(f, 'r', encoding='utf') as g:
        test = json.load(g)
        test = test[1]
        out = test['semantic_types']
        types = []
        for dictionary in out:
            if dictionary['semantic_type'] == None:
                dictionary['semantic_type'] = 'other'
            types.append(dictionary['semantic_type'])
        predicted_types[f] = types
    #types[f] = test["semantic_type]
    
with open('task2groundtruth.json', 'r', encoding='utf') as g:
        labeled_types = json.load(g)
    
new_labeled_types = {}
 
for s in labeled_types.keys():
    tmp_list = []
    for item in labeled_types[s]:
        tmp_list.append(item['FIELD2'])
    new_labeled_types[s] = tmp_list
    
labeled_types = new_labeled_types
    
    
list_of_types = ['person_name', 'business_name', 'phone_number', 'address', 'street_name',
'city', 'neighborhood', 'lat_lon_cord', 'zip_code', 'borough', 'school_name',
'color', 'car_make', 'city_agency', 'area_of_study', 'subject_in_school',
'school_level', 'college_name', 'website', 'building_classification',
'vehicle_type', 'location_type', 'park_playground', 'other']

"""
for key in predicted_types.keys():
    if key in labeled_types.keys():
        for item in predicted_types[key]: #this is a list
            if item in labeled_types[key]:
"""

precision_and_recall = []

for item in list_of_types:
    counter = 0
    for key in predicted_types.keys():
        if key.rstrip(".json") in list(labeled_types.keys()):
            if (item in predicted_types[key]) and (item in labeled_types[key.rstrip(".json")]):
                counter+=1
    precision = sum(item in s for s in predicted_types.values())
    recall = sum(item in s for s in labeled_types.values())
    if precision == 0 and recall != 0:
        precision_and_recall.append([item, 'NaN', counter/sum(item in s for s in labeled_types.values())])
    elif recall == 0 and precision != 0:
        precision_and_recall.append([item, counter/sum(item in s for s in predicted_types.values()), 'NaN'])
    elif precision == 0 and recall == 0:
        precision_and_recall.append([item, 'NaN', 'NaN'])
    else:
        precision_and_recall.append([item, counter/sum(item in s for s in predicted_types.values()), counter/sum(item in s for s in labeled_types.values())])
         
# bottom half of precision   
#sum('person_name' in s for s in predicted_types.values())

# bottom half of recall
#sum('person_name' in s for s in labeled_types.values())
            
                
    
#precision =  number of columns correctly predicted as type / all columns predicted as type
#recall = number of columns correctly predicted as type / number of actual columns of type


    
