import csv
import json
import pandas as pd
import numpy as np

index = ['person_name', 'business_name', 'phone_number', 'address', 'street_name', 'city',
         'neighborhood', 'lat_lon_cord', 'zip_code', 'borough', 'school_name',
         'color', 'car_make', 'city_agency', 'area_of_study', 'subject_in_school',
         'school_level', 'college_name', 'website', 'building_classification', 'vehicle_type',
         'location_type', 'park_playground', 'other']

column = ['correctly predicted as type',
          'total columns of type', 'total columns predicted as type']

df = pd.DataFrame(index=index, columns=column).fillna(0)

file_list = open('cluster3.txt').readline().strip().replace(' ', '').split(",")
for file_name in file_list:
    file_name = file_name[1:-1]
    try:
        with open(file_name+"_semantic_result.json", 'r') as load_f:
            load_dict = json.load(load_f)
            print(load_dict)
            df['total columns of type'][load_dict['true_type']] += 1
            df['total columns predicted as type'][load_dict['prediction type']] += 1
            if load_dict['prediction type'] == load_dict['true_type']:
                df['correctly predicted as type'][load_dict['true_type']] += 1
    except Exception:
        print(file_name)
