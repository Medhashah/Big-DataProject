#!/usr/bin/env python
# coding: utf-8

import os, sys
import pyspark
import string
import json
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import upper, lower, col

def predict_result(col_list, pre_name, index):
    if pre_name != "other":
        for i in range(len(col_list)):
            temp = col_list[i][0]
            if predict_by_name(pre_name, temp) == pre_name:
                value_arr[index] += 1
    else:
        for i in range(len(col_list)):
            temp = col_list[i][0]
            temp = str(temp).strip()
            temp = temp.split(' ')
            s = check_other_category(temp)
            for t in range(23):
                if s == value_name[t]:
                    value_arr[t] += 1

def predict_by_name(pre_name, value):
    phone_pattern = r"^\s*(\+?(\d{1,3}))?[-. (]*\(?\d{3}\)?[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"
    lat_lon_cord_pattern = r"^\s*\(?-?(\d{0,3}\.\d),\s*-?(\d{0,3}\.\d)\)\s*$"
    zip_code_pattern = r"^\s*\d{5}-?(\d{4})?\s*$"
    building_classification_pattern = r"^\s*[A-Za-z]\d{1}-.*\s*$"
    website_pattern = r"(WWW\.)?(HTTP(s)?:\/\/)?([\w]+(-?[\w])*\.)+[\w]{2,}(\/\S*)?$|([\w]+(-?[\w])*\.)+[\w]{2,}(\.NET)?(\.ORG)?(\.COM)?(\/\S*)?$"
    special_char_pattern = r'^[|\^&+%*#=!>0-9]+.*$'
 
    if pre_name == "person_name":
        if re.match(special_char_pattern, str(value)):
            return "other"
        else:
            return "person_name"
    elif pre_name == "business_name":
        return "business_name"
    elif pre_name == "phone_number":
        if re.match(phone_pattern, str(value)):
            return "phone_number"
        else:
            return "other"
    elif pre_name == "lat_lon_cord":
        if re.match(lat_lon_cord_pattern, str(value)):
            return "lat_lon_cord"
        else:
            return "other"
    elif pre_name == "building_classification":
        if re.match(building_classification_pattern, str(value)):
            return "building_classification"
        else:
            return "other"
    elif pre_name == "zip_code":
        if re.match(zip_code_pattern, str(value)):
            return "zip_code"
        else:
            return "other"
    elif pre_name == "website":
        if re.match(website_pattern, str(value)):
            return "website"
        else:
            return "other"

    s = str(value).strip()
    s = s.split(' ')   
    if pre_name == "address":
        for i in range(len(s)):
            if s[i].upper() in list_json['address_list']:
                return "address"
            else:
                return "other"
    elif pre_name == "street_name":
        for i in range(len(s)):
            if s[i].upper() in list_json['address_list']:
                return "street_name"
            else:
                return "other"
    elif pre_name == "borough":
        for i in range(len(s)):
            if s[i].upper() in list_json['borough_list']:
                return "borough"
            else:
                return "other"
    elif pre_name == "city":
        for i in range(len(s)):
            if s[i].upper() in list_json['city_list']:
                return "city"
            else:
                return "other"
    elif pre_name == "neighborhood":
        for i in range(len(s)):
            if s[i].upper() in list_json['neighborhood_list']:
                return "neighborhood"
            else:
                return "other"
    elif pre_name == "color":
        for i in range(len(s)):
            if s[i].upper() in list_json['color_list']:
                return "color"
            else:
                return "other"
    elif pre_name == "city_agency":
        for i in range(len(s)):
            if s[i].upper() in list_json['city_agency_list']:
                return "city_agency"
            else:
                return "other"
    elif pre_name == "area_of_study":
        for i in range(len(s)):
            if s[i].upper() in list_json['area_of_study_list']:
                return "area_of_study"
            else:
                return "other"
    elif pre_name == "school_name":
        for i in range(len(s)):
            if s[i].upper() in list_json['school_name_list']:
                return "school_name"
            else:
                return "other"
    elif pre_name == "subject_in_school":
        for i in range(len(s)):
            if s[i].upper() in list_json['subject_in_school_list']:
                return "subject_in_school"
            else:
                return "other"
    elif pre_name == "school_level":
        for i in range(len(s)):
            if s[i].upper() in list_json['school_level_list']:
                return "school_level"
            else:
                return "other"
    elif pre_name == "college_name":
        for i in range(len(s)):
            if s[i].upper() in list_json['college_name_list']:
                return "college_name"
            else:
                return "other"
    elif pre_name == "car_make":
        for i in range(len(s)):
            if s[i].upper() in list_json['car_make_list']:
                return "car_make"
            else:
                return "other"
    elif pre_name == "vehicle_type":
        for i in range(len(s)):
            if s[i].upper() in list_json['vehicle_type_list']:
                return "vehicle_type"
            else:
                return "other"
    elif pre_name == "location_type":
        for i in range(len(s)):
            if s[i].upper() in list_json['location_type_list']:
                return "location_type"
            else:
                return "other"    
    elif pre_name == "park_playground":
        for i in range(len(s)):
            if s[i].upper() in list_json['park_playground_list']:
                return "park_playground"
            else:
                return "other"
    else:
        return check_other_category(s)

def check_other_category(s):
    phone_pattern = r"^\s*(\+?(\d{1,3}))?[-. (]*\(?\d{3}\)?[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"
    lat_lon_cord_pattern = r"^\s*\(?-?(\d{0,3}\.\d),\s*-?(\d{0,3}\.\d)\)\s*$"
    zip_code_pattern = r"^\s*\d{5}-?(\d{4})?\s*$"
    building_classification_pattern = r"^\s*[A-Za-z]\d{1}-.*\s*$"
    website_pattern = r"^(WWW\.)?(HTTP(s)?:\/\/)?([\w]+(-?[\w])*\.)+[\w]{2,}(\/\S*)?$|([\w]+(-?[\w])*\.)+[\w]{2,}(\.NET)?(\.ORG)?(\.COM)?(\/\S*)?$"

    for i in range(len(s)):
        if s[i].upper() in list_json['address_list']:
            return "address"
        elif s[i].upper() in list_json['address_list']:
            return "street_name"
        elif s[i].upper() in list_json['borough_list']:
            return "borough"
        elif s[i].upper() in list_json['city_list']:
            return "city"
        elif s[i].upper() in list_json['neighborhood_list']:
            return "neighborhood"
        elif s[i].upper() in list_json['color_list']:
            return "color"
        elif s[i].upper() in list_json['city_agency_list']:
            return "city_agency"
        elif s[i].upper() in list_json['subject_in_school_list']:
            return "subject_in_school"
        elif s[i].upper() in list_json['area_of_study_list']:
            return "area_of_study"
        elif s[i].upper() in list_json['school_level_list']:
            return "school_level"
        elif s[i].upper() in list_json['school_name_list']:
            return "school_name"
        elif s[i].upper() in list_json['college_name_list']:
            return "college_name"
        elif s[i].upper() in list_json['car_make_list']:
            return "car_make"
        elif s[i].upper() in list_json['vehicle_type_list']:
            return "vehicle_type"
        elif s[i].upper() in list_json['location_type_list']:
            return "location_type"
        elif s[i].upper() in list_json['park_playground_list']:
            return "park_playground"
        else:
            value = s[0]
            if re.match(phone_pattern, value):
                return "phone_number"
            elif re.match(lat_lon_cord_pattern, value):
                return "lat_lon_cord"
            elif re.match(zip_code_pattern, value):
                return "zip_code"
            elif re.match(building_classification_pattern, value):
                return "building_classification"
            elif re.match(website_pattern, value):
                return "website"
            else: 
                return "other"

def put_to_dict(v1, n1):
    list_ = []
    t1 = dict()
    if n1 != "other":
        t1['semantic_type'] = n1
        t1['count'] = v1
        list_.append(t1)
    else:
        t1['semantic_type'] = n1
        t1['label'] = "undefined"
        t1['count'] = v1
        list_.append(t1)
    return list_
    
if __name__ == "__main__":

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("proj") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    outputJSON = []

    with open("cluster3.txt", 'r') as f:
        cols = f.read().replace('\n', '').replace(' ', '').replace("'", '')
    cols = cols[1:-1]
    file_list = cols.split(',')

    with open("list.json", 'r') as f:
        list_json = json.load(f)
    with open("label.json", 'r') as f:
        label = json.load(f)

    file_id = 0
    path = '/user/hm74/NYCColumns/'

    value_frequency = [0] * 24
    value_correct = [0] * 24
    value_total = [0] * 24

    for file in file_list: 
        print("---" + str(file_id) + "th file---")
        file_id += 1
        print("File Name: ", file)
        inFile = path + file
        spark = SparkSession(sc)
        df = sqlContext.read.format('csv').options(inferschema='true', delimiter='\t').load(inFile)
        col = df.collect()

        value_arr = [0] * 23
        value_name = ["person_name", "business_name", "phone_number", "address", "street_name", "city", "neighborhood", "lat_lon_cord", "zip_code", "borough", "school_name", \
                    "color", "car_make", "city_agency", "area_of_study", "subject_in_school", "school_level", "college_name", "website", "building_classification", \
                    "vehicle_type", "location_type", "park_playground"] 

        temp_output = dict()
        temp_output['column_name'] = file
        semantic_types = []
        fileName = file.split('.')[1]
        fileName = fileName.lower()

        #person_name
        if re.match(r"^.*(first.*name|last.*name).*", fileName):
            predict_result(col, "person_name", 0)
        #business_name
        elif re.match(r"^.*(business.*name|org.*name|dba).*", fileName):
            predict_result(col, "business_name", 1)
        #phone_number
        elif re.match(r"^.*phone|fax.*", fileName):
            predict_result(col, "phone_number", 2)
        #address
        elif re.match(r"^.*address.*", fileName):
            if re.match(r"^.*zip.*", fileName):
                predict_result(col, "zip_code", 8)
            else:
                predict_result(col, "address", 3)
        #street_name
        elif re.match(r"^.*street.*", fileName):
            predict_result(col, "street_name", 4)
        #city
        elif re.match(r"^.*city.*", fileName):
            predict_result(col, "city", 5)
        #neighborhood
        elif re.match(r"^.*neighborhood.*", fileName):
            predict_result(col, "neighborhood", 6)
        #lat_lon_cord
        elif re.match(r"^.*location.*", fileName):
            predict_result(col, "lat_lon_cord", 7)
        #zip_code
        elif re.match(r"^.*zip.*", fileName):
            predict_result(col, "zip_code", 8)
        #borough
        elif re.match(r"^.*boro.*", fileName):
            predict_result(col, "borough", 9)
        #school_name
        elif re.match(r"^.*school.*name.*", fileName):
            predict_result(col, "school_name", 10)
        #color
        elif re.match(r"^.*color.*", fileName):
            predict_result(col, "color", 11)
        #car_make
        elif re.match(r"^.*vehicle.*make.*", fileName):
            predict_result(col, "car_make", 12)
        #city_agency
        elif re.match(r"^.*agency.*", fileName):
            predict_result(col, "city_agency", 13)
        #area_of_study
        elif re.match(r"^.*interest.*", fileName):
            predict_result(col, "area_of_study", 14)
        #subject_in_school
        elif re.match(r"^.*subject.*", fileName):
            predict_result(col, "subject_in_school", 15)
        #school_level
        elif re.match(r"^.*school.*level.*", fileName):
            predict_result(col, "school_level", 16)
        #college_name
        elif re.match(r"^.*college.*", fileName):
            predict_result(col, "college_name", 17)
        #website
        elif re.match(r"^.*website.*", fileName):
            predict_result(col, "website", 18)
        #building_classification
        elif re.match(r"^.*building.*classification.*", fileName):
            predict_result(col, "building_classification", 19)
        #vehicle_type
        elif re.match(r"^.*vehicle.*type.*", fileName):
            predict_result(col, "vehicle_type", 20)
        #location_type
        elif re.match(r"^.*typ.*desc.*", fileName):
            predict_result(col, "location_type", 21)
        #park_playground
        elif re.match(r"^.*(park|school).*", fileName):
            predict_result(col, "park_playground", 22)
        #other
        else:
            predict_result(col, "other", 23)

        t = []
        label_list = []
        t = label[file]
        label_list = t[1]
        for i in range(23):
            if value_name[i] in label_list:
                value_total[i] += 1
        if "other" in label_list:
            value_total[23] += 1

        total = len(col)
        for i in range(23):
            if value_arr[i] != 0:
                semantic_types += put_to_dict(value_arr[i], value_name[i])
                value_frequency[i] += 1
                total -= value_arr[i]
                if value_name[i] in label_list:
                    value_correct[i] += 1
            if total == 0:
                break
        if total != 0:
            semantic_types += put_to_dict(total, "other")
            value_frequency[23] += 1
            if "other" in label_list:
                value_correct[23] += 1


        

        temp_output['semantic_types'] = semantic_types
        outputJSON.append(temp_output)


    with open('task2.json', 'w') as f:
       json.dump(outputJSON, f)

    print("Total columns predicted as type: ", value_frequency)
    print("Correctly predicted as type: ", value_correct)
    print("Total columns of type by label.json: ", value_total)

    text = open("precision_and_recall.txt", 'w')
    for i in range(23):
        text.write(value_name[i] + "\n")
        if value_frequency[i] != 0:
            precision = value_correct[i] / value_frequency[i]
        else:
            precision = 0.0
        if value_total[i] != 0:
            recall = value_correct[i] / value_total[i]
        else:
            recall = 0.0
        text.write("precision:\t %s \n"% precision)
        text.write("recall:\t %s \n"% recall)

    text.write("other\n")
    if value_frequency[23] != 0:
        precision = value_correct[23] / value_frequency[23]
    else:
        precision = 0.0
    if value_total[i] != 0:
        recall = value_correct[23] / value_total[23]
    else:
        recall = 0.0
    text.write("precision:\t %s \n"% precision)
    text.write("recall:\t %s \n"% recall)
    text.close()


