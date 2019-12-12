#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from csv import reader
import sys
from pyspark.sql import functions as F
import json 
import re
import os
from difflib import SequenceMatcher

spark = SparkSession \
     .builder \
     .appName("Task5-sql") \
     .getOrCreate()

conf = SparkConf().setAppName("task2").setMaster("local")

sc = SparkContext.getOrCreate(conf)

df = spark.read.csv(sys.argv[1], sep = '\t',header = 'false')

df.createOrReplaceTempView("table1")
######################LABELS USING REGEX#####################################
zipRegex = re.compile(r'\d{5}$|^\d{5}-\d{4}$')
#phoneNumberRegex1 = re.compile(r'([0-9]( |-)?)?(\(?[0-9]{3}\)?|[0-9]{3})( |-)?([0-9]{3}( |-)?[0-9]{4}|[a-zA-Z0-9]{7})$')
phoneNumberRegex2 =re.compile(r'\D?(\d{3})\D?\D?(\d{3})\D?(\d{4})$') 
emailRegex = re.compile(r'.+@[^\.].*\.[a-z]{2,}$')
coordinatesRegex = re.compile(r'(\()([-+]?)([\d]{1,2})(((\.)(\d+)(,)))(\s*)(([-+]?)([\d]{1,3})((\.)(\d+))?(\)))$')
websiteRegex = re.compile(r'(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$')
addressRegex = re.compile(r'(\d{3,})\s?(\w{0,5})\s([a-zA-Z]{2,30})\s([a-zA-Z]{2,15})\.?\s?(\w{0,5})$')
nameRegex = re.compile(r'([a-zA-Z]{3,30}\s*)+$')

###########################################################################

################LABELS USING DICTIONARY####################################
school = ["SCHOOL","SCHOO"]
college = ["ACADEMY","COLLEGE","TECHNOLOGY"]

street = ["AVE","AVENUE", "STREET", "ST", "BOULEVARD", "BLVD"]

vehicleType = ["SEDAN","AMBULANCE","TRUCK", "BICYCLE","BUS","CONVERTIBLE","MOTORCYCLE", "VEHICLE", "MOPED", "SCOOTER", "TAXI", "PEDICAB","BOAT","VAN"]
schoolLevel = ["K-2", "MIDDLE", "ELEMENTARY","HIGH","K-3","K-4","K-5","K-6","K-7","K-8","K-9","K-10","K-11","K-12"]

businessName = ["DELI","PIZZA","RESTAURANT","CHINESE","SHUSHI","BAR","SNACK","CAFE","COFFEE","KITCHEN", "GROCERY", "FOOD", "FARM","MARKET","WOK","GOURMET", "BURGER", "LAUNDROMAT", "WINE", "LIQUORS", "GARDEN", "DINER", "CUISINE", "PLACE", "CLEANERS", "PIZZERIA"]

subjects = ["MATH", "MATH A", "MATH B", "US HISTORY", "SCIENCE", "ENGLISH", "SOCIAL STUDIES"]

buildingClassification = ["R0-CONDOMINIUM", "R2-WALK-UP","C1-WALK-UP","C2-WALK-UP","C3-WALK-UP","C4-WALK-UP","C5-WALK-UP","C6-WALK-UP","C7-WALK-UP", "C8-WALK-UP","D0-ELEVATOR", "D1-ELEVATOR","D2-ELEVATOR","D3-ELEVATOR","D4-ELEVATOR", "D5-ELEVATOR", "D6-ELEVATOR","D7-ELEVATOR", "D8-ELEVATOR","D9-ELEVATOR"]

parks = ["PARK","PLAYGROUND", "GARDEN"]

borough = ["brooklyn","bronx", "manhattan", "queens", "staten island"]

neighborhood = sc.textFile("neighborhood.txt").collect()
city = sc.textFile("city.txt").collect()
cityAgency = sc.textFile("agency.txt").collect()
carMake = sc.textFile("carmake.txt").collect()
color = sc.textFile("colors.txt").collect()

#########################################################################
dict = {'personName': 0, 'businessName': 0, 'phoneno':0, 'address':0, 'street':0,'city':0, 'neighborhood':0, 'coordinate':0, 'zip':0, 'borough':0,\
       'school':0, 'color':0, 'carMake':0, 'cityAgency':0, 'subjects':0, 'schoolLevel':0, 'college':0, 'website':0, \
       'buildingClassification':0, 'vehicleType': 0, 'park':0, 'other':0}

count =0

def similar(a, b):
	for j in b:
		
		if SequenceMatcher(None, a.lower(), j.lower()).ratio() > 0.75:
			return True
	return False

def similarity(a, b):
	for i in a:
		for j in b:
			if (SequenceMatcher(None, i.lower(), j.lower()).ratio() >= 0.75):
				return True
	return False


column_data = spark.sql("SELECT _c0 as attr1,_c1 as attr2 from table1 ").collect()

for row in column_data:
	if(row.attr1 is not None):
		if similar(row.attr1, borough):
			dict['borough']+= int(row.attr2)
		elif similar(row.attr1, neighborhood):
			dict['neighborhood'] += int(row.attr2)
		elif similar(row.attr1, city):
			dict['city'] +=int(row.attr2)
		elif similar(row.attr1, cityAgency):
			dict['cityAgency'] +=int(row.attr2)
		elif similar(row.attr1, carMake):
			dict['carMake'] +=int(row.attr2)                    
		elif similar(row.attr1, color):
			dict['color'] +=int(row.attr2)
		elif(re.match(zipRegex, row.attr1)):
			dict['zip'] +=int(row.attr2)
		elif(re.match(phoneNumberRegex2, row.attr1)):
			dict['phoneno'] +=int(row.attr2)
		elif(re.match(emailRegex,row.attr1)):
			dict['email'] +=int(row.attr2)
		elif(re.match(coordinatesRegex, row.attr1)):
			dict['coordinate'] +=int(row.attr2)
		elif(re.match(addressRegex,row.attr1)):
			dict['address'] += int(row.attr2)
		elif similarity(set((row.attr1).split()), street):
			dict['street'] += int(row.attr2)
		elif similar(row.attr1, subjects):
			dict['subjects'] +=int(row.attr2)                
		elif(re.match(websiteRegex,row.attr1)):
			dict['website'] +=int(row.attr2)
		elif similarity(set((row.attr1).split()), school):
			dict['school'] +=int(row.attr2)
		elif similarity(set((row.attr1).split()), college):
			dict['college'] +=int(row.attr2)
		elif similarity(set((row.attr1).split()), vehicleType):
			dict['vehicleType'] +=int(row.attr2)		
		elif similarity(set((row.attr1).split()), schoolLevel):				
			dict['schoolLevel'] +=int(row.attr2)
		elif similarity(set((row.attr1).split()), businessName):
			dict['businessName'] +=int(row.attr2)		
		elif similarity(set((row.attr1).split()), buildingClassification):
			dict['buildingClassification'] +=int(row.attr2)
		elif similarity(set((row.attr1).upper().split()), parks):
			dict['park'] +=int(row.attr2)
		elif (re.match(nameRegex,row.attr1)):	
			dict['personName'] +=int(row.attr2)
print(dict)


max1 = max(dict, key=dict.get)
count1 = dict[max1]
finalcount = count1
semantic_label = max1
del dict[max1]
max2 = max(dict, key=dict.get)
count2 = dict[max2]
del dict[max2]
max3 = max(dict, key=dict.get)
count3 = dict[max3]
if count1 > 0:
	if ((count1-count2)/count1) < 0.5 :
		semantic_label = max1 + ", " + max2
		finalcount +=count2    
		if ((count1-count3)/count1) < 0.5:
			semantic_label = max1 + ", " + max2 + ", " + max3
			finalcount +=count3
        
##############JSON IMPLEMENTATION##################

result_json = {
    "dataset_name": sys.argv[1].split('/')[-1]
    }
result_json["semantic_types"]=list()

column_json={
		"semantic_type": semantic_label,
		"count":finalcount,
	}

result_json["semantic_types"].append(column_json)

filename=sys.argv[1].split('/')[-1]+ ".json"

with open(filename, 'w') as f:
    json.dump(result_json, f)

##################################################


print(sys.argv[1].split('/')[-1])
print("label:",semantic_label)


sc.stop()
