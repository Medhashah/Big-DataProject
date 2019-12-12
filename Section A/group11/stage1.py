import sys
from pyspark.sql import SparkSession
from csv import reader
from pyspark import SparkContext
import gzip
from pyspark.sql import *
from pyspark.sql.functions import *
import os
from os import listdir
from os.path import isfile, join
import math
import re
import datetime
import json
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from pylab import *
import numpy as np
import sys

sc = SparkContext()
spark = SparkSession.builder.appName("finak").config("spark.some.config.option", "some-value").getOrCreate()


#Qiang Luo: I split the who 1901 filenames into 19 pieces.
#We process 100 files each time and store the information like the number of type
#Each file will come with one json file.


#QL:change the TXT name according to which part of files you want to process
txt_name = "filename300.txt"
file_txt = open(txt_name,"r")
file_names = [line.replace("\n","") for line in file_txt.readlines()]


#Define map function to decide which type data belongs to
def typeMap(x):
	if type(x) == type("a"):
		date = ""
		time = ""
		am = " am" if "am" in x.lower() else ""
		pm = " pm" if "pm" in x.lower() else ""
#There is data like "2018/01/01 12:00:00 AM"
#So we need to check whether the data contains date or time or "am"
#First time pattern hour/min
		tp = r"24:00|2[0-3]:[0-5]\d|[0-1]\d:[0-5]\d"
		time_pattern = re.compile(tp)
		time_list = time_pattern.findall(x)
		if(len(time_list)!=0):
			time = time_list[0]
#Second time pattern: hour/min/sec
		tp = r"24:00:00|2[0-3]:[0-5]\d:[0-5]\d|[0-1]\d:[0-5]\d:[0-5]\d"
		time_pattern = re.compile(tp)
		time_list = time_pattern.findall(x)
		if(len(time_list)!=0):
			time = time_list[0]
#First date pattern: year/month/day for four types of symbol "-","/",".",""
		for symbol in date_symbol:
			dp = r"\d{4}"+symbol+"\d{2}"+symbol+"\d{2}"
			date_pattern = re.compile(dp)
			date_list = date_pattern.findall(x)
			if(len(date_list)!=0):
				try:
					date = datetime.datetime.strptime(date_list[0], '%Y'+symbol+'%m'+symbol+'%d').strftime('%Y/%m/%d')
				except:
					date = ""
#Second date pattern: month/day/year for four types of symbol "-","/",".",""
		for symbol in date_symbol:
			dp = r"\d{2}"+symbol+"\d{2}"+symbol+"\d{4}"
			date_pattern = re.compile(dp)
			date_list = date_pattern.findall(x)
			if(len(date_list)!=0):
				try:
					date = datetime.datetime.strptime(date_list[0], '%m'+symbol+'%d'+symbol+'%Y').strftime('%Y/%m/%d')
				except:
					date = ""
#Datetype must have valid date
#Half of string contains date information should be considered as datetpe.
		if(date!="" and len(x)<25):
			return ("Date",date+" "+ time+ " "+ am+pm)
		return ("Text",x)
	else:
		if(type(x)==type(1.0)):
			return ("Real",x)
		elif(type(x)==type(1)):
			return ("Integer",x)
		elif(isinstance(x, datetime.datetime)):
			return("Date",str(x))
	return ("Other",x)

date_symbol = ['/',' ','-',"\."]

#create a dict to store all data information, which will be turned into json file.
col_list = []

#create a dict to count the total number of four data types of all files.
typeColumnCount= {}
typeColumnCount["Date"]= 0
typeColumnCount["Integer"]= 0
typeColumnCount["Text"]= 0
typeColumnCount["Real"]= 0

#record datatype frequency
twoFreq= []
threeFreq= []
fourFreq= []

def real_compute(x):
	threshold = (sys.float_info.max/real_count)**0.5
	res = x-mean_real
	if res > threshold or res < -threshold:
		return threshold**2
	return res**2

def int_compute(x):
	threshold = (sys.maxsize/int_count)**0.5
	res = x-mean_int
	if res > threshold or res < -threshold:
		return threshold**2
	return res**2



for name in file_names:
	df= spark.read.format('csv').options(header='true',inferschema='true', delimiter='\t').load(name)
	df.createOrReplaceTempView("df")
	rdd= df.rdd.map(list)
	total_count = rdd.count()
	column_name= df.columns
	dict_dataset = {}
	fileName = name.split('/')[4].split(".")[0]
	dict_dataset["dataset_name"] = fileName
#
	for i in range(len(column_name)):
		rSingleColumn= rdd.map(lambda x: x[i])
		rSingleColumn_count= rSingleColumn.map(lambda x: (str(x), 1))
		rNonEmpty= rSingleColumn_count.filter(lambda x: x[0] != None)
		nonEmpty_count = rNonEmpty.count()
		nonEmpty= rNonEmpty.reduceByKey(lambda x, y: x+y)
		empty_count = total_count - nonEmpty_count
		dist_count = nonEmpty.filter(lambda x: x[1]==1).count()
		freq_list = nonEmpty.sortBy(lambda x: x[1], False).keys().take(5)
#Store the information of each column
		col= {}
		col["column_name"] = column_name[i]
		col["number_non_empty_cells"] = nonEmpty_count
		col["number_empty_cells"] = empty_count
		col["number_distinct_values"] = dist_count
		col["frequent_values"] = freq_list
#Given that column may have multiple types, each type will checked for each column.
		type_list = []
		rSingleColumn_type = rSingleColumn.map(typeMap)
		rSingleColumn_date = rSingleColumn_type.filter(lambda x:x[0] == "Date")
		rSingleColumn_int = rSingleColumn_type.filter(lambda x:x[0] == "Integer").values()
		rSingleColumn_real = rSingleColumn_type.filter(lambda x:x[0] == "Real").values()
		rSingleColumn_text = rSingleColumn_type.filter(lambda x:x[0] == "Text").values()
#count the number of each data type of each column
		date_count= 0
		text_count= 0
		integer_count= 0
		real_count= 0
#if there is a date type
#date_count add one and all information of date type will be stored
		if(len(rSingleColumn_date.take(1))!=0):
			typeColumnCount["Date"]+= 1
			date_count = rSingleColumn_date.count()
			min_time = rSingleColumn_date.min()[1]
			max_time = rSingleColumn_date.max()[1]
			type_dict = {}
			type_dict["type"] = "DATE/TIME"
			type_dict["count"] = date_count
			type_dict["max_value"] = max_time
			type_dict["min_value"] = min_time
			type_list.append(type_dict)
#
		if(len(rSingleColumn_int.take(1))!=0):
			typeColumnCount["Integer"]+= 1
			int_count = rSingleColumn_int.count()
			min_int = rSingleColumn_int.min()
			max_int = rSingleColumn_int.max()
			sum_int = rSingleColumn_int.sum()
			mean_int = sum_int/int_count
			variance_int = rSingleColumn_int.map(int_compute).sum()/int_count
			std_int = variance_int**0.5
			type_dict = {}
			type_dict["type"] = "INTEGER (LONG)"
			type_dict["count"] = int_count
			type_dict["max_value"] = max_int
			type_dict["min_value"] = min_int
			type_dict["mean"] = mean_int
			type_dict["stddev"] = std_int
			type_list.append(type_dict)
#
		if(len(rSingleColumn_real.take(1))!=0):
			typeColumnCount["Real"]+= 1
			real_count = rSingleColumn_real.count()
			min_real = rSingleColumn_real.min()
			max_real = rSingleColumn_real.max()
			sum_real = rSingleColumn_real.sum()
			mean_real = sum_real/real_count
			variance_real = rSingleColumn_real.map(real_compute).sum()/real_count
			std_real = variance_real**0.5
			type_dict = {}
			type_dict["type"] = "REAL"
			type_dict["count"] = real_count
			type_dict["max_value"] = max_real
			type_dict["min_value"] = min_real
			type_dict["mean"] = mean_real
			type_dict["stddev"] = std_real
			type_list.append(type_dict)
#
		if(len(rSingleColumn_text.take(1))!=0):
			typeColumnCount["Text"]+= 1
			text_count = rSingleColumn_text.count()
			rSingleColumn_text_length = rSingleColumn_text.map(lambda x: (x,len(x)))
			rSingleColumn_text_short = rSingleColumn_text_length.sortBy(lambda x: x[1]).take(5)
			text_short = [x[0] for x in rSingleColumn_text_short]
			rSingleColumn_text_long = rSingleColumn_text_length.sortBy(lambda x: x[1],False).take(5)
			text_long = [x[0] for x in rSingleColumn_text_long]
			text_avg = rSingleColumn_text_length.values().sum()/text_count
			type_dict = {}
			type_dict["type"] = "TEXT"
			type_dict["count"] = text_count
			type_dict["shortest_values"] = text_short
			type_dict["longest_values"] = text_long
			type_dict["average_length"] = text_avg
			type_list.append(type_dict)
#
		arrUnSort= []
		arrUnSort.append(("Date", date_count))
		arrUnSort.append(("Text", text_count))
		arrUnSort.append(("Integer", integer_count))
		arrUnSort.append(("Real", real_count))
		arrUnSort.sort(key= lambda x: x[1], reverse=True)
		firstItem= arrUnSort[0]
		secondItem= arrUnSort[1]
		thridItem= arrUnSort[2]
		fourthItem= arrUnSort[3]
#Store each heterogeneous column's data types
		if secondItem[1]!= 0:
			twoFreqItem= ((firstItem[0], secondItem[0]), secondItem[1])
			twoFreq.append(twoFreqItem)
		if thridItem[1]!= 0:
			thirdFreqItem= ((firstItem[0], secondItem[0], thridItem[0]), thridItem[1])
			threeFreq.append(thirdFreqItem)
		if fourthItem[1]!= 0:
			fourthFreqItem= ((firstItem[0], secondItem[0], thridItem[0], fourthItem[0]), fourthItem[1])
			fourFreq.append(fourthFreqItem)
#
		col["data_types"] = type_list
		col_list.append(col)
#Record the number of data types of 100 files
#Change the filename if you test different collection of files
#If you process the data of "filename200.txt", then use the filename "count200" to store information.
	typeColumnCountFile = open("count300.txt","w")
	for key in typeColumnCount.keys():
		typeColumnCountFile.write(key+": "+str(typeColumnCount.get(key,0))+"\n")
	for item in twoFreq:
			typeColumnCountFile.write(item[0][0]+" "+ item[0][1]+": "+str(item[1])+"\n")
	for item in threeFreq:
		typeColumnCountFile.write(item[0][0]+" "+ item[0][1]+" "+item[0][2]+": "+str(item[1])+"\n")
	for item in fourFreq:
		typeColumnCountFile.write(item[0][0]+" "+ item[0][1]+" "+item[0][2]+" "+ item[0][3]+": "+str(item[1])+"\n")
	typeColumnCountFile.close()
#store all columns information and turn it into json file
	dict_dataset["columns"] = col_list
	with open(fileName+'.json', 'w') as fp:
		json.dump(dict_dataset, fp,default=str)




#below codes are used to generate a histogram shows the number of each data type

label_list= []
keys= typeColumnCount.keys()
label_list = [key for key in keys]

plt.bar(range(len(label_list)), [typeColumnCount.get(xtick, 0) for xtick in label_list], align='center',yerr=0.000001)
plt.xticks(range(len(label_list)), label_list)
plt.xlabel('Type')
plt.ylabel('Frequency')
plt.savefig("total.jpg")





