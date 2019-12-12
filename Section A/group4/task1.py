#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, BooleanType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from datetime import datetime, date
from dateutil.parser import parse
import json

sc = SparkContext()

spark = SparkSession.builder.appName("project").config("spark.some.config.option", "some-value").getOrCreate()
sqlContext = SQLContext(spark)

filePath = "/user/hm74/NYCOpenData/" + sys.argv[1] + ".tsv.gz"
fileName = sys.argv[1]

print("Processing file : %s" % filePath)

#read file as csv format using separater "\t"
fileDF = spark.read.option("sep", "\t").options(header = 'true').csv(filePath)

print(*(fileDF.columns))

columnsName = fileDF.columns

#check if a cell is integer or not
def is_integer(string):
	try:
		int(string)
		return True
	except:
		return False

isInt = F.udf(lambda string : is_integer(string), BooleanType())

#check if a cell is float or not
def is_float(string):
	try:
		float(string)
		return ("." in string)
	except:
		return False
isFloat = F.udf(lambda string : is_float(string), BooleanType())

#check if a cell is a date or not
def is_date(string, fuzzy=False):
    try: 
    	parse(string, fuzzy=fuzzy)
    	return True
    except:
    	#"201810"
    	return False

isDate = F.udf(lambda string : is_date(string), BooleanType())

dateToday = parse(str(date.today()));

def convertStrToDate(x):
	try:
		return parse(x)
	except:
		return dateToday

convertToDate = F.udf(lambda string: convertStrToDate(string), DateType())

#integer might be out of range. e.g. phone number
def convertStrToInt(x):
	try:
		return int(x)
	except:
		return 0

convertToInt = F.udf(lambda string: convertStrToInt(string), LongType())

def convertStrToFloat(x):
	try:
		return float(x)
	except:
		return 0

convertToFloat = F.udf(lambda string: convertStrToFloat(string), FloatType())

def lengthOfStr(x):
	return len(x)

lengthOfString = F.udf(lambda string: lengthOfStr(string), IntegerType())

# For integer types, using default aggregation functions to compute max, min, avg, standard deviation
def computeIntStatis(file, columnName):
	newName = "Int"
	dataWithIntegerColumn = file.where(isInt(F.col(columnName))).withColumn(newName, convertToInt(columnName)).select(newName)
	countNum = dataWithIntegerColumn.count() #count non-null values
	maxValue = dataWithIntegerColumn.agg(F.max(newName)).collect()[0][0]
	minValue = dataWithIntegerColumn.agg(F.min(newName)).collect()[0][0]
	meanValue = dataWithIntegerColumn.agg(F.mean(newName)).collect()[0][0]
	stddevValue = dataWithIntegerColumn.agg(F.stddev(newName)).collect()[0][0]
	return countNum, maxValue, minValue, meanValue, stddevValue

# For float types, using default aggregation functions to compute max, min, avg, standard deviation
def computeFloatStatis(file, columnName):
	newName = "Float"
	#remove those valuse that can be converted to integers?????
	dataWithFloatColumn = file.where(isInt(F.col(columnName)) != True).where(isFloat(F.col(columnName))).withColumn(newName, convertToFloat(columnName))
	newColumnDf = dataWithFloatColumn.select(newName)
	countNum = newColumnDf.count()
	maxValue = newColumnDf.agg(F.max(newName)).collect()[0][0]
	minValue = newColumnDf.agg(F.min(newName)).collect()[0][0]
	meanValue = newColumnDf.agg(F.mean(newName)).collect()[0][0]
	stddevValue = newColumnDf.agg(F.stddev(newName)).collect()[0][0]
	return countNum, maxValue, minValue, meanValue, stddevValue

# For date types, using default aggregation functions to compute max, min
def computeDateStatis(file, columnName):
	newColumn = "Date"
	dataWithDateColumn = file.where(isInt(F.col(columnName)) != True).where(isFloat(F.col(columnName)) != True).where(isDate(F.col(columnName))).withColumn(newColumn, convertToDate(columnName)).select(newColumn)
	countNum = dataWithDateColumn.count()
	if countNum >= 1:
		maxDate = dataWithDateColumn.where(F.col(newColumn) != dateToday).agg(F.max(newColumn)).collect()[0][0]
		minDate = dataWithDateColumn.agg(F.min(newColumn)).collect()[0][0]
	else:
		maxDate = None
		minDate = None
	return countNum, maxDate, minDate

# For date types, using default aggregation functions to compute average length, and longest values and shortest values
def computeTextStatics(file, columnName):
	textColumn = file.withColumn("length", lengthOfString(columnName))
	countNum = textColumn.count()
	avgLen = textColumn.agg(F.mean("length")).collect()[0][0]
	sortedTextColumn = textColumn.sort("length", ascending = False).limit(5).collect()
	longest_values = [sortedTextColumn[i][0] for i in range(len(sortedTextColumn))]
	sortedTextColumn = textColumn.sort("length", ascending = True).limit(5).collect()
	shortest_values = [sortedTextColumn[i][0] for i in range(len(sortedTextColumn))]
	return countNum, avgLen, longest_values, shortest_values

# deleted
def isColumnInteger(columnName):
	currectColumn = fileDF.select(columnName)
	total = currectColumn.count()
	intElementNumber = currectColumn.rdd.filter(lambda string: is_integer(string[0])).count()
	
	#if intElementNumber >= total * 0.95 :
	if intElementNumber >= 1:
		return True
	else:
		return False

def isColumnFloat(columnName):
	currectColumn = fileDF.select(columnName)
	total = currectColumn.count()
	floatElementNumber = currectColumn.rdd.filter(lambda string: is_float(string[0])).count()

	#if floatElementNumber >= total * 0.95 :
	if floatElementNumber >= 1:
		return True
	else:
		return False

def isColumnDate(columnName):
	currectColumn = fileDF.select(columnName)
	total = currectColumn.count()
	dateElementNumber = currectColumn.rdd.filter(lambda string: is_date(string[0])).count()

	#if dateElementNumber >= total * 0.95 :
	if dateElementNumber >= 1:
		return True
	else:
		return False

# compute general values for each column
def countValid(currectColumn, columnName):
	non_emptyCount = currectColumn.count()
	emptyCount = fileDF.select(columnName).count() - non_emptyCount
	distinctCount = currectColumn.distinct().count()
	topfive = currectColumn.groupBy(columnName).count().sort('count', ascending=False).limit(5).collect()
	topfive = [topfive[i][0] for i in range(len(topfive))]
	return non_emptyCount, emptyCount, distinctCount, topfive

#Write to json
dataJson = {}
dataJson["fileName"] = fileName
dataJson["columns"] = []

# iterate all the columns to get statics of each type.
for i in columnsName:
	if "." in i:
		i = "`" + i + "`"
	columnDF = fileDF.select(i).where(F.col(i).isNotNull()).cache()
	initDic = {}
	initDic["column_name"] = i
	non_emptyCount, emptyCount, distinctCount, topfive = countValid(columnDF, i)
	initDic["number_non_empty_cells"] = non_emptyCount
	initDic["number_empty_cells"] = emptyCount
	initDic["number_distinct_values"] = distinctCount
	initDic["frequent_values"] = topfive
	initDic["data_types"] = []
	stringFlag = True;
	#Integer
	
	intDataTypes = {}
	intDataTypes["types"] = "INTEGER(LONG)"
	countNum, maxValue, minValue, meanValue, stddevValue = computeIntStatis(columnDF, i)
	if countNum >= 1:
		stringFlag = False
		intDataTypes["count"] = countNum
		intDataTypes["max_value"] = maxValue
		intDataTypes["min_value"] = minValue
		intDataTypes["mean"] = meanValue
		intDataTypes["stddev"] = stddevValue
		initDic["data_types"].append(intDataTypes)

	floatDataTypes = {}
	floatDataTypes["types"] = "REAL"
	countNum, maxValue, minValue, meanValue, stddevValue = computeFloatStatis(columnDF, i)
	if countNum >= 1:
		stringFlag = False
		floatDataTypes["count"] = countNum
		floatDataTypes["max_value"] = maxValue
		floatDataTypes["min_value"] = minValue
		floatDataTypes["mean"] = meanValue
		floatDataTypes["stddev"] = stddevValue
		initDic["data_types"].append(floatDataTypes)

	dateDataTypes = {}
	dateDataTypes["types"] = "DATE/TIME"
	countNum, maxDate, minDate = computeDateStatis(columnDF, i)
	if countNum >= 1:
		stringFlag = False
		dateDataTypes["count"] = countNum
		dateDataTypes["max_value"] = str(maxDate)
		dateDataTypes["min_value"] = str(minDate)
		initDic["data_types"].append(dateDataTypes)# append()?
	
	if stringFlag:#String
		strDataTypes = {}
		strDataTypes["types"] = "TEXT"
		countNum, avgLen, longest_values, shortest_values = computeTextStatics(columnDF, i)
		strDataTypes["count"] = countNum
		strDataTypes["shortest_values"] = shortest_values
		strDataTypes["longest_values"] = longest_values
		strDataTypes["average_length"] = avgLen
		initDic["data_types"].append(strDataTypes)

	dataJson["columns"].append(initDic)
	columnDF.unpersist()

with open(fileName + ".json", "w") as f:
	json.dump(dataJson,f)

sc.stop()
