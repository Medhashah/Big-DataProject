import os
import gc
import csv
import json
import pyspark
from pyspark import SparkContext

import datetime
from dateutil import parser

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from pyspark.sql.functions import isnan, when, count, col

sc = SparkContext()

spark = SparkSession \
		.builder \
		.appName("Big data project") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

def getFileList():
	op = os.popen("hdfs dfs -ls /user/hm74/NYCOpenData").read().split('\n')
	files = []
	for file in op:
		if '.gz' in file:
			files.append(file.split()[-1])
	return files

def getData(file):
	return spark.read.option("delimiter", "\\t").option("header","true").option("inferSchema","true").csv(file)

def castAsType(x):
	try:
		if float(x).is_integer() == True:
			x = int(x)
			x = 'INTEGER'
			return x
	except:
		pass
	try:
		x = float(x)
		x = "REAL"
		return x
	except:
		try:
			x = parser.parse(x)
			x = "DATE/TIME"
			return x
		except:
			x = str(x)
			x = 'TEXT'
			return x

def getIntStats(rdd):
	subset = rdd.filter(lambda x:x[1] == "INTEGER").map(lambda x: int(x[0]))
	if subset.isEmpty():
		return 0, 0, 0, 0, 0
	else:
		count = subset.countApprox(timeout=10000)
		return count, subset.max(), subset.min(), subset.mean(), subset.stdev()

def getRealStats(rdd):
	subset = rdd.filter(lambda x:x[1] == "REAL").map(lambda x:float(x[0]))
	if subset.isEmpty():	return 0, 0, 0, 0, 0
	count = subset.countApprox(timeout=10000)
	return count, subset.max(), subset.min(), subset.mean(), subset.stdev()

def getDateStats(rdd):
	subset = rdd.filter(lambda x: x[1]=="DATE/TIME").map(lambda x: parser.parse(x[0]))
	if subset.isEmpty(): return 0, '', ''
	count = subset.countApprox(timeout=10000)
	return count, subset.max(), subset.min()

def getTextStats(rdd):
	subset = rdd.filter(lambda x: x[1]=="TEXT").map(lambda x: (x[0],len(x[0])))
	if subset.isEmpty():	return 0, [], [], 0 
	count = subset.countApprox(timeout=10000)
	longest = subset.takeOrdered(5, key = lambda x: -x[1])
	shortest = subset.takeOrdered(5, key= lambda x: x[1])
	avgLen = subset.map(lambda x: x[1]).mean()
	return count, longest, shortest, avgLen

def datetime_handler(x):
	if isinstance(x, datetime.datetime):
		return x.isoformat()
	raise TypeError("Unknown type")

files = getFileList()

freqColItems = []
freqId = 0
#Processing should be done inside for loop for each dataset
for file in files:
	
	data = {}
	data["dataset_name"] = file
	df = getData(file).cache()
	rdd = df.rdd.cache()
	# 1 & 2
	emptyDf = df.select([count(when(isnan(c) | col(c).contains('NA') | col(c).contains('NULL') | col(c).isNull(),c)).alias(c) for c in df.columns])
	
	emptyCount = emptyDf.rdd.map(lambda row : row.asDict()).collect()[0]
	rows = rdd.countApprox(timeout=10000)
	nonEmptyCount = {}
	for c in emptyDf.columns:
		nonEmptyCount[c] = rows - emptyCount[c]
	
	#3 & 4 & 5
	mostFrequent = {}
	distinct = {}
	dataTypes = dict(df.dtypes)
	colStats = {}
	colListData = []
	
	for column in df.columns:
		#print(col)
		colData = {}
		colData["column_name"] = column
		colData["number_non_empty_cells"] = nonEmptyCount[column]
		colData["number_empty_cells"] = emptyCount[column]
		#grouped = df.groupBy(column).count()
		grouped = df.groupBy(column).agg(F.count(column).alias("ColCount"))
		distinctCount = grouped.count()
		colData["number_distinct_values"] = distinctCount
		tempFreq = grouped.sort(F.desc('ColCount')).select(column).take(5)
		freqList = []
		for item in tempFreq:
			freqList.append(item[0])
		colData["frequent_values"] = freqList
		mostFrequent[column] = freqList
		distinct[column] = distinctCount
		if dataTypes[column] == "string":
			colRdd = df.select(column).dropna().rdd.map(lambda x: x[0])
			colRdd = colRdd.map(lambda x: (x,castAsType(x)))
			freqColItems.append((freqId,colRdd.map(lambda x:x[1]).distinct().collect()))
			freqId += 1
			intStats = getIntStats(colRdd)
			realStats = getRealStats(colRdd)
			dateStats = getDateStats(colRdd)
			textStats = getTextStats(colRdd)
			colStats[column] = [intStats,realStats,dateStats,textStats]
		elif dataTypes[column] in ["int","long","bigint"]:
			colRdd = df.select(column).dropna().rdd.map(lambda x: x[0])
			intStats = (nonEmptyCount[column], colRdd.max(), colRdd.min(), colRdd.mean(), colRdd.stdev())
			realStats = (0,0,0,0,0)
			dateStats = (0,0,0)
			textStats = (0,0,0,0)
			colStats[column] = [intStats,realStats,dateStats,textStats]
		elif dataTypes[column] in ["double","float"]:
			colRdd = df.select(column).dropna().rdd.map(lambda x: x[0])
			intStats = (0,0,0,0,0)
			realStats = (nonEmptyCount[column], colRdd.max(), colRdd.min(), colRdd.mean(), colRdd.stdev())
			dateStats = (0,0,0)
			textStats = (0,0,0,0)
			colStats[column] = [intStats,realStats,dateStats,textStats]
		colRdd.unpersist()
		
		dataTypesValue = {}
		
		for key in colStats.keys():
			keyData = colStats[key]
			#0 Int,1 real, 2 date, 3 text
			tempArray = []
			if keyData[0][0] != 0:
				tempData = {}
				tempData["type"] = "INTEGER (LONG)"
				tempData["count"] = keyData[0][0]
				tempData["max_value"] = keyData[0][1]
				tempData["min_value"] = keyData[0][2]
				tempData["mean"] = keyData[0][3]
				tempData["stddev"] = keyData[0][4]
				tempArray.append(tempData)
			if keyData[1][0] != 0:
				tempData = {}
				tempData["type"] = "REAL"
				tempData["count"] = keyData[1][0]
				tempData["max_value"] = keyData[1][1]
				tempData["min_value"] = keyData[1][2]
				tempData["mean"] = keyData[1][3]
				tempData["stddev"] = keyData[1][4]
				tempArray.append(tempData)
			if keyData[2][0] != 0:
				tempData = {}
				tempData["type"] = "DATE/TIME"
				tempData["count"] = keyData[2][0]
				tempData["max_value"] = keyData[2][1]
				tempData["min_value"] = keyData[2][2]
				tempArray.append(tempData)
			if keyData[3][0] != 0:
				tempData = {}
				tempData["type"] = "TEXT"
				tempData["count"] = keyData[3][0]
				tempData["shortest_values"] = keyData[3][2]
				tempData["longest_values"] = keyData[3][1]
				tempData["average_length"] = keyData[3][3]
				tempArray.append(tempData)
			dataTypesValue[key] = tempArray
		colData["dataTypes"] = dataTypesValue[column]
		colListData.append(colData)
	
	data["columns"] = colListData

	with open(file.split("/")[-1] +'.json', 'w') as fp:
		json.dump(data, fp,default=datetime_handler)

	df.unpersist()
	rdd.unpersist()
	gc.collect()

freqDf = spark.createDataFrame(freqColItems,["id","dtypes"])
fpGrowth = FPGrowth(itemsCol="dtypes", minSupport=0.5, minConfidence=0.6)
model = fpGrowth.fit(df)
freqSet = model.freqItemsets.collect()

with open('freqDataTypes.csv','w') as f:
	wr = csv.writer(f)
	for item in freqSet:
		if len(item[0]) > 1:
			wr.writerow(item[0])
