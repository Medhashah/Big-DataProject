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
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import isnan, when, count, col

sc = SparkContext()

spark = SparkSession \
		.builder \
		.appName("Big data project") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()


def getData(file):
	return spark.read.option("delimiter", ",").option("header","true").option("inferSchema","true").csv(file)

def castAsDate(x):
	return parser.parse(x)

castAsDate_udf = udf(castAsDate,DateType())

def getMostFreqComp(df):
	'''
	Returns list for 3 most frequent complaints and their count by Borough
	'''
	df.createOrReplaceTempView("311datatemp")
	mostFreq = spark.sql("select Borough, `Complaint Type`, count(*) from 311datatemp group by Borough, `Complaint Type` order by count(*) desc")
	boroughs = mostFreq.select("Borough").distinct().collect()
	compByBorough = []
	for borough in boroughs:
		values = mostFreq.filter(mostFreq["Borough"] == borough[0]).take(5)
		values = [[i[0],i[1],i[2]] for i in values]
		compByBorough.append(values)
	spark.catalog.dropTempView("311datatemp")
	return compByBorough


def writeCompToCsv(compList, fileName):
	with open(fileName, "w") as fp:
		wr = csv.writer(fp)
		for boroughWise in compList:
			for item in boroughWise:
				wr.writerow(item)


def secToDays(x):
	return float(x)/3600/24

secToDays_udf = udf(secToDays,FloatType())


#Overall 
df = getData("311_2017-2019.csv")
df = df.filter(df["Created Date"].isNotNull())
df = df.filter(df["Closed Date"].isNotNull())
#df.createOrReplaceTempView("311data")
#mostFreq = spark.sql("select Borough, `Complaint Type`, count(*) from 311data group by Borough, `Complaint Type` order by count(*) desc")

#boroughs = mostFreq.select("Borough").distinct().collect()
#compByBorough = []
#for borough in boroughs:
	#compByBorough.append(mostFreq.filter(mostFreq["Borough"] == borough[0]).take(3))

#spark.catalog.dropTempView("311data")

print(df.printSchema())

#allComp = getMostFreqComp(df)
#writeCompToCsv(allComp,"complaintsAllYears.csv")

#Yearly analysis
dateDf = df.withColumn("Created Date",castAsDate_udf(df["Created Date"]))
dateDf = dateDf.withColumn("Closed Date",castAsDate_udf(dateDf["Closed Date"]))
dateDf.createOrReplaceTempView("dateDf")


#YYYY-MM-DD
dates = [("2017-01-01",  "2017-12-31"),("2018-01-01",  "2018-12-31"), ("2019-01-01",  "2019-12-31")]

print("Printing year wise....")

for date in dates:
	print("Processing for :{0}".format(date))
	yearDf = dateDf.where(col('Created Date').between(*date))
	yearDf.createOrReplaceTempView("311data")
	writeCompToCsv(getMostFreqComp(yearDf), date[0].split("-")[0] + ".csv")
	
	delayDf = spark.sql("SELECT Borough, `Complaint Type`, abs(datediff(date(`Created Date`),date(`Closed Date`))) as Days from 311data")
	boroughDelay = delayDf.groupBy("Borough").agg({'Days':'avg'})
	boroughDelay.write.csv("borough_avg_delay_"+date[0].split("-")[0]+".csv")

	boroughs = delayDf.select("Borough").distinct().collect()
	#Order in descending
	boroughCompDelay = delayDf.groupBy(["Borough","Complaint Type"]).agg({'Days':'avg'}).orderBy("avg(Days)", ascending=False)
	compByBoroughAvg = []
	for borough in boroughs:
		values = boroughCompDelay.filter(boroughCompDelay["Borough"] == borough[0]).take(5)
		values = [[i[0],i[1],i[2]] for i in values]
		compByBoroughAvg.append(values)
	
	writeCompToCsv(compByBoroughAvg,"borough_comptype_avg_delay_"+date[0].split("-")[0]+".csv")
	#values = borough.orderBy("avg(Days)").filter(borough["Borough"] == "BRONX").take(5)
	spark.catalog.dropTempView("311data")
	gc.collect()




#timeDiff = (unix_timestamp('Closed Date', "yyyy-MM-dd") - unix_timestamp('Created Date', "yyyy-MM-dd"))
#delafDf = dateDf.withColumn("Duration", timeDiff.cast(FloatType()))

#delayDf = spark.sql("SELECT Borough, `Complaint Type`, abs(datediff(date(`Created Date`),date(`Closed Date`))) as Days from dateDf")


#Month wise for 2017
dates = [("2017-01-01",  "2017-01-31"),("2017-02-01",  "2017-02-31"),("2017-03-01",  "2017-03-31"),("2017-04-01",  "2017-04-31"),
("2017-05-01",  "2017-05-31"),("2017-06-01",  "2017-06-31"),("2017-07-01",  "2017-07-31"),("2017-08-01",  "2017-08-31"),
("2017-09-01",  "2017-09-31"),("2017-10-01",  "2017-10-31"),("2017-11-01",  "2017-11-31"),("2017-12-01",  "2017-12-31")]

print("Processing month wise for year 2017....")
for month in dates:
	print("Processing for :{0}".format(month))
	monthDf = dateDf.where(col('Created Date').between(*month))
	#Consider only those entries for which we have location information
	monthDf = monthDf.filter(monthDf["Latitude"].isNotNull())
	monthDf = monthDf.filter(monthDf["Longitude"].isNotNull())
	monthDf.createOrReplaceTempView("311data")
	#writeCompToCsv(getMostFreqComp(yearDf), date[0].split("-")[0] + ".csv")
	
	countDf = spark.sql("SELECT `Complaint Type`, Latitude, Longitude from 311data")
	compTypeCount = countDf.groupBy(["Complaint Type"]).agg({'Complaint Type':'count'}).orderBy("count(Complaint Type)", ascending=False).take(3)
	compByLoc = []
	for compType in compTypeCount:
		values = countDf.filter(countDf["Complaint Type"] == compType[0]).collect()
		values = [[i[0],i[1],i[2]] for i in values]
		compByLoc.append(values)

	writeCompToCsv(compByLoc,"comptype_most_loc_"+month[0].split("-")[1]+".csv")

	spark.catalog.dropTempView("311data")
	gc.collect()
