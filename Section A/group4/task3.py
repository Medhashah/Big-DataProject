#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, BooleanType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import unix_timestamp

from datetime import datetime, date
from dateutil.parser import parse
import json

sc = SparkContext()

#filePath = sm48-t3s6.tsv.gz

spark = SparkSession.builder.appName("project").config("spark.some.config.option", "some-value").getOrCreate()
sqlContext = SQLContext(spark)

filePath = "/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz"
fileDF = spark.read.option("sep", "\t").options(header = 'true').csv(filePath)

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.
    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
    	parse(string, fuzzy=fuzzy)
    	return parse(string, fuzzy=fuzzy)
    except:
    	return parse(str(date.today()))

isDate = F.udf(lambda string : is_date(string), TimestampType())

fileDF.createOrReplaceTempView("fileDF")
fd = fileDF.where(F.col("Closed Date").isNotNull()).where(F.col("Created Date").isNotNull()).where(F.col("Incident Zip").isNotNull())

fd = fd.withColumn("end", isDate("Closed Date"))
fd = fd.withColumn("start", isDate("Created Date"))

#calculate the Duration
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
timeDiff = (F.unix_timestamp('end', format=timeFmt)
            - F.unix_timestamp('start', format=timeFmt))
fd = fd.withColumn("Duration", timeDiff)
fd.createOrReplaceTempView("fd")

#Select Descriptor, Location Type and Complaint Type with "Sidewalk" or "Curb"
tmp = fd.where(~fd["Descriptor"].like('%Sidewalk%')).where(~fd["Descriptor"].like('%Curb%')).where(~fd["Location Type"].like('%Sidewalk%'))\
	.where(~fd["Location Type"].like('%Curb%')).where(~fd["Complaint Type"].like('%Sidewalk%')).where(~fd["Complaint Type"].like('%Curb%'))
fd = fd.subtract(tmp)

#Groupby "Incident Zip" to compute the average response time.
fd.groupby("Incident Zip").agg(F.avg(F.col("Duration"))).sort("avg(Duration)").show(5000)



