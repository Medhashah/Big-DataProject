import os
import pyspark
from pyspark import SparkContext

from dateutil import parser

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from pyspark.sql.functions import isnan, when, count, col


with open("cluster1.txt","r") as f:
	content = f.readlines()

files = content[0].strip("[]").replace("'","").replace(" ","").split(",")

'''
Prathamesh 0-89
Chandan 90-180
John 181-272
'''
fileData = files[0].split(".")
fileName = fileData[0]
colName = fileData[1].replace("_", " ")
df = spark.read.option("delimiter", "\\t").option("header","true").option("inferSchema","true").csv("/user/hm74/NYCOpenData/" + fileName + ".tsv.gz")
List = df.select(colName).distinct().collect()
print(fileName)
print(colName)
for item in List:
	print(item[0])
