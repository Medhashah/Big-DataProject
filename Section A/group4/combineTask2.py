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
from glob import glob

"""
types4 = ["INTEGER(LONG)", "REAL", "DATE/TIME", "TEXT"]

frequencyLists = [{"key":{"INTEGER(LONG)"},"count" : 0},{"key":{"REAL"},"count" : 0}, {"key":{"DATE/TIME"},"count" : 0}, {"key":{"TEXT"},"count" : 0}, {"key":{"INTEGER(LONG)", "REAL"},"count" : 0},{"key":{"REAL", "DATE/TIME"},"count" : 0}, {"key":{"DATE/TIME", "TEXT"},"count" : 0}, {"key":{"DATE/TIME", "INTEGER(LONG)"},"count" : 0}, {"key":{"TEXT", "REAL"},"count" : 0}, {"key":{"INTEGER(LONG)", "TEXT"},"count" : 0}, {"key":{"INTEGER(LONG)", "REAL", "DATE/TIME"},"count" : 0}, {"key":{"REAL", "DATE/TIME", "TEXT"},"count" : 0}, {"key":{"INTEGER(LONG)", "DATE/TIME", "TEXT"},"count" : 0}, {"key":{"INTEGER(LONG)", "DATE/TIME", "TEXT"},"count" : 0}, {"key":{"INTEGER(LONG)", "REAL", "TEXT"}, "count":0}, {"key": {"INTEGER(LONG)", "REAL", "DATE/TIME", "TEXT"}, "count":0}]

histogram = {"INTEGER(LONG)" : 0, "REAL" : 0, "DATE/TIME" : 0, "TEXT" : 0}
"""

def cat_json(output_filename, input_filenames):
	jsons = []
	with open(output_filename, "w") as outfile:
		for infile_name in input_filenames:
			print(infile_name)
			newDic = {}
			with open(infile_name, 'r') as f:
				newDic = json.load(f)
				f.close()
			jsons.append(newDic)
		json.dump(jsons, outfile) #jia []
		outfile.close()

cat_json("Task2.json", glob(r'*.json'))
#print(histogram)
#print(*frequencyLists)


