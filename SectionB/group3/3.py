import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions, types

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()

######################## Utils ########################

datasets = (spark.read.format('csv')
            .options(inferschema='true', sep='\t')
            .load('/user/hm74/NYCOpenData/datasets.tsv')
            .toDF('filename', 'title'))

datasets = datasets.filter(datasets['title'].like)

