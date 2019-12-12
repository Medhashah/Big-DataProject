import pandas
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from itertools import chain, combinations

def key_options(items):
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items)+1) )

directory = "/user/hm74/NYCOpenData"
sc = SparkContext()
fileNames = sc.textFile(directory+"/datasets.tsv").map(lambda x: x.split('\t')[0]).collect()
spark = SparkSession \
.builder \
.appName("Python Spark SQL Project") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

with open("task1extracredit.txt", "w") as text_file:
    for name in fileNames:
        filePath = directory + "/" + fileNames[0] +".tsv.gz"
        fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath)
        fileDF = fileDF.toPandas()
        text_file.write('File Name: '+ name )
        for candidate in key_options(list(fileDF)):
            deduped = fileDF.drop_duplicates(candidate)
            if len(deduped.index) == len(fileDF.index):
                print(','.join(candidate))
                text_file.write(','.join(candidate))
