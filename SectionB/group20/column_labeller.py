# Manually labelling datasets

import pyspark
import json
import sys
import re
import math
import pandas as pd
from dateutil.parser import parse
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import isnan, when, count, col


sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("hw3") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

def log(msg):
    date_timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(date_timestamp + " INFO: " + str(msg.encode(sys.stdout.encoding, 'ignore').decode()))

def logError(msg):
    date_timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(date_timestamp + " ERROR: " + str(msg.encode(sys.stdout.encoding, 'ignore').decode()))

dataset_names = []
# TODO: Control I/O variations and arguments through shell script
if re.match('.*\.txt$', sys.argv[1]):
    input_file = sys.argv[1]
    log("Reading file: " + input_file)
    with open(input_file, 'r') as f:
        dataset_names = f.read().splitlines();
else:
    dataset_names = sys.argv[1:-1]
    log("Received " + str(len(dataset_names)) + " filepaths as input") 

output_file = sys.argv[-1]
log("Writing output to " + output_file)

output_dict = {}
for dataset_name in dataset_names:
    #dataset_name='/user/hm74/NYCColumns/5694-9szk.Business_Website_or_Other_URL.txt.gz'
    log("Started processing - " + dataset_name)
    input_data = spark.read.format('csv').options(delimiter='\t').load(dataset_name)
    try:
        input_data.distinct().show(50)
    except Exception as e:
        logError("Skipping " + dataset_name)
        continue
    column_label = input("Column Label:")
    #output_json = process_dataset(dataset_name)
    log("Column label - " + column_label)
    log("Processed dataset - " + dataset_name)
    output_dict[dataset_name] = column_label
    with open("columns_labelled.txt", "a+") as f:
        f.write(dataset_name + "\t" + column_label + "\n")


