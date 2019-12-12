from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from itertools import chain
import csv
import pandas as pd
import numpy as numpy
import json
import string

# Init PySpark context
sc = SparkContext()
spark = SparkSession.builder.appName("final_p3").config("spark.some.config.option", "some-value").getOrCreate()
# Load Data
data_url = "/user/hm74/NYCOpenData"

# 311 Service Requests for 2004, 2005, 2006, 2007, 2008, 2009, 2010~Present
datasets = ["sqcr-6mww", "sxmw-f24h", "hy4q-igkk", "aiww-p3af", "uzcy-9puk", "3rfa-3xsf", "erm2-nwe9"]
# Get Datasets (local version)
dataset_prefix = "311_Service_Requests_"
dataset_label = ["for_2004", "for_2005", "for_2006", "for_2007", "for_2008", "for_2009", "from_2010_to_Present"]
dataset_names = [dataset_prefix + i + '.csv' for i in dataset_label]
# print(dataset_names)

# Import Zipcode-Borough maps
# The zip map is relatively small(7kb), so store a seperate copy in each node

zip_borough_dir = "zip_borough.csv" # relatively small: 7kb
zip_borough_dict = {}
with open (zip_borough_dir, newline='') as zip_borough_csv:
    zip_reader = csv.DictReader(zip_borough_csv, ['zip', 'borough'])
    for row in zip_reader:
        if row['borough'] == "Staten":
            zip_borough_dict[row['zip']] = "STATEN ISLAND"
        else:
            zip_borough_dict[row['zip']] = row['borough'].upper()
zip_borough_dict['null'] = 'Unspecified'
# test
# print(json.dumps(zip_borough_dict))

# Import Datasets

# Remote Version
nyc_data = "/user/hm74/NYCOpenData"
cp_dir_list = []
for cp_name in datasets:
    cp_dir = nyc_data + "/" + cp_name + ".tsv.gz"
    cp_dir_list.append(cp_dir)

# Local Version
# Load main datasets
df_full_list = []
df_part_list = []

for dataset in dataset_names:
    df = spark.read.csv(dataset, header=True)
    df_part = df.select("Created Date","Complaint Type", "Incident Zip", "City", "Borough")
    df_full_list.append(df)
    df_part_list.append(df_part)
# test
df_part_list[0].show(50)

def fill_empty_borough():
    """
    Fill up the <borough> column of each case
    ! Operation needed: combine existing data Rdd & zipcode map
    """
    # Try to fill the borough column of each cell
    zip_brgh_map = create_map([lit(x) for x in chain(*zip_borough_dict.items())])    
    for df in df_part_list:
        df = df.withColumn("Borough2", when((col('Incident Zip') != 'null') \
            & (col('Borough') == 'Unspecified'), zip_brgh_map.getItem(col('Incident Zip')))\
                .otherwise(col('Borough')))

def get_freq_cp_type():
    years = [2004, 2005, 2006, 2007, 2008, 2009, 2010]
    df_agg_dic = {}
    for df, year in zip(df_part_list, years):
        df_agg_dic[year] = {
            "bk" : df.filter("Borough='BROOKLYN'").groupBy('Borough','Complaint Type'). \
                count().orderBy('count', ascending=False).take(5),
            "ma" : df.filter("Borough='MANHATTAN'").groupBy('Borough','Complaint Type'). \
                count().orderBy('count', ascending=False).take(5),
            "qu" : df.filter("Borough='QUEENS'").groupBy('Borough','Complaint Type'). \
                count().orderBy('count', ascending=False).take(5),
            "br" : df.filter("Borough='BRONX'").groupBy('Borough','Complaint Type'). \
                count().orderBy('count', ascending=False).take(5),
            "si" : df.filter("Borough='STATEN ISLAND'").groupBy('Borough','Complaint Type'). \
                count().orderBy('count', ascending=False).take(5)
        }




