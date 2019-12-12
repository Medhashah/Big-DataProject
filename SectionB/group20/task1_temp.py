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

def check_float(val):
    try:
        float(val)
        return True
    except:
        return False

def is_date(string):
    if re.match('^[0-9]*,([0-9]{3},)?[0-9]{3}$|^[0-9]+,[0-9]+$|^[0-9]+,([0-9]{1,3},)*[0-9]{1,3}$', string.strip()):
        return False
    try:
        parse(string, fuzzy=False, ignoretz=True)
        return True
    except:
        return False

def return_data_types(val):
    if re.match('[-+]?[0-9]+$', val):
        return "INTEGER (LONG)", int(val)
    elif re.match('[-+]?[0-9]*?\.[0-9]+$', val):
        return "REAL", float(val)
    elif is_date(val):
        return  "DATE/TIME", val
    elif val:
        return "TEXT", val

def get_col_name(col_name):
    if "." in col_name:
        return str("`" + col_name + "`")
    return col_name

def process_dataset(filename):
    # filename = "/user/hm74/NYCOpenData/2232-dj5q.tsv.gz" # TODO: Receive as argument from driver script
    log("Started processing - " + filename)
    # filename = "/user/hm74/NYCOpenData/" + filename
    input_data = spark.read.format('csv').options(header='true', delimiter='\t').load(filename)
    
    column_list = input_data.columns
    json_file_data = []
    json_column_data = []
    
    #count_nan_vals = input_data.select([count(when(isnan(get_col_name(column_name)), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    count_null_vals = input_data.select([count(when(col(get_col_name(column_name)).isNull(), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    
    #count_non_nan_vals = input_data.select([count(when(~isnan(get_col_name(column_name)), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    count_non_null_vals = input_data.select([count(when(col(get_col_name(column_name)).isNotNull(), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    
    #input_data_pd = input_data.toPandas()
    #count_distinct_vals = input_data_pd.nunique()
    
    #top_5_frequent_vals = {}
    #for column_name in input_data_pd.columns:
    #    frequency_vals = input_data_pd[column_name].value_counts()
    #    top_5_frequent_vals[column_name] = frequency_vals[:5].index.tolist()
    
    for column_name in column_list:
        column_data = {}
        fin_dict = {}
        col_df = input_data.select(get_col_name(column_name)).toPandas()[column_name].dropna()
        col_rdd = sc.parallelize(col_df).filter(lambda x: x!=None)
        freq_val_tuples = col_rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False).take(5)
        col_dict = {}
        for i in col_df:
            if not i:
                continue
            dt, val = return_data_types(i)
            if dt in col_dict.keys():
                col_dict[dt].append(val)
            else:
                col_dict[dt] = [val]
        fin_dict["column_name"] = column_name
        fin_dict["data_types"] = []
        for j in col_dict.keys():
            dt_dict = {}
            rdd = sc.parallelize(col_dict[j])
            dt_dict["type"] = j
            dt_dict["count"] = int(rdd.count())
            if j ==  "INTEGER (LONG)":
                dt_dict["max_value"] = int(rdd.max())
                dt_dict["min_value"] = int(rdd.min())
            if j == "REAL":
                dt_dict["max_value"] = float(rdd.max()) # Any specific reason why max is int?
                dt_dict["min_value"] = float(rdd.min())
            if j ==  "INTEGER (LONG)" or  j == "REAL":
                dt_dict["mean"] = float(rdd.mean())
                dt_dict["stddev"] = float(rdd.stdev())
            if j == "TEXT":
                rdd = rdd.map(lambda x: (x, len(x))).distinct().sortBy(lambda x: x[1])
                longest_length = rdd.top(5, key=lambda x: x[1])
                shortest_length = rdd.take(5)
                dt_dict["shortest_values"] = [k[0] for k in shortest_length]
                dt_dict["longest_values"] = [k[0] for k in longest_length]
                dt_dict["average_length"] = float(rdd.map(lambda x: x[1]).mean())
            if j == "DATE/TIME":
                rdd = rdd.map(lambda x: (x, parse(x, fuzzy=False, ignoretz=True))).distinct().sortBy(lambda x: x[1])
                max_val = rdd.top(1, key=lambda x: x[1])
                min_val = rdd.take(1)
                dt_dict["max_value"] = max_val[0][0]
                dt_dict["min_value"] = min_val[0][0]
            fin_dict["data_types"].append(dt_dict)
        column_data["column_name"] = column_name
        column_data["number_non_empty_cells"] = int(count_non_null_vals.collect()[0][column_name])
        column_data["number_empty_cells"] = int(count_null_vals.collect()[0][column_name])
        column_data["number_distinct_values"] = len(col_df.unique())
        column_data["frequent_values"] = [v[0] for v in freq_val_tuples]
        column_data["data_types"] = fin_dict["data_types"]
        json_column_data.append(column_data)
    
    output_data = {}
    output_data["dataset_name"] = filename
    output_data["columns"] = column_list
    output_data["key_column_candidates"] = []
    
    json_file_data.append(output_data)
    json_file_data.append(json_column_data)
    
    json_data = json.dumps(json_file_data)
    
    # with open('test.json', 'w') as f:
    #     json.dump(json_file_data, f)
    
    return json_file_data


final_merged_json = []
count_processed_files = 0
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

for dataset_name in dataset_names:
    output_json = {}
    try:
        output_json = process_dataset(dataset_name)
    except Exception as e:
        try:
            logError("Exception occured while processing - " + dataset_name + "\n" + str(e))
        except Exception as e2:
            logError("Exception occured while processing - " + dataset_name)
        continue
    #output_json = process_dataset(dataset_name)
    final_merged_json.append(output_json)
    log("Processed dataset - " + dataset_name)
    count_processed_files += 1
    if (count_processed_files == 10):
        count_processed_files = 0
        log("Writing json to file")
        with open(output_file, 'w') as out_file:
            json.dump(final_merged_json, out_file)

with open(output_file, 'w') as out_file:
    json.dump(final_merged_json, out_file)

