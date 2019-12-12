import pyspark
import json
import sys
import re
import math
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

def is_date(string):
    if not re.match('^[0-9]{1,2},[0-9]{1,2},(1[6-9])?[0-9]{2}$\|^(1[6-9])?[0-9]{2},[0-9]{1,2},[0-9]{1,2}$', string):
        return False
    try:
        parse(string, fuzzy=False)
        return True
    except:
        return False

# TODO check commas in int and float
def return_data_types(val):
    if re.match('[-+]?[0-9]+$', val):
        return ("INTEGER (LONG)", int(val))
    elif re.match('[-+]?[0-9]*?\.[0-9]+$', val):
        return ("REAL", float(val))
    elif is_date(val):
        return  ("DATE/TIME", val)
    elif val:
        return ("TEXT", val)

def get_col_name(col_name):
    if "." in col_name:
        return str("`" + col_name + "`")
    return col_name

def process_dataset(filename):
    log("Started processing - " + filename)
    input_data = spark.read.format('csv').options(header='true', delimiter='\t').load(filename)
    
    column_list = input_data.columns
    json_file_data = []
    json_column_data = []
    
    count_null_vals = input_data.select([count(when(col(get_col_name(column_name)).isNull(), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    count_non_null_vals = input_data.select([count(when(col(get_col_name(column_name)).isNotNull(), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    
    for column_name in column_list:
        column_data = {}
        fin_dict = {}
        fin_dict["data_types"]=[]
        int_flag = real_flag = date_flag = False
        dts_df = None
        df_list = []
        col_df = input_data.select(get_col_name(column_name))
        col_rdd = col_df.rdd.map(lambda x: x[(column_name)]).filter(lambda x: x!=None)
        dt_rdd = col_rdd.map(lambda x: return_data_types(x))
        int_rdd = dt_rdd.filter(lambda x: x[0]=="INTEGER (LONG)").map(lambda x: Row(value=x[1]))
        if not int_rdd.isEmpty():
            int_df = int_rdd.toDF().select(F.lit('INTEGER (LONG)').alias('type'), F.count('value').alias('count'), F.max('value').alias('max_value'), F.min('value').alias('min_value'), F.mean('value').alias('mean'), F.stddev('value').alias('stddev'))
            int_flag = True
        real_rdd = dt_rdd.filter(lambda x: x[0]=="REAL").map(lambda x: Row(type='REAL', value=x[1]))
        if not real_rdd.isEmpty():
            real_df = real_rdd.toDF().select(F.lit('REAL').alias('type'), F.count('value').alias('count'), F.max('value').alias('max_value'), F.min('value').alias('min_value'), F.mean('value').alias('mean'), F.stddev('value').alias('stddev'))
            real_flag = True
        date_rdd = dt_rdd.filter(lambda x: x[0]=="DATE/TIME").map(lambda x: Row(value=x[1], fvalue=parse(x[1])))
        if not date_rdd.isEmpty():
            date_df = date_rdd.toDF().orderBy('fvalue')
            date_df = date_df.select(F.lit('DATE/TIME').alias('type'), F.count('value').alias('count'), F.last('value').alias('max_value'), F.first('value').alias('min_value'), F.lit(None).alias('mean'), F.lit(None).alias('stddev'))
            date_flag = True
        if int_flag:
            dts_df = int_df
        if real_flag:
            if dts_df:
                dts_df = dts_df.union(real_df)
            else:
                dts_df = real_df
        if date_flag:
            if dts_df:
                dts_df = dts_df.union(date_df)
            else:
                dts_df = date_df
        if dts_df:
            df_list = dts_df.collect()
        
        text_rdd = dt_rdd.filter(lambda x: x[0]=="TEXT").map(lambda x: (x[1],len(x[1])))
        text_count = 0
        if not text_rdd.isEmpty():
            longest_length = text_rdd.distinct().sortBy(lambda x: x[1]).top(5, key=lambda x: x[1])
            shortest_length = text_rdd.distinct().sortBy(lambda x: x[1]).take(5)
            text_count = text_rdd.count()
            average_length = text_rdd.map(lambda x: x[1]).mean()
        for j in df_list:
            dt_dict = {}
            dt_dict["type"] = j['type']
            dt_dict["count"] = j['count']
            if j['type'] == "DATE/TIME":
                dt_dict["max_value"] = str(j['max_value'])
                dt_dict["min_value"] = str(j['min_value'])
            if j['type'] ==  "INTEGER (LONG)":
                dt_dict["max_value"] = int(j['max_value'])
                dt_dict["min_value"] = int(j['min_value'])
            if j['type'] == "REAL":
                dt_dict["max_value"] = float(j['max_value'])
                dt_dict["min_value"] = float(j['min_value'])
            if j['type'] ==  "INTEGER (LONG)" or  j['type'] == "REAL":
                dt_dict["mean"] = float(j['mean'])
                dt_dict["stddev"] = float(j['stddev'] if not math.isnan(j['stddev']) else 0)
            fin_dict["data_types"].append(dt_dict)
        if text_count:
            dt_dict = {}
            dt_dict["type"] = "TEXT"
            dt_dict["count"] = int(text_count)
            dt_dict["shortest_values"] = [k[0] for k in shortest_length]
            dt_dict["longest_values"] = [k[0] for k in longest_length]
            dt_dict["average_length"] = float(average_length)
            fin_dict["data_types"].append(dt_dict)
        freq_val_tuples = col_rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False).take(5)
        column_data["column_name"] = column_name
        column_data["number_non_empty_cells"] = int(count_non_null_vals.collect()[0][column_name])
        column_data["number_empty_cells"] = int(count_null_vals.collect()[0][column_name])
        column_data["number_distinct_values"] = col_rdd.count()
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
with open('dataset_names_new.txt', 'r') as f:
    dataset_names = f.read().split(", ")

# dataset_names = [sys.argv[1]]
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
        with open('task1_new.json', 'w') as out_file:
            json.dump(final_merged_json, out_file)

with open('task1_new.json', 'w') as out_file:
    json.dump(final_merged_json, out_file)

