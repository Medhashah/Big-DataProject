import json
import sys
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
from dateutil.parser import parse

filename = "/user/hm74/NYCOpenData/2232-dj5q.tsv.gz" # TODO: Receive as argument from driver script

input_data = spark.read.format('csv').options(header='true', delimiter='\t').load(filename)

def check_float(val):
    try:
        float(val)
        return True
    except:
        return False

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False

def return_data_types(val):
    if str(val).isdigit():
        return "INTEGER (LONG)", int(val)
    elif check_float(val):
        return "REAL", float(val)
    elif is_date(val):
        return  "DATE/TIME", val
    elif val:
        return "TEXT", val

json_file_data = []
json_column_data = []

count_nan_vals = input_data.select([count(when(isnan(column_name), column_name)).alias(column_name) for column_name in input_data.columns])       
count_null_vals = input_data.select([count(when(col(column_name).isNull(), column_name)).alias(column_name) for column_name in input_data.columns])

count_non_nan_vals = input_data.select([count(when(~isnan(column_name), column_name)).alias(column_name) for column_name in input_data.columns])
count_non_null_vals = input_data.select([count(when(col(column_name).isNotNull(), column_name)).alias(column_name) for column_name in input_data.columns])

input_data_pd = input_data.toPandas()
count_distinct_vals = input_data_pd.nunique()

top_5_frequent_vals = {}
for column_name in input_data_pd.columns:
    frequency_vals = input_data_pd[column_name].value_counts()
    top_5_frequent_vals[column_name] = frequency_vals[:5].index.tolist()

for column_name in input_data.columns:
    column_data = {}
    fin_dict = {}
    d = input_data.toPandas()[column_name]
    col_dict = {}
    for i in d:
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
        ser = pd.Series(col_dict[j])
        dt_dict["type"] = j
        dt_dict["count"] = int(ser.count())
        if j == "TEXT":
            text_RDD = sc.parallelize(ser.tolist())
            text_RDD = text_RDD.map(lambda x: (x.lower(), len(x)))
            longest_length = text_RDD.distinct().sortBy(lambda x: x[1]).top(5, key=lambda x: x[1])
            shortest_length = text_RDD.distinct().sortBy(lambda x: x[1]).take(5)
            dt_dict["shortest_values"] = [k[0] for k in shortest_length]
            dt_dict["longest_values"] = [k[0] for k in longest_length]
        if j == "DATE/TIME":
            dt_dict["max_value"] = str(ser.max())
            dt_dict["min_value"] = str(ser.min())
        if j ==  "INTEGER (LONG)":
            dt_dict["max_value"] = int(ser.max())
            dt_dict["min_value"] = int(ser.min())
        if j == "REAL":
            dt_dict["max_value"] = int(ser.max()) # Any specific reason why max is int?
            dt_dict["min_value"] = float(ser.min())
        if j ==  "INTEGER (LONG)" or  j == "REAL":
            dt_dict["mean"] = float(ser.mean())
            dt_dict["stddev"] = float(ser.std())
        fin_dict["data_types"].append(dt_dict)
    column_data["column_name"] = column_name
    column_data["number_non_empty_cells"] = int(count_non_null_vals.collect()[0][column_name])
    column_data["number_empty_cells"] = int(count_null_vals.collect()[0][column_name])
    column_data["number_distinct_values"] = int(count_distinct_vals[column_name])
    column_data["frequent_values"] = top_5_frequent_vals[column_name]
    column_data["data_types"] = fin_dict["data_types"]
    json_column_data.append(column_data)

output_data = {}
output_data["dataset_name"] = filename
output_data["columns"] = input_data.columns
output_data["key_column_candidates"] = []

json_file_data.append(output_data)
json_file_data.append(json_column_data)

json_data = json.dumps(json_file_data)

with open('test.json', 'w') as f:
    json.dump(json_file_data, f)

print(json_data)
