import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import ast
from dateutil.parser import parse
from statistics import mean, stdev 
import json
import os
import subprocess
from pyspark.sql.functions import col
from datetime import datetime

def get_data_type(value):
    if value == None: return (value, 'TEXT')
    value=value.strip()
    if len(value) == 0: return (value, 'TEXT')
    try:
        datetime_object = datetime.strptime(value, '%Y-%m-%d')
        datetime_object = datetime.strptime(value, '%Y-%m')
        return (datetime_object, 'DATE/TIME')
    except ValueError:
        try:
            t=ast.literal_eval(value)
            if type(t) in [int, float]:
                if type(t) is int:
                    return (t, 'INTEGER(LONG)')
                if type(t) is float:
                    return (t, 'REAL')
            else:
                return (value, 'TEXT')
        except ValueError:
            try: 
                x = parse(value, fuzzy=False)
                return (x, 'DATE/TIME')
            except ValueError:
                return (value, 'TEXT')
        except SyntaxError:
            try: 
                x = parse(value, fuzzy=False)
                return (x, 'DATE/TIME')
            except ValueError:
                return (value, 'TEXT')

def int_report(int_list, data_types_list):
    if len(int_list) > 0:
        data_type_dic = {}
        data_type_dic["type"] = "INTEGER (LONG)"
        data_type_dic["count"] = len(int_list)
        data_type_dic["max_value"] = max(int_list)
        data_type_dic["min_value"] = min(int_list)
        data_type_dic["mean"] = mean(int_list)
        if len(int_list) > 1:
            data_type_dic["stddev"] = stdev(int_list)
        data_types_list.append(data_type_dic)

def real_report(real_list, data_types_list):
    if len(real_list) > 0:
        data_type_dic = {}
        data_type_dic["type"] = "REAL"
        data_type_dic["count"] = len(real_list)
        data_type_dic["max_value"] = max(real_list)
        data_type_dic["min_value"] = min(real_list)
        data_type_dic["mean"] = mean(real_list)
        if len(real_list) > 1:
            data_type_dic["stddev"] = stdev(real_list)
        data_types_list.append(data_type_dic)

def date_report(date_list, data_types_list):
    if len(date_list) > 0:
        data_type_dic = {}
        data_type_dic["type"] = "DATE/TIME"
        data_type_dic["count"] = len(date_list)
        date_list.sort()
        #!!!!!!!!!!!原本的值
        data_type_dic["max_value"] = str(date_list[-1])
        data_type_dic["min_value"] = str(date_list[0])
        data_types_list.append(data_type_dic)

def text_report(text_list, data_types_list):
    if len(text_list) > 0:
        data_type_dic = {}
        data_type_dic["type"] = "TEXT"
        text_list.sort(key = len) 
        data_type_dic["shortest_values"] = text_list[0:5]
        temp_list = text_list[len(text_list) - 5:]
        temp_list.reverse()
        data_type_dic["longest_values"] = temp_list
        data_type_dic["average_length"] = value_length_sum / len(text_list)
        data_types_list.append(data_type_dic)

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()
cmd = 'hdfs dfs -ls /user/hm74/NYCOpenData'
files = subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')
#53
for i in range(305, len(files)):
    path = files[i]
    filePath = path.split(":")[1].split(" ")[1]
    fileName = filePath.split("/")[-1]
    print(str(i) + " " + fileName)
    df = spark.read.option("header", "true").option("delimiter", "\t").csv(filePath)
    res = {}
    res["dataset_name"] = fileName
    res["columns"] = []
    for i in range(0, len(df.columns)):
        cur_column_dic = {}
        original_colName = df.columns[i]
        cur_column_dic["column_name"] = original_colName
        colName = original_colName.replace(" ", "_")
        cur_df = df.withColumnRenamed(original_colName, colName)
        print("**********In column <" + colName + ">:")
        cur_column = cur_df.select(colName)
        totalCell = cur_column.count()
        emptyCell = cur_column.filter(col(colName).isNull()).count()
        nonEmptyCell = totalCell - emptyCell
        cur_column_dic["number_non_empty_cells"] = nonEmptyCell
        cur_column_dic["number_empty_cells"] = emptyCell
        groupedDF = cur_column.groupBy(colName).count().orderBy(desc("count"))
        cur_column_dic["number_distinct_values"] = groupedDF.count()
        frequent_values = [row[0] for row in groupedDF.head(5)]
        cur_column_dic["frequent_values"] = frequent_values
        cur_column_dic["data_types"] = []
        int_list = []
        real_list = []
        text_list = []
        date_list = []
        value_length_sum = 0
        for row in groupedDF.rdd.collect():
            value, data_type = get_data_type(row[0])
            if data_type == 'TEXT' and value is not None:
                text_list.append(value)
                value_length_sum += len(value)
            elif  data_type == 'INTEGER(LONG)':
                int_list.append(value)
            elif data_type == 'REAL':
                real_list.append(value)
            elif  data_type == 'DATE/TIME':
                date_list.append(value)
        int_report(int_list, cur_column_dic["data_types"])
        real_report(real_list, cur_column_dic["data_types"])
        date_report(date_list, cur_column_dic["data_types"])
        text_report(text_list, cur_column_dic["data_types"])
        res["columns"].append(cur_column_dic)
    y = json.dumps(res, indent=2)
    output_file = "/home/jc8773/project/task1/output/output_" + fileName + ".json"
    f = open(output_file, 'w')
    print(y, file=f)
    f.close()


