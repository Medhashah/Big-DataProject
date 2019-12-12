#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
import os, glob
from dateutil.parser import parse
import statistics, math, datetime, json, re
def listdir_fullpath(d):
    return [os.path.join(d, f) for f in os.listdir(d)]
#guess type of a string using regular expression
def guess_type(s):
    if re.match("\A[0-9]+\.[0-9]+\Z", s):
        return float
    elif re.match("\A[0-9]+\Z", s):
        return int
    elif re.match("^(?:\d{1,2}(?:(?:-|/)|(?:th|st|nd|rd)?\s))?(?:(?:(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)(?:(?:-|/)|(?:,|\.)?\s)?)?(?:\d{1,2}(?:(?:-|/)|(?:th|st|nd|rd)?\s))?)(?:\d{2,4})$", s): 
        return datetime.date
    else:
        return str
if __name__ == "__main__":

    def parse_data(line, pos):
        fields = line.split("\t")
        # use 0 for column 1, 2 for column 2 and so on
        return fields[pos]
    sc = SparkContext()
    spark = SparkSession \
        .builder \
        .appName("project-part1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    os.chdir("/home/cpt289/testfiles/")
    #get dataset names
    ds = spark.read.option("sep", "\t").option("header", "false").option("encoding", "utf-8").csv("/user/cpt289/testfiles/xad")
    datasets = {}
    for row in ds.rdd.collect():
        #print(row[0], row[1])
        datasets[row[0]] = row[1]
    for f in datasets:
        js={}
        js["dataset_name"] = datasets[f]
        print("Processing file: ", f)
        js["columns"] = []
        js["key_column_candidates"] = []
        #read the file and store it either in memory or physical disk (when it's to big)
        df = spark.read.option("sep", "\t").option("header", "true").option("encoding", "utf-8").csv("/user/hm74/NYCOpenData/" + f + ".tsv.gz").persist() 
        len_df = df.count()  #number of rows
        col_names = df.columns   #original column names
        cols = [str(i) for i in range(len(col_names))]  #change column names so that they can be accessed later even when names are duplicated
        df = df.toDF(*cols)
        pos = 0
        #for each column, create a new DataFrame and look for information to fill in the metadata
        for c in df.columns:
            df_sub = df.select(c).cache() #cache the column in memory so it can be processed quickly
            col_dict = {}
            col_dict["column_name"] = col_names[pos]
            pos+=1
            col_dict["number_empty_cells"] = df.filter(df[c].isNull()).count()  #filter non-empty cells
            col_dict["number_non_empty_cells"] = len_df - col_dict["number_empty_cells"]
            col_dict["number_distinct_values"]= df_sub.distinct().count() #filter distinct values
            #sort the column and find frequent values
            current_col = [row[c] for row in df_sub.groupBy(c).count().orderBy("count", ascending=False).collect()]
            col_dict["frequent_values"] = current_col[0:5]
            col_dict["data_types"] =[]
            #as we are about to iterate through each value, categorize it into appropriate data type, and record the max/min values
            int_set = []
            max_int = 0
            min_int = 999999
            str_set = []
            max_str = 0
            min_str = 999999
            real_set = []
            max_real = 0.0
            min_real = 999999.9
            total_length = 0 #total length of all strings
            date_set = []
            #guess type of each distinct value of a column
            for i in current_col:
                if type(i) == int:
                     int_set.append(i)
                     if int(i) > max_int:
                         max_int = i
                     if int(i) < min_int:
                         min_int = i
                elif type(i) == float:
                     real_set.append(i)
                     if float(i) > max_real:
                         max_real = i
                     if float(i) < min_real:
                         min_real = i
                #if type is string, infer the type. If the infered type is str, check whether or not it can be datetime
                elif type(i) == str:
                    value = guess_type(i)
                    if value == int:
                        int_set.append(int(i))
                        if int(i) > max_int:
                            max_int = int(i)
                        if int(i) < min_int:
                            min_int = int(i)
                    elif value == float:
                        real_set.append(float(i))
                        if float(i) > max_real:
                            max_real = float(i)
                        if float(i) < min_real:
                            min_real = float(i)
                    elif value == datetime.date:
                         date_set.append(i)    
                    else:
                        str_set.append(i)
                        total_length += len(i)
                        if len(i) > max_str:
                            max_str = len(i)
                        if len(i) < min_str:
                            min_str = len(i)
            #if a column contains one or more field of certain type, record that type in the Json file
            if (len(int_set) > 0):
                int_dict = {}
                int_dict["type"] = "INTEGER (LONG)"
                int_dict["count"] = len(int_set)
                int_dict["max_value"] = max_int
                int_dict["min_value"] = min_int
                int_dict["mean_value"] = round(statistics.mean(int_set), 2)
                #if there is only one data point, the standard deviation should be 0
                if (len(int_set) == 1):
                    int_dict["standard_deviation"] = 0
                else:
                    int_dict["standard_deviation"] = round(statistics.stdev(int_set), 2)
                col_dict["data_types"].append(int_dict)
            if (len(real_set) > 0):
                real_dict = {}
                real_dict["type"] = "REAL"
                real_dict["count"] = len(int_set)
                real_dict["max_value"] = max_real
                real_dict["min_value"] = min_real
                real_dict["mean_value"] = round(statistics.mean(real_set), 2)
                #if there is only one data point, the standard deviation should be 0
                if (len(real_set) == 1):
                    real_dict["standard_deviation"] = 0
                else:
                    real_dict["standard_deviation"] = round(statistics.stdev(real_set), 2)
                col_dict["data_types"].append(real_dict)
            if (len(date_set) > 0):
                date_dict = {}
                date_dict["type"] = "DATE/TIME"
                date_dict["count"] = len(date_set)
                date_dict["max_value"] = max(date_set)
                date_dict["min_value"] = min(date_set)
                col_dict["data_types"].append(date_dict)
            if (len(str_set) > 0):
                #sort the strings by length
                str_set = sorted(str_set, key=len)
                str_dict = {}
                str_dict["type"] = "TEXT"
                str_dict["count"] = len(str_set)
                #pick the logest and shortest strings in the sorted set
                str_dict["shortest_values"] = str_set[0:5]
                str_dict["longest_values"] = str_set[::-1][0:5]
                str_dict["average_length"] = round((total_length/len(str_set)), 2)
                col_dict["data_types"].append(str_dict)
            js["columns"].append(col_dict)
            df_sub.unpersist()  #release the memory occupation of DataFrame
        df.unpersist() 
        with open('/home/cpt289/ProjectOutput/' + f + '.json', 'w+') as outfile:
            json.dump(js, outfile, indent=1)
    sc.stop()

