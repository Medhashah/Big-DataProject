import sys
import string
import os
import csv
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
import re
import json
import pytz

import sys
from pyspark import SparkContext
from datetime import datetime
import numpy as np
from dateutil.parser import *

class Dataset:
    def __init__(self, dataset_name, columns, key_column_candidates):
        self.dataset_name = dataset_name
        self.columns = columns
        self.key_column_candidates = key_column_candidates

class Column:
    def __init__(self, name, non_empty, empty, distinct, frequent, data_types):
        self.column_name = name
        self.number_non_empty_cells = non_empty
        self.number_empty_cells = empty
        self.number_distinct_values = distinct
        self.frequent_values = frequent
        self.data_types = data_types

class INTEGER:
    def __init__(self, count, max_value, min_value, mean, stddev):
        self.type = "INTEGER"
        self.count = count
        self.max_value = max_value
        self.min_value = min_value
        self.mean = mean
        self.stddev = stddev

class REAL:
    def __init__(self, count, max_value, min_value, mean, stddev):
        self.type = "REAL"
        self.count = count
        self.max_value = max_value
        self.min_value = min_value
        self.mean = mean
        self.stddev = stddev

class DATETIME:
    def __init__(self, count, max_value, min_value):
        self.type = "DATE/TIME"
        self.count = count
        self.max_value = max_value
        self.min_value = min_value

class TEXT:
    def __init__(self, count, shortest, longest, average):
        self.type = "TEXT"
        self.count = count
        self.shortest_values = shortest
        self.longest_values = longest
        self.average_length = average

def print_obj(obj): 
    print(obj.__dict__)


def checkBaseType(col_name, x):
    if x == None or "":
        return "NONE"
    elif re.match(r"^-?[1-9]\d*$", x):
        return "INT"
    elif re.match(r"^-?([1-9]\d*\.\d*|0\.\d*[1-9]\d*|0?\.0+|0)$", x):
        return "REAL"
    elif len(x)>4 :
        try:
            parse(x)
            return "DATE/TIME"
        except:
            pass
    else:
        return "TEXT"




#or re.search(r"((0?[1-9])|(1[0-2]))[\-/]((0?[1-9])|([1-2][0-9])|30|31)[\-/]\d{4}" ,x)
class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
'''
        if isinstance(obj, type[time]):
            return obj.__str__()
        else:
            return super(NpEncoder, self).default(obj)
'''

def store(x, filename):
    import json
    filename = filename[0:9]
    outputfilename = filename + ".json"
    outputfile = "./result/" + outputfilename
    with open(outputfile, 'w') as f:
        f.write(json.dumps(x, cls=MyEncoder, indent=4, separators=(',', ': ')))
    f.close()



def dealwithfile(filename):
    column_list = []
    key_column_list = []
    pathname = "/user/hm74/NYCOpenData/" + filename
    data_df = spark.read.options(header = True, delimiter='\t').csv(pathname)
    col_names = data_df.columns
    data_all = data_df.rdd
    columns = []
    for i in range(len(col_names)):
        datatypes_list = []
        col_name = col_names[i]
        column = data_all.map(lambda x: x[col_name])
        print(column)
        empty_num = column.filter(lambda x: x == None or "").count()
        col_num = column.count()
        non_empty_num = col_num - empty_num
        distinct_num = column.distinct().count()
        if distinct_num == col_num:
            key_column_list.append(col_name)
        fre_out = column.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y) \
                        .sortBy(lambda x: -x[1]).map(lambda x: x[0]).take(5)
        data_types = column.map(lambda x: checkBaseType(col_name, x)).distinct().collect()
        if 'INT' in data_types:
            column_i = column.filter(lambda x: checkBaseType(col_name, x) == 'INT').map(lambda x: int(x))
            integer = INTEGER(column_i.count(),column_i.max(), column_i.min(), column_i.mean(), column_i.stdev())
            datatypes_list.append(integer.__dict__)
        if 'REAL' in data_types:
            column_r = column.filter(lambda x: checkBaseType(col_name, x) == 'REAL').map(lambda x: float(x))
            real = REAL(column_r.count(),column_r.max(), column_r.min(), column_r.mean(), column_r.stdev())
            datatypes_list.append(real.__dict__)
        if 'DATE/TIME' in data_types:
            utc = pytz.UTC
            column_d = column.filter(lambda x: checkBaseType(col_name, x) == 'DATE/TIME')\
                        .map(lambda x: (x, parse(x).replace(tzinfo=utc)))
            min_d = column_d.min(lambda x: x[1])
            max_d = column_d.max(lambda x: x[1])
            time = DATETIME(column_d.count(), max_d[0], min_d[0])
            print_obj(time)
            datatypes_list.append(time.__dict__)
        if 'TEXT' in data_types:
            column_t = column.filter(lambda x: checkBaseType(col_name, x) == 'TEXT') \
                           .map(lambda x: len(x))
            text = TEXT(column_t.count(), column_t.max(), column_t.min(), column_t.mean())
            datatypes_list.append(text.__dict__)
        sin_col = Column(col_name, non_empty_num, empty_num, distinct_num, fre_out, datatypes_list)
        column_list.append(sin_col.__dict__)
    column_arr = np.asarray(column_list)
    data_out = Dataset(filename, column_arr, key_column_list)
    store(data_out.__dict__, filename)
    


path = "./NYCOpenData/"
files = sorted(os.listdir(path))
sc = SparkContext()
'''
for file in files:
    print("Working with " + file)
    dealwithfile(file)
sc.stop()
'''

count = 0
#for file in files:
for file in files:
    spark = SparkSession.builder \
                        .appName("bigdata") \
                        .getOrCreate()
    count = count + 1
    print("Working with No." + str(count) + " file: " + file)
    try:
        dealwithfile(file)
        print("Done with " +  str(count) + " file: " + file)
    except Exception as e:
        print(file + " has Exception")
        print(e)
        w = open("file.txt", "a")
        string = file + " " + str(count) + "\n"
        w.write(string)
        w.close()
sc.stop()


