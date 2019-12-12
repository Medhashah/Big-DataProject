#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
from operator import add
import statistics
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import FileInputManager as fm
from dateutil import parser

key_column_threshold = 10
output_path = ''

def profileTable(data,_sc, sqlContext, table_name):
    results = []
    key_columns = []
    data_type = [0,0,0]
    for i in range(0,len(data.columns)):
        colName = fm.Process_column_name_for_dataframe(data.columns[i])
        temp_results = profile_colum(_sc, sqlContext, colName, table_name)
        results.append(temp_results[0])
        if len(key_columns) > 0:
            key_columns.append(temp_results[1])
        data_type[0] += temp_results[2][0]
        data_type[1] += temp_results[2][1]
        data_type[2] += temp_results[2][2]
    return [results, key_columns, data_type]


def profile_colum(_sc, sqlContext, colName, table_name):
    results = []
    query = "select %s from %s" % (colName, table_name)
    temp = sqlContext.sql(query)
    # get data sets
    discinct_rows = temp.distinct()
    non_empty_rows = temp.filter(temp[0].isNull())
    null_count = non_empty_rows.count()
    non_empty = temp.count() - null_count
    distinct_count = discinct_rows.count()
    query = "select %s as val, count(*) as cnt from %s group by val order by cnt desc" % (colName, table_name)
    top5 = sqlContext.sql(query)
    top5 = top5.rdd.map(lambda x: x[0]).take(5)
    data_type_stats, typeCount = calc_statistics(_sc, discinct_rows)
    temp_col_metadata = {
        "column_name": colName,
        "number_non_empty_cells": non_empty,
        "number_empty_cells": null_count,
        "number_distinct_values": distinct_count,
        "frequent_values": top5,
        "data_types": data_type_stats
    }
    results.append(temp_col_metadata)
    key_columns = []
    diff = abs(non_empty - distinct_count)
    if diff < key_column_threshold and len(key_columns) > 0:
        key_columns.append(colName)

    return results, key_columns, typeCount

def extractMeta(_sc, sqlContext, file_path, final_results):
    data = _sc.read.csv(path=file_path, sep='\t', header=True, inferSchema=False)
    col_size = len(data.columns)
    row_size = data.count()
    size_limit = 10000000
    if col_size * row_size > size_limit:
        return
    for col in range(0,len(data.columns)):
        data = data.withColumnRenamed(data.columns[col], fm.Process_column_name_for_dataframe(data.columns[col]))
    delm = ""
    if file_path.find('/') > -1:
        delm = '/'
    else:
        delm = '\\'
    table_name = file_path.split(delm)[-1]
    dot_index = table_name.find(".")
    if dot_index == -1:
        dot_index = len(table_name)
    table_name = table_name[0: dot_index].replace("-", "_")
    data.createOrReplaceTempView(table_name)
    data = profileTable(data, _sc, sqlContext, table_name)
    col_metadata = data[0]
    key_col_candidate = data[1]
    data_type_count = data[2]
    #OUTPUT
    results = {
        "dataset_name": table_name,
        "columns": col_metadata,
        "key_column_candidates": key_col_candidate,
        "data_type_count": {"integer/real": data_type_count[0],
                            "date": data_type_count[1],
                            "text": data_type_count[2]}
    }
    final_results.append(results)
    sqlContext.dropTempTable(table_name)



# getting statistics based on data type of the elements in a column
def calc_statistics(_sc, discinct_rows):
    intList =[]
    txtList=[]
    date_count = 0
    res = []
    typeCount = [0,0,0]
    rows = discinct_rows.collect()

    max_int = -100000000000
    min_int = 1000000000000

    max_date = datetime.datetime.strptime("1/1/1900 12:00:00 AM", "%m/%d/%Y %H:%M:%S %p")
    min_date = datetime.datetime.strptime("12/31/9999 12:00:00 AM", "%m/%d/%Y %H:%M:%S %p")

    for i in range(len(rows)):
        val = str(rows[i][0])
        if val.isnumeric():
            intList.append(int(val))
            max_int = max(max_int, int(val))
            min_int = min(min_int, int(val))
        else:
            #check date
            try:
                temp_date = parser.parse(val)
                max_date = max(max_date, temp_date)
                min_date = min(min_date, temp_date)
                date_count = date_count + 1
            except:
                if val != "None":
                    txtList.append(val)

    if len(intList) > 0:
        if len(intList)>1:
            result = {
                "type": "INTEGER/REAL",
                "count": len(intList),
                "max_value": max_int,
                "min_value": min_int,
                "mean": statistics.mean(intList),
                "stddev": statistics.stdev(intList)
            }
        else:
            result = {
                "type": "INTEGER/REAL",
                "count": len(intList),
                "max_value": max_int,
                "min_value": min_int,
                "mean": statistics.mean(intList),
                "stddev": 0
            }
        typeCount[0]=1
        res.append(result)

    if date_count > 0:
        result = {
            "type": "DATE/TIME",
            "count": date_count,
            "max_value": max_date.strftime("%Y/%m/%d %H:%M:%S"),
            "min_value": min_date.strftime("%Y/%m/%d %H:%M:%S")
        }
        res.append(result)
        typeCount[1]=1

    if len(txtList) > 0:
        templist = _sc.sparkContext.parallelize(txtList)
        sorted_list = templist.map(lambda x: (len(x), x)).distinct().sortBy(lambda x: x[0], ascending=False)
        longest = sorted_list.map(lambda x: x[1]).take(5)
        sorted_list = templist.map(lambda x: (len(x), x)).distinct().sortBy(lambda x: x[0], ascending=True)
        shortest = sorted_list.map(lambda x: x[1]).take(5)
        count = templist.count()
        sum = templist.map(lambda x: (len(x))).reduce(add)
        average = float(sum) / float(count)
        result = {
            "type": "TEXT",
            "count": len(txtList),
            "shortest_values": shortest,
            "longest_values": longest,
            "average_length": "%.f2" % average
        }
        res.append(result)
        typeCount[2]=1

    return res, typeCount

if __name__ == "__main__":
    config = pyspark.SparkConf().setAll(
        [('spark.executor.memory', '8g'), ('spark.executor.cores', '5'), ('spark.cores.max', '5'),
         ('spark.driver.memory', '8g')])
    sc = SparkContext(conf=config)
    sc.addFile("FileInputManager.py")
    sc.addFile("task1_coinflippers.py")
    sc.addFile("task2_coinflippers.py")

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    fm.iterate_files_from_file_for_task1(sc, spark, sqlContext, "/user/yy3090/input/task1_filename.txt",
                                         0, output_path)

    sc.stop()