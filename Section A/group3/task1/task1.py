#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from csv import reader
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
import json
from dateutil.parser import parse
from pyspark.sql.functions import col


def get_type(cell_value):
    if cell_value:
        try:
            y = int(cell_value)
            return 1
        except ValueError:
            try:
                y = float(cell_value)
                return 2
            except ValueError:
                try:
                    y = parse(cell_value)
                    return 3
                except (ValueError, OverflowError):
                    return 4
    else:
        return 0


spark = SparkSession \
    .builder \
    .getOrCreate()
df = spark.read.csv(sys.argv[1], sep='\t', header='true')

#table1 = df.replace("No Data",'null').replace('n/a','null').replace('NA','null').replace('-','null').replace('None','null')
df.createOrReplaceTempView("table1")
result_json = {
    "dataset_name": sys.argv[1].split('/')[-1]
}
result_json["columns"] = list()
candidate_keys=[]

for column in df.columns:
    nonemptycells = 0
    emptycells = 0
    result = spark.sql("SELECT COUNT(`"+column+"`) as cellcount from table1 WHERE `" +
                       column + "`<> 'null' and `"+column + "` is not null ")
    nonempty = result.collect()
    nonemptycells = nonempty[0].cellcount
    result = spark.sql("SELECT COUNT(`"+column+"`) as cellcount from table1 WHERE `" +
                       column + "`= 'null' or `"+column + "` is  null ")
    empty = result.collect()
    emptycells = empty[0].cellcount
    result = spark.sql("SELECT COUNT(DISTINCT `"+column +
                       "`) as distinct_count from table1")
    distinct_values = result.collect()[0].distinct_count
    result = spark.sql("SELECT `"+column+"` as attr, COUNT(`"+column +
                       "`) as frequency from table1 GROUP BY `"+column+"` ORDER BY frequency DESC LIMIT 5")
    top_frequent_elements = [row.attr for row in result.collect()]

    column_json = {
        "column_name": column,
        "number_non_empty_cells": nonemptycells,
        "number__empty_cells": emptycells,
        "number_distinct_values": distinct_values,
        "frequent_values": top_frequent_elements
    }

    #Calculate candidate keys
    if(df.select(column).count()==df.select(column).distinct().count()):
        candidate_keys.append(column)

    column_json["data_types"] = list()
    get_data_type = F.udf(get_type, IntegerType())
    current = df.select(column, get_data_type(column).alias("data_type"))
    int_type = current.where(current["data_type"] == 1)
    float_type = current.where(current["data_type"] == 2)
    date_type = current.where(current["data_type"] == 3)
    string_type = current.where(current["data_type"] == 4)
    if int_type.count() != 0:
        intJson = {"type": "INTEGER (LONG)",
                   "count": int_type.count(),
                   "max_value": int_type.select(F.max(col(column).cast(IntegerType()))).collect()[0][0],
                   "min_value": int_type.select(F.min(col(column).cast(IntegerType()))).collect()[0][0],
                   "mean": int_type.select(F.mean(column)).collect()[0][0],
                   "stddev": int_type.select(F.stddev(column)).collect()[0][0]
                   }
        column_json["data_types"].append(intJson)
    if float_type.count() != 0:
        realJson = {"type": "REAL",
                    "count": float_type.count(),
                    "max_value": float_type.select(F.max(col(column).cast(DoubleType()))).collect()[0][0],
                    "min_value": float_type.select(F.min(col(column).cast(DoubleType()))).collect()[0][0],
                    "mean": float_type.select(F.mean(column)).collect()[0][0],
                    "stddev": float_type.select(F.stddev(column)).collect()[0][0]
                    }
        column_json["data_types"].append(realJson)
    if date_type.count() != 0:
        dateTimeJson = {"type": "DATE/TIME",
                        "count": date_type.count(),
                        "max_value":date_type.select(F.max(column)).collect()[0][0], # yet to code -- waiting for date time data type identification
        				"min_value":date_type.select(F.max(column)).collect()[0][0] # yet
                        }
        column_json["data_types"].append(dateTimeJson)
    if string_type.count() != 0:
        shortest_values = string_type.select(column).distinct().orderBy(
            F.length(column), ascending=True).limit(5).rdd.flatMap(lambda x: x).collect()
        longest_values = string_type.select(column).distinct().orderBy(
            F.length(column), ascending=False).limit(5).rdd.flatMap(lambda x: x).collect()
        avg_length = string_type.select(
            F.avg(F.length(column))).collect()[0][0]
        textJson = {"type": "TEXT",
                    "count": string_type.count(),
                    "shortest_values": shortest_values,
                    "longest_values": longest_values,
                    "average_length": avg_length
                    }
        column_json["data_types"].append(textJson)
    result_json["columns"].append(column_json)

filename = sys.argv[1].split('/')[-1] + ".json"
with open(filename, 'w') as f:
    json.dump(result_json, f)
spark.stop()
