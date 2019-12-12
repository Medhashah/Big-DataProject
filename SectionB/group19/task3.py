import sys
import pyspark
import string
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, countDistinct, desc
import os
import re
from pyspark.sql.types import FloatType, IntegerType



def date_num(string):
    arr = string.split('-')
    res = int(arr[0])
    res = res * 13 + int(arr[1])
    res = res * 32 + int(arr[2])
    return res




sc = SparkContext()
spark = SparkSession.builder.appName("projectTask1").config("spark.some.config.option", "some-value").getOrCreate()
#sqlContext = SQLContext(spark)
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark )
df = sqlContext.read.format('com.databricks.spark.csv'). \
    option("header", True). \
    option("inferSchema", "true"). \
    option("multiline", "true"). \
    load('AB_NYC_2019.csv')


dic_neigh = {'Brooklyn', 'Manhattan', 'Queens', 'Staten Island', 'Bronx'}
dic_room = {'Private room', 'Entire home/apt', 'Shared room'}
nullDict = {"NULL","Null","null","N/A","-","NaN"," ","999-999-99999"}

res_nullList = []
column_specification = []
output_json = {
            "column_specification": column_specification,
            "null_value": res_nullList
        }


num_df = df.count()
bounds = {}

#judge for null value

len_col = len(df.columns)
lists = df.collect()
for item in lists:
    for i in range(0, len_col):
        if item[i] in nullDict:
            null_val = item[i]
            if null_val not in res_nullList:
                res_nullList.append(null_val)



for column in df.columns:
    df_col = df.select(column)

    col_json = {
        "column_name": column,
    }




    if column == 'neighbourhood_group':
        rdd_neigh = df_col.rdd.filter(lambda x: x[0] not in dic_neigh).map(lambda x:x[0]).collect()
        col_json['outlier'] = rdd_neigh
        column_specification.append(col_json)
        continue
    elif column == 'room_type':
        rdd_room = df_col.rdd.filter(lambda x: x[0] not in dic_room).map(lambda x: x[0]).collect()
        col_json['outlier'] = rdd_room
        column_specification.append(col_json)
        continue



    df_integer = df_col.filter(df_col[column].rlike('^\-{0,1}\d+$'))  # Matching integers with regular expressions
    count_integer = df_integer.count()
    if count_integer > num_df / 2:
        df_integer = df_integer.withColumn(column, df_integer[column].cast('integer'))
        quantiles = df_integer.approxQuantile(column, [0.15, 0.85], 0.05)
        IQR = quantiles[1] - quantiles[0]
        bounds[column] = [
            quantiles[0] - 1.5 * IQR,
            quantiles[1] + 1.5 * IQR
        ]
        rdd_integer = df_integer.rdd.filter(lambda x: x[0] < bounds[column][0] or x[0] > bounds[column][1]).map(lambda x: x[0]).collect()
        col_json['outlier'] = rdd_integer




    df_real = df_col.filter(df_col[column].rlike('^\-{0,1}\d+\.[0-9]+$'))
    count_real = df_real.count()
    if count_real > num_df / 2:
        df_real = df_real.withColumn(column, df_real[column].cast('double'))
        #dataframe.withColumn('gen_val', dataframe['gen_val'].cast('double'))
        quantiles = df_real.approxQuantile(column, [0.15, 0.85], 0.05)
        IQR = quantiles[1] - quantiles[0]
        bounds[column] = [
            quantiles[0] - 1.5 * IQR,
            quantiles[1] + 1.5 * IQR
        ]
        rdd_real = df_real.rdd.filter(lambda x: x[0] < bounds[column][0] or x[0] > bounds[column][1]).map(lambda x: x[0]).collect()
        col_json['outlier'] = rdd_real



    df_date = df_col.filter(df_col[column].rlike('^\d{4}\-\d{1,2}\-\d{1,2}.*$'))
    count_date = df_date.count()
    if count_date > num_df / 2:
        rdd_date = df_date.rdd.map(lambda x: (x[0], date_num(x[0])))
        df_data_two = rdd_date.toDF(["date", "num"])
        quantiles = df_data_two.approxQuantile('num', [0.15, 0.85], 0.05)
        IQR = quantiles[1] - quantiles[0]
        bounds[column] = [
            quantiles[0] - 1.5 * IQR,
            quantiles[1] + 1.5 * IQR
        ]
        rdd_date = rdd_date.filter(lambda x: x[1] < bounds[column][0] or x[1] > bounds[column][1]).map(lambda x: x[0]).collect()
        col_json['outlier'] = rdd_date


    column_specification.append(col_json)



json_str = json.dumps(output_json)
with open('task3.json', 'w') as json_file:
    json_file.write(json_str)





#print(bounds)
