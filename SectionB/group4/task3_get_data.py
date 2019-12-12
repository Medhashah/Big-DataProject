# -*- coding: UTF-8 -*-
import json
import os
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)


def cast_boro(boro_id):
    try:
        boro_id = int(boro_id)
        if boro_id == 1:
            return 'BRONX'
        if boro_id == 2:
            return 'BROOKLYN'
        if boro_id == 3:
            return 'MANHATTAN'
        if boro_id == 4:
            return 'QUEENS'
        if boro_id == 5:
            return 'STATEN ISLAND'
    except:
        return "null"


def get_food_data(data_dir, files_311):
    print("-------------------------------------------")
    print("Start Process Food 311 Problem Data")
    food_df_list = []
    upper_boro = F.udf(lambda x: x.upper() if x != 'unspecified' else 'null')
    for file in files_311:
        full_file = data_dir + file + ".tsv.gz"
        opendata_df = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(full_file)
        food_df = opendata_df.select("Created Date",
                                     "Borough",
                                     "Complaint Type") \
            .filter("`Complaint Type` like '%Food%'")
        oldColumns = food_df.schema.names
        newColumns = ["date", "borough", "type"]
        for i in range(len(oldColumns)):
            food_df = food_df.withColumnRenamed(oldColumns[i], newColumns[i])
        food_df = food_df.withColumn("borough", upper_boro(food_df.borough))
        food_df_list.append(food_df)
        print("%s processed with %s records" % (file, food_df.count()))
    res_df = reduce(lambda df1, df2: df1.union(df2), food_df_list)
    res_df.toPandas().to_csv("./task3_data/food_problems.csv", encoding='utf-8')
    print("All processed with %s records" % res_df.count())


def get_res_inspect_data(data_dir, file_res_inspect):
    print("-------------------------------------------")
    print("Start Process Restaurant Inspection Data")
    full_file = data_dir + file_res_inspect + ".tsv.gz"
    upper_boro = F.udf(lambda x: x.upper() if x != 'unspecified' else 'null')
    opendata_df = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(full_file)
    res_df = opendata_df.select("BORO", "INSPECTION DATE", "GRADE").where("ACTION not like 'No violations%'")
    res_df = res_df.withColumnRenamed("INSPECTION DATE", "DATE")
    res_df = res_df.withColumn("BORO", upper_boro(res_df.BORO))
    res_df.toPandas().to_csv("./task3_data/food_inspect.csv", encoding='utf-8')
    print("%s processed with %s records" % (file_res_inspect, res_df.count()))


def get_poverty_data(data_dir, files_poverty):
    print("-------------------------------------------")
    print("Start Process Poverty Data")
    poverty_df_list = []
    change_boro = F.udf(cast_boro)
    for i in range(len(files_poverty)):
        year = 2005 + i
        full_file = data_dir + files_poverty[i] + ".tsv.gz"
        opendata_df = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(full_file)
        poverty_df = opendata_df.select("Boro", "EducAttain", "PreTaxIncome_PU", "Off_Pov_Stat")
        poverty_df = poverty_df.withColumn('year', F.lit(year))
        poverty_df = poverty_df.withColumn('Boro', change_boro(poverty_df.Boro))
        poverty_df_list.append(poverty_df)
        print("%s of year %s processed with %s records" % (files_poverty[i], year, poverty_df.count()))
    res_df = reduce(lambda df1, df2: df1.union(df2), poverty_df_list)
    res_df.toPandas().to_csv("./task3_data/poverty.csv", encoding='utf-8')
    print("All processed with %s records" % res_df.count())


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("bigdata_project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # get file and dir
    mkdir("./task3_data")
    data_dir = "/user/hm74/NYCOpenData/"
    # 311 Service Requests for 2004~present & Web Content - Services
    files_311 = ['sqcr-6mww', 'sxmw-f24h', 'hy4q-igkk', 'aiww-p3af', 'uzcy-9puk', '3rfa-3xsf', 'erm2-nwe9']
    # DOHMH New York City Restaurant Inspection Results
    file_res_inspect = '43nn-pn8j'
    # NYCgov Poverty Measure Data (2005-2017)
    files_poverty = ['nat5-7dxa', '43yz-zfrw', 'ka3c-bcci', 'qbb6-g5em',
                     'wfw5-9psu', '9cx5-kdi8', 'weuc-cs8c', '9nyy-a6qt',
                     'expp-gbrz', 'gi42-6x8d', '9ny4-8k6g', 'y9gu-cxxw',
                     'mwus-92t3']
    get_food_data(data_dir, files_311)
    get_res_inspect_data(data_dir, file_res_inspect)
    get_poverty_data(data_dir, files_poverty)
