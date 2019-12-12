#!/usr/bin/env python
# coding: utf-8

import os, sys
import pyspark
import string
import json
import time
import dateutil.parser
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *


def identify(value):
    try:
        check = float(value)
        if check.is_integer():
            return "INTEGER"
        elif check: 
            return "REAL"
    except:
        pass

    try:
        s = str(value)
        check = dateutil.parser.parse(s)
        return "DATE/TIME"
    except:
        return "TEXT"
 
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


if __name__ == "__main__":

    start_time = time.time()

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("proj") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    outputJSON = []
    #inFile = sys.argv[1]
    file_id = 1

    inFile = '/user/hm74/NYCOpenData'
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(inFile))
    file_list = []
    for file in list_status:
        file_list.append((file.getPath().getName(), file.getLen()))
    file_list.sort(key = lambda x: x[1])

    fail_write = []

    for file in file_list:
        print("---" + str(file_id) + "th file---")
        fileName = file[0]
        print(fileName)
        file_id += 1

        temp_JSON = dict()
        temp_JSON['dataset_name'] = fileName
        path = inFile + "/" + fileName
        
        spark = SparkSession(sc)
        test = sqlContext.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(path)
        test.createOrReplaceTempView("test")
        sqlContext.cacheTable("test")
        #spark.sql("select * from test").show()
        #spark.sql("select count(*) from test").show()
        attrs = test.schema.names
        columns = []
        candidate_keys = []

        for attr in attrs:
            temp_column = dict()
            #print("column name:", attr)
            temp_column['column_name'] = attr
            temp = spark.sql("SELECT `" + attr + "` as attr FROM test")
            #temp.show()
            temp.createOrReplaceTempView("temp")
            temp_count = temp.count()

            temp1 = spark.sql("SELECT count(*) as count FROM temp WHERE attr IS NOT NULL")
            #temp1.show()
            t1 = temp1.select("count").collect()[0][0]
            temp_column['number_non_empty_cells'] = t1;

            temp2 = spark.sql("SELECT count(*) as count FROM temp WHERE attr IS NULL")
            #temp2.show()
            t2 = temp2.select("count").collect()[0][0]
            temp_column['number_empty_cells'] = t2;

            temp3 = spark.sql("SELECT count(distinct(attr)) as count FROM temp")
            #temp3.show()
            t3 = temp3.select("count").collect()[0][0]
            temp_column['number_distinct_values'] = t3;
            
            temp4 = spark.sql("SELECT attr, COUNT(*) FROM temp GROUP BY attr ORDER BY COUNT(*) DESC LIMIT 5")
            #temp4.show()
            t4_array = [str(row.attr) for row in temp4.collect()]
            temp_column['frequent_values'] = t4_array;

            #candidate key
            #primary key: values can't be null and have to be distinct
            if temp_count == t1 and t1 == t3:
                candidate_keys.append(attr)
            elif temp_count == t3:
                candidate_keys.append(attr)

            temp_no_null = spark.sql("SELECT attr FROM temp WHERE attr IS NOT NULL")
            temp_no_null.createOrReplaceTempView("temp_no_null")
            
            identify_types = udf(lambda x: identify(x))
            temp5 = temp_no_null.withColumn('types', identify_types('attr'))
            temp5.createOrReplaceTempView("temp5")
            temp6 = spark.sql("SELECT distinct(types) FROM temp5")
            #temp6.show()

            types_array = [str(row.types) for row in temp6.collect()]
            types = []
            for type_ in types_array:
                temp_types = dict()
                if type_ == 'INTEGER':
                    temp_types['type'] = 'INTEGER (LONG)'
                    temp_int = spark.sql("SELECT attr, types FROM temp5 WHERE types = 'INTEGER'")
                    #temp_real.createOrReplaceTempView("temp_real")
                    to_int = udf(lambda x: int(float(x)))
                    temp_int = temp_int.withColumn('attr', to_int('attr'))
                    temp_int.createOrReplaceTempView("temp_int")
                    temp5_int = spark.sql("SELECT count(*) as count, max(attr) as max, min(attr) as min, mean(attr) as mean, stddev(attr) as stddev FROM temp_int WHERE types = 'INTEGER'")
                    #temp5_int.show()
                    t5 = temp5_int.select("count").collect()[0][0]
                    temp_types['count'] = t5
                    t5 = temp5_int.select("max").collect()[0][0]
                    temp_types['max_value'] = t5
                    t5 = temp5_int.select("min").collect()[0][0]
                    temp_types['min_value'] = t5
                    t5 = temp5_int.select("mean").collect()[0][0]
                    temp_types['mean'] = t5
                    t5 = temp5_int.select("stddev").collect()[0][0]
                    temp_types['stddev'] = t5

                elif type_ == 'REAL':
                    temp_types['type'] = 'REAL'
                    temp_real = spark.sql("SELECT attr, types FROM temp5 WHERE types = 'REAL'")
                    #temp_real.createOrReplaceTempView("temp_real")
                    to_float = udf(lambda x: float(x))
                    temp_real = temp_real.withColumn('attr', to_float('attr'))
                    temp_real.createOrReplaceTempView("temp_real")
                    temp5_real = spark.sql("SELECT count(*) as count, max(float(attr)) as max, min(float(attr)) as min, mean(attr) as mean, stddev(attr) as stddev FROM temp_real WHERE types = 'REAL'")
                    #temp5_real.show()
                    t5 = temp5_real.select("count").collect()[0][0]
                    temp_types['count'] = t5
                    t5 = temp5_real.select("max").collect()[0][0]
                    temp_types['max_value'] = t5
                    t5 = temp5_real.select("min").collect()[0][0]
                    temp_types['min_value'] = t5
                    t5 = temp5_real.select("mean").collect()[0][0]
                    temp_types['mean'] = t5
                    t5 = temp5_real.select("stddev").collect()[0][0]
                    temp_types['stddev'] = t5

                elif type_ == 'DATE/TIME':
                    temp_types['type'] = 'DATE/TIME'
                    temp5_date = spark.sql("SELECT count(*) as count, max(attr) as max_date, min(attr) as min_date FROM temp5 WHERE types = 'DATE/TIME'")
                    t5 = temp5_date.select("count").collect()[0][0]
                    temp_types['count'] = t5
                    t5 = temp5_date.select("max_date").collect()[0][0]
                    temp_types['max_value'] = t5
                    t5 = temp5_date.select("min_date").collect()[0][0]
                    temp_types['min_value'] = t5
                    #temp5_date.show()

                elif type_ == 'TEXT':
                    temp_types['type'] = 'TEXT'
                    temp5_text_s = spark.sql("SELECT attr, length(attr) as len FROM temp5 WHERE types = 'TEXT' ORDER BY len LIMIT 5")
                    temp5_text_l = spark.sql("SELECT attr, length(attr) as len FROM temp5 WHERE types = 'TEXT' ORDER BY len DESC LIMIT 5")
                    temp5_text = spark.sql("SELECT count(*) as count, avg(length(attr)) as len_ave FROM temp5 WHERE types = 'TEXT'")
                    t5 = temp5_text.select("count").collect()[0][0]
                    temp_types['count'] = t5
                    t5_short_array = [str(row.attr) for row in temp5_text_s.collect()]
                    temp_types['shortest_values'] = t5_short_array
                    t5_long_array = [str(row.attr) for row in temp5_text_l.collect()]
                    temp_types['longest_values'] = t5_long_array
                    t5 = temp5_text.select("len_ave").collect()[0][0]
                    temp_types['average_length'] = t5

                    #temp5_text_s.show()
                    #temp5_text_l.show()
                    #temp5_text.show()
                types.append(temp_types)

            temp_column['data_types'] = types
            columns.append(temp_column)


        temp_JSON['columns'] = columns
        temp_JSON['key_column_candidates'] = candidate_keys
        outputJSON.append(temp_JSON)
        #print(temp_JSON)

        try:
            s = "json/" + fileName + ".json"
            with open(s, 'w') as f:
                json.dump(temp_JSON, f, cls=DateTimeEncoder)
        except:
            print("---" + str((file_id-1)) + "th file fail to write to JSON---")
            fail_write.append(file_id-1)


    print("--- %s seconds to completed ---" % (time.time() - start_time))

    sc.stop()

