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

sc = SparkContext()
spark = SparkSession.builder.appName("projectTask1").config("spark.some.config.option", "some-value").getOrCreate()
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

# read datasets
df_rf = sqlContext.read.format('com.databricks.spark.csv'). \
    option("sep", "\t"). \
    option("header", False). \
    option("inferSchema", "true"). \
    option("multiline", "true"). \
    load('NYCOpenData/datasets.tsv')

datasets_index = df_rf.rdd.map(lambda x: (x[0], x[1])).collect()
start_position = int(sys.argv[1])
end_position = int(sys.argv[2])


count_int = 0
count_real = 0
count_date = 0
count_text = 0

count_type4 = 0   #a column contains 4 data types

count_type3_int = 0  #a column contains 3 data types that exclude int
count_type3_real = 0
count_type3_date = 0
count_type3_text = 0

count_type2_int_real = 0  #a column contains int and real
count_type2_int_date = 0
count_type2_int_text = 0
count_type2_real_date = 0
count_type2_real_text = 0
count_type2_date_text = 0

arr_json =[]
new_json = {
    "datasets": arr_json
}

for idx in range(start_position, end_position):
    try:
        with open("output_for_json/index-" + str(idx) + "-" + str(datasets_index[idx][0]) + '.json', 'r') as f:
            data = json.load(f)

        arr_json.append(data)

        list_one = data['column_specification']
        for key in list_one:
            list_two = key['data_types']
            contain = []
            for dic in list_two:
                if dic['type'] == "INTEGER(LONG)":
                    count_int = count_int + 1
                    contain.append("INTEGER(LONG)")
                elif dic['type'] == "REAL":
                    count_real = count_real + 1
                    contain.append("REAL")
                elif dic['type'] == "DATE/TIME":
                    count_date = count_date + 1
                    contain.append("DATE/TIME")
                elif dic['type'] == "TEXT":
                    count_text = count_text + 1
                    contain.append("TEXT")

            if len(contain) == 4:
                count_type4 = count_type4 + 1
            elif len(contain) == 3:
                if "INTEGER(LONG)" not in contain:
                    count_type3_int = count_type3_int + 1
                elif "REAL" not in contain:
                    count_type3_real = count_type3_real + 1
                elif "DATE/TIME" not in contain:
                    count_type3_date = count_type3_date + 1
                elif "TEXT" not in contain:
                    count_type3_text = count_type3_text + 1
            elif len(contain) == 2:
                if ("INTEGER(LONG)" in contain and "REAL" in contain):
                    count_type2_int_real = count_type2_int_real + 1
                elif ("INTEGER(LONG)" in contain and "DATE/TIME" in contain):
                    count_type2_int_date = count_type2_int_date + 1
                elif ("INTEGER(LONG)" in contain and "TEXT" in contain):
                    count_type2_int_text = count_type2_int_text + 1
                elif ("REAL" in contain and "DATE/TIME" in contain):
                    count_type2_real_date = count_type2_real_date + 1
                elif ("REAL" in contain and "TEXT" in contain):
                    count_type2_real_text = count_type2_real_text + 1
                elif ("DATE/TIME" in contain and "TEXT" in contain):
                    count_type2_date_text = count_type2_date_text + 1

    except Exception:
        print("exception")


output = {
            "int": count_int,
            "real": count_real,
            "date": count_real,
            "text": count_text,
            "count_type4": count_type4,
            "count_type3_int": count_type3_int,
            "count_type3_real": count_type3_real,
            "count_type3_date": count_type3_date,
            "count_type3_text": count_type3_text,
            "count_type2_int_real": count_type2_int_real,
            "count_type2_int_date": count_type2_int_date,
            "count_type2_int_text": count_type2_int_text,
            "count_type2_real_date": count_type2_real_date,
            "count_type2_real_text": count_type2_real_text,
            "count_type2_date_text": count_type2_date_text
}
json_str = json.dumps(output)
with open('num_output.json', 'w') as json_file:
            json_file.write(json_str)

json_str = json.dumps(new_json)
with open('whole_output.json', 'w') as json_file:
            json_file.write(json_str)




#print("int:" + str(count_int)+'\n')
#print("real:" + str(count_real)+'\n')
#print("date:" + str(count_date)+'\n')
#print("text:" + str(count_text)+'\n')

#print("count_type4:" + str(count_type4)+'\n')

#print("count_type3_int:" + str(count_type3_int)+'\n')
#print("count_type3_real:" + str(count_type3_real)+'\n')
#print("count_type3_date:" + str(count_type3_date)+'\n')
#print("count_type3_text:" + str(count_type3_text)+'\n')

#print("count_type2_int_real:" + str(count_type2_int_real)+'\n')
#print("count_type2_int_date:" + str(count_type2_int_date)+'\n')
#print("count_type2_int_text:" + str(count_type2_int_text)+'\n')
#print("count_type2_real_date:" + str(count_type2_real_date)+'\n')
#print("count_type2_real_text:" + str(count_type2_real_text)+'\n')
#print("count_type2_date_text:" + str(count_type2_date_text)+'\n')


