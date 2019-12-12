#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string
import json
import subprocess

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("part1_cooccur") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

sqlContext = SQLContext(spark)


def parse_json_one_row(row):
    type_list = []
    dataset = row["dataset_name"]
    for w in row["columns"]:
        name = w["column_name"]
        for s in w["data_type"]:
            type_list.append((dataset+'/'+name, s["type"]))
    return type_list


def parse_json(files):
    schema = StructType([
        StructField("uid", StringType(), True),
        StructField("val", StringType(), True)])
    list_all = []
    for f in files:
        try:
            df_json = spark.read.json(f).rdd
            df_parsed = df_json.flatMap(lambda r: parse_json_one_row(r))
            list_all.append(df_parsed)
        except Exception as e:
            print("cannot read {}".format(f))

    rdd_all = sc.union(list_all)
    df_all = spark.createDataFrame(rdd_all, schema)

    return df_all


def compute_relationship(df_type, supp):
    # df_type = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
    df_type.createOrReplaceTempView("df_type")
    sqlContext.cacheTable("df_type")
    spark.sql("select count(*) from df_type").show()

    # compute frequent itemsets of size 1, store in F1(attr, val)
    query = "select val, count(*) as supp \
                   from df_type \
                  group by val \
                 having count(*) >= " + str(supp)
    F1 = spark.sql(query)
    F1.createOrReplaceTempView("F1")
    print("F1 computation finished")

    # Compute R2, as described in the homework specification
    # R2(attr1, val1, attr2, val2, supp, conf)

    # sort items in order: val into lexicographic order
    query = "select * from F1 \
                 order by F1.val"
    F1 = spark.sql(query)
    F1.createOrReplaceTempView("F1")

    # Apriori
    # candidate generating
    query = "select p.val as val1, q.val as val2 \
                 from F1 as p join F1 as q \
                 on p.val < q.val"
    C = spark.sql(query)
    C.createOrReplaceTempView("C")

    # compute support
    query = "select C.val1, C.val2, count(*) as supp \
                 from C join df_type as p join df_type as q \
                 on p.uid = q.uid \
                   and p.val = C.val1 \
                   and q.val = C.val2 \
                 group by C.val1, C.val2 \
                 having count(*) >=" + str(supp)

    F2 = spark.sql(query)
    F2.createOrReplaceTempView("F2")
    print("F2 computation finished")

    # generating association rules
    # query = "select F2.val1, F2.val2, F2.supp, F2.supp/F1.supp as conf \
    #              from F1 join F2 \
    #              on F1.val = F2.val1 \
    #                 and F2.supp/F1.supp >=" + str(conf)
    # R2 = spark.sql(query)
    # R2.createOrReplaceTempView("R2")

    # Compute R3, as described in the homework specification
    # R3(attr1, val1, attr2, val2, attr3, val3, supp, conf)

    # Apriori
    # candidate generating
    query = "select p.val1 as val1, \
                    p.val2 as val2, \
                    q.val2 as val3 \
                 from F2 as p join F2 as q \
                 on p.val1 = q.val1 \
                    and p.val2 < q.val2"
    C = spark.sql(query)
    C.createOrReplaceTempView("C")

    # compute support using portions of df_type
    query = "select C.val1, C.val2, C.val3, count(*) as supp \
                 from C join df_type as p join df_type as q join df_type as s \
                 on p.uid = q.uid and p.uid = s.uid \
                    and p.val = C.val1 \
                    and q.val = C.val2 \
                    and s.val = C.val3 \
                 group by C.val1, C.val2, C.val3 \
                 having count(*) >=" + str(supp)

    F3 = spark.sql(query)
    F3.createOrReplaceTempView("F3")
    print("F3 computation finished")

    # generating association rules
    # query = "select F3.attr1, F3.val1, F3.attr2, F3.val2, F3.attr3, F3.val3, F3.supp, F3.supp/F2.supp as conf \
    #              from F2 join F3 \
    #              on F3.attr3 = 'vdecile' \
    #                 and F2.attr1 = F3.attr1 and F2.val1 = F3.val1 \
    #                 and F2.attr2 = F3.attr2 and F2.val2 = F3.val2 \
    #                 and F3.supp/F2.supp >=" + str(conf)
    # R3 = spark.sql(query)
    # R3.createOrReplaceTempView("R3")
    # Apriori
    # candidate generating
    query = "select p.val1 as val1, \
                        p.val2 as val2, \
                        p.val3 as val3, \
                        q.val3 as val4 \
                     from F3 as p join F3 as q \
                     on p.val1 = q.val1 \
                        and p.val2 = q.val2 \
                        and p.val3 < q.val3"
    C = spark.sql(query)
    C.createOrReplaceTempView("C")

    # compute support using portions of df_type
    query = "select C.val1, C.val2, C.val3, C.val4, count(*) as supp \
                     from C join df_type as p join df_type as q join df_type as s join df_type as m \
                     on p.uid = q.uid and p.uid = s.uid and p.uid = m.uid \
                        and p.val = C.val1 \
                        and q.val = C.val2 \
                        and s.val = C.val3 \
                        and m.val = C.val4 \
                     group by C.val1, C.val2, C.val3, C.val4 \
                     having count(*) >=" + str(supp)

    F4 = spark.sql(query)
    # F4.createOrReplaceTempView("F3")
    print("F4 computation finished")

    return F2, F3, F4


if __name__ == "__main__":
    # get command-line arguments
    inFile = sys.argv[1]
    supp = sys.argv[2]

    print(
        "Executing Task 1 co-occur with input from " + inFile + ", support=" + supp)

    cmd = "hadoop fs -ls " + inFile
    files = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
    pfiles = [x.split()[7] for x in files[1:]]
    df_all = parse_json(pfiles)
    print("parsing json files finished")

    F2, F3, F4 = compute_relationship(df_all, supp)
    F2.show()
    F3.show()
    F4.show()
