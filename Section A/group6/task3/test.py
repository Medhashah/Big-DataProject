from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col
from csv import reader
from pyspark.sql.functions import desc
from dateutil.parser import parse
import datetime
import pytz
import time
import re





NULL = "NULL"
INTEGER = "INTEGER (LONG)"
REAL = "REAL"
DATE = "DATE/TIME"
TEXT = "TEXT"
tablename = 'table'

if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession \
        .builder \
        .appName("final-project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # load table into database
    # Setup
    filepath = 'complain_tables.txt'
    year_list = [2007, 2010, 2006, 2004, 2005, 2008, 2009]
    with open(filepath) as fp:
        line = fp.readline()
        idx = 0
        while line:
            # try:
                start_time = time.time()
                arr = line.split(",")
                index = int(arr[0].strip())
                argv1 = arr[1].replace("'", "").strip()
                argv2 = arr[2].replace("'", "").strip()
                line = fp.readline()
                table = spark.read.format('csv') \
                    .options(delimiter="\t", header='true', inferschema='true') \
                    .load(argv1)
                # create table
                table.createOrReplaceTempView(tablename)
                # the name of the dataset correlating to the tsv file
                filename = argv2
                inFile = argv1.split('.', 1)[0]
                print("profiling file:" + filename)

                columns = {}  # a list of columns
                key_column_candidates = []  # all the columns that could be primary key candidates

                # go through the table and for each column, create the column_specification content
                Borough = 'Borough'
                type = 'Complaint Type'
                Borough_type_df = spark.sql('SELECT `%s` as Borough , `%s` as Complaint_type, count(*) as _count  FROM `%s`                                            \
                               GROUP BY  `%s`, `%s`                                 \
                               order by count(*) DESC' % (Borough, type, tablename, Borough, type)) \
                .collect()
                # Borough_type_df.show()
                type_count_dict = {}
                # Borough_list = ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'Unspecified']
                for row in Borough_type_df :
                    borough = row.Borough
                    type = row.Complaint_type
                    count = row._count
                    if type_count_dict.__contains__(borough) :
                        type_count_dict[borough][type] = count
                    else :
                        type_count_dict[borough] = {}
                # print(len(type_count_dict))
                #
                for key in type_count_dict.keys() :
                    idx = 0
                    print("Borough :", key)
                    for type in type_count_dict[key] :
                        if idx < 3 :
                            print(type)
                            idx += 1
                    print('\n')

    sc.stop()


