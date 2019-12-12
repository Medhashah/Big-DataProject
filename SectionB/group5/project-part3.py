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

sc = SparkContext()
spark = SparkSession \
    .builder \
    .appName("project-part3") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#guess type of a string
def guess_type(s):
    if re.match("\A[0-9]+\.[0-9]+\Z", s):
        return float
    elif re.match("\A[0-9]+\Z", s):
        return int
    elif re.match("^(?:\d{1,2}(?:(?:-|/)|(?:th|st|nd|rd)?\s))?(?:(?:(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)(?:(?:-|/)|(?:,|\.)?\s)?)?(?:\d{1,2}(?:(?:-|/)|(?:th|st|nd|rd)?\s))?)(?:\d{2,4})$", s): 
        return datetime.date
    else:
        return str

f = "8dhd-zvi6"
df = spark.read.option("sep", "\t").option("header", "true").option("encoding", "utf-8").csv("/user/hm74/NYCOpenData/" + f + ".tsv.gz")
columns = df.columns

missing = {}
table = []
count = 0
js = {}
js["null"] = []
js["disguised"] = []

for i in range(len(columns)):
    c = df.columns[i]
    df_sub = df.select(c).collect()
    current = [row[c] for row in df_sub]
    table.append(current)
    num_size = 0
    str_size = 0
    # 1. find all the empty cells
    for j in range(len(current)):
        t = current[j]
        if (not t):
            missing[(i, j)] = 1
            break
        if (type(t) == int or type(t) == float):
            num_size = num_size + 1
        else:
            v = guess_type(t)
            if (v == int or v == float):
                num_size = num_size + 1
            else:
                str_size = str_size + 1
    # 2. find all the value that is not in the domain of current column
    column_type = ""
    if (num_size > str_size):
        column_type = "num"
    else:
        column_type = "str"
    col_dict = {}
    col_dict["column_name"] = columns[i]
    col_dict["null_value"] = []
    for j in range(len(current)):
        t = current[j]
        if (not t):
            break
        if (type(t) == int or type(t) == float):
            if (column_type == "str"):
                missing[(i, j)] = 1
                col_dict["null_value"].append(t)
        else:
            v = guess_type(t)
            if (v != int and v!= float and column_type == "num"):
                missing[(i, j)] = 1
                col_dict["null_value"].append(t)
    js["null"].append(col_dict)

# 3. disguised missing value
disguised_missing = {}
value_pairs = {}
value_1 = {}
value_2 = {}
a = 0
b = 1
# find all the value pairs and distinct value and their counts
for i in range(len(table[0])):
    if ((table[a][i], table[b][i]) not in value_pairs):
        value_pairs[(table[a][i], table[b][i])] = 1
    else:
        value_pairs[(table[a][i], table[b][i])] = value_pairs[(table[a][i], table[b][i])] + 1
    if (table[a][i] not in value_1):
        value_1[table[a][i]] = 1
    else:
        value_1[table[a][i]] = value_1[table[a][i]] + 1
    if (table[b][i] not in value_2):
        value_2[table[b][i]] = 1
    else:
        value_2[table[b][i]] = value_2[table[b][i]] + 1
# for each column
for i in range(len(table)):
    values = {}
    col_dict = {}
    col_dict["column_name"] = columns[i]
    # find the size of MEUS for every value
    for j in range(len(table[i])):
        value = table[i][j]
        if (value not in values.keys() and (i, j) not in missing):
            new_table = []
            for k in range(len(table[i])):
                if (table[i][k] == value):
                    new_table.append([table[a][k], table[b][k]])
            flag = True
            while (flag):
                new_table_pairs = {}
                new_value_1 = {}
                new_value_2 = {}
                for k in range(len(new_table)):
                    x1 = new_table[k][0]
                    x2 = new_table[k][1]
                    if ((x1, x2) not in new_table_pairs):
                        new_table_pairs[(x1, x2)] = 1
                    else:
                        new_table_pairs[(x1, x2)] = new_table_pairs[(x1, x2)] + 1
                    if (x1 not in new_value_1):
                        new_value_1[x1] = 1
                    else:
                        new_value_1[x1] = new_value_1[x1] + 1
                    if (x2 not in new_value_2):
                        new_value_2[x2] = 1
                    else:
                        new_value_2[x2] = new_value_2[x2] + 1
                # calculate the dv-score of this new table
                score = 0
                for key in new_table_pairs:
                    x1 = key[0]
                    x2 = key[1]
                    corr_1 = (new_table_pairs[(x1, x2)] * len(new_table)) / (new_value_1[x1] * new_value_2[x2])
                    corr_2 = (value_pairs[(x1, x2)] * len(table[i])) / (value_1[x1] * value_2[x2])
                    p = value_pairs[(x1, x2)] / len(table[i])
                    score = score + (p / (1 + abs(corr_1 - corr_2)))
                # find the tuple with the max dv-score gain
                max_score = 0
                best_row = 0
                for k in range(len(new_table)):
                    t1 = new_table[k][0]
                    t2 = new_table[k][1]
                    temp_table = []
                    for t in range(len(new_table)):
                        if (t != k):
                            temp_table.append(new_table[t])
                    temp_pair = {}
                    temp_value_1 = {}
                    temp_value_2 = {}
                    for t in range(len(temp_table)):
                        x1 = temp_table[t][0]
                        x2 = temp_table[t][1]
                        if ((x1, x2) not in temp_pair):
                            temp_pair[(x1, x2)] = 1
                        else:
                            temp_pair[(x1, x2)] = temp_pair[(x1, x2)] + 1
                        if (x1 not in temp_value_1):
                            temp_value_1[x1] = 1
                        else:
                            temp_value_1[x1] = temp_value_1[x1] + 1
                        if (x2 not in temp_value_2):
                            temp_value_2[x2] = 1
                        else:
                            temp_value_2[x2] = temp_value_2[x2] + 1
                    temp_score = 0
                    for key in temp_pair:
                        x1 = key[0]
                        x2 = key[1]
                        corr_1 = (temp_pair[(x1, x2)] * len(new_table)) / (temp_value_1[x1] * temp_value_2[x2])
                        corr_2 = (value_pairs[(x1, x2)] * len(table[i])) / (value_1[x1] * value_2[x2])
                        p = value_pairs[(x1, x2)] / len(table[i])
                        temp_score = temp_score + (p / (1 + abs(corr_1 - corr_2)))
                    if (temp_score > score):
                        if (temp_score > max_score):
                            max_score = temp_score
                            best_row = k
                if (k == 0):
                    flag = False
                    break
                else:
                    del new_table[best_row]
            values[value] = len(new_table)
    max = 0
    best = None
    for key in values:
        if (values[key] > max):
            max = values[key]
            best = key
    for j in range(len(table[i])):
        if (table[i][j] == best):
            disguised_missing[(i, j)] = 1
    col_dict["disguised_missing_value"] = best
    js["disguised"].append(col_dict)

with open('/home/jz3536/null/' + f + '.json', 'w+') as outfile:
    json.dump(js, outfile)
    outfile.close()

sc.stop()