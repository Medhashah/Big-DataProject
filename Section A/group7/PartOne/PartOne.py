import sys
import numpy as np
import json
import re
import csv
from pyspark import SparkContext


def get_type(data):
    if re.match("^\d+?\.\d+?$", data) is not None:
        return "REAL"
    elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None:
        return "INTEGER (LONG)"
    elif re.match('^(([0-1][0-9])|([2][0-3])):([0-5][0-9])(:([0-5][0-9]))?$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
        return "DATE/TIME"
    else:
        return "TEXT"


def data_with_type(data):
    type_ = get_type(data)
    return data, type_


def get_file_names(sc, path):
    lines = sc.textFile(path+"datasets.tsv", 1)
    file_names = lines.map(lambda x: x.split('\t')).map(
        lambda x: path+x[0]+".tsv.gz").collect()
    return file_names


def sortDate(dateTime):
    # rewrite the string to yyyymmdd then let spark sort
    if '/' in dateTime:
        dateTime = dateTime.split("/")
        return dateTime[2]+dateTime[0]+dateTime[1]
    else:
        return dateTime


def profile_single_file(sc, file):
    print("Current file is: ", file[23:])

    lines = sc.textFile(file, 1).mapPartitions(
        lambda x: csv.reader(x, delimiter='\t', quotechar='"'))

    # get the header which is the column name
    header = lines.first()
    #print("the column name are: ", header)

    # modify the dataset without the header row
    lines_without_header = lines.filter(lambda line: line != header)

    columns_information = []
    key_column_candidates = []  # extra credit

    # go through every column
    for i in range(len(header)):
        lines_mapped = lines_without_header.map(lambda x: (x[i], 1))
        #print("\nCurrent Column: ", header[i])
        # Part one: question 2 --- count the empty column:
        number_empty = lines_mapped.filter(
            lambda x: x[0] is None or x[0] == 'No Data' or x[0] == '').count()
        # Part one: question 1 --- count the non empty column:
        number_non_empty = lines_mapped.count() - number_empty
        #print("Number of non-empty cells: ", number_non_empty)
        #print("Number of empty-cells: ", number_empty)

        # Part one: question 3 --- number of distinct number
        number_distinct = lines_mapped.reduceByKey(lambda x, y: x+y).count()
        #print("Number of distinct values: ", number_distinct)
        if number_distinct == lines_mapped.count():
            key_column_candidates.append(header[i])

        # Part one: question 4 --- the most 5 frequency items in each column
        top_five_freq = []
        number_frequency = lines_mapped.reduceByKey(
            lambda x, y: x+y).sortBy(lambda x: x[1], False).take(5)
        for j in range(len(number_frequency)):
            top_five_freq.append(number_frequency[j][0].strip())
        #print("Top-5 most frequent values: ", top_five_freq)

        # Part one: question 5 --- get the data type
        data_type = []
        column_data_types = lines_without_header.map(
            lambda x: (data_with_type(x[i].strip()), 1))

        column_data_if_int = column_data_types.filter(
            lambda x: x[0][1] == "INTEGER (LONG)")
        column_data_if_real = column_data_types.filter(
            lambda x: x[0][1] == 'REAL')
        column_data_if_datetime = column_data_types.filter(
            lambda x: x[0][1] == 'DATE/TIME')
        column_data_if_text = column_data_types.filter(
            lambda x: x[0][1] == 'TEXT'and x[0][0] != "No Data")

        if column_data_if_int.count() != 0:
            #print("column data has int\n")
            column_data = column_data_if_int.map(lambda x: x[0][0])
            # max and min
            max_value = column_data.sortBy(lambda x: int(x), False).take(1)
            # print("Max value is: ", max_value)
            min_value = column_data.sortBy(lambda x: int(x), True).take(1)
            # average and std
            column_data = np.array(column_data.collect()).astype('float')
            mean_value = np.mean(column_data)
            std = np.std(column_data)
            data_type.append({"type": "INTEGER (LONG)", "count": len(column_data), "max_value": int(max_value[0]),
                              "min_value": int(min_value[0]), "mean": mean_value, "stddev": std})

        if column_data_if_real.count() != 0:
            #print("column data has real\n")
            column_data = column_data_if_real.map(lambda x: x[0][0])
            # max and min
            max_value = column_data.sortBy(lambda x: float(x), False).take(1)
            min_value = column_data.sortBy(lambda x: float(x), True).take(1)
            # average and std
            column_data = np.array(column_data.collect()).astype('float')
            mean_value = np.mean(column_data)
            std = np.std(column_data)
            data_type.append({"type": "REAL", "count": len(column_data), "max_value": float(max_value[0]),
                              "min_value": float(min_value[0]), "mean": mean_value, "stddev": std})

        if column_data_if_datetime.count() != 0:
            #print("column data has datetime\n")
            column_data = column_data_if_datetime.map(lambda x: x[0][0])
            max_date_time = column_data.sortBy(
                lambda x: sortDate(x), False).take(1)
            min_date_time = column_data.sortBy(
                lambda x: sortDate(x), True).take(1)
            data_type.append({"type": "DATE/TIME", "count": column_data.count(), "max_value": max_date_time[0],
                              "min_value": min_date_time[0]})

        if column_data_if_text.count() != 0:
            #print("column data has string\n")
            # output striped text value
            column_data_with_length = column_data_if_text.map(
                lambda x: (x[0][0].strip(), len(x[0][0].strip())))
            top_longest_length = column_data_with_length.sortBy(
                lambda x: x[1], False).distinct().map(lambda x: x[0]).take(5)
            top_shortest_length = column_data_with_length.sortBy(
                lambda x: x[1], True).distinct().map(lambda x: x[0]).take(5)

            length = np.array(column_data_with_length.map(
                lambda x: x[1]).collect()).astype('float')
            avg_length = np.mean(length)
            data_type.append({"type": "TEXT", "count": len(
                length), "shortest_value": top_shortest_length, "longest_value": top_longest_length, "average_length": avg_length})

        columns_information.append({"column_name": header[i], "number_non_empty_cells": number_non_empty, "number_empty_cells":
                                    number_empty, "number_distinct_values": number_distinct, "frequent_values": top_five_freq, "data_type": data_type})

    basic_information = {"dataset_name": file, "columns": columns_information,
                         "key_column_candidates": key_column_candidates}
    with open(file[23:32]+'_result.json', 'w') as fp:
        json.dump(basic_information, fp)


sc = SparkContext()
path = "/user/hm74/NYCOpenData/"
# profile_single_file(sc, path+"i8ys-e4pm.tsv.gz")

problems = []
# file_names = get_file_names(sc, "/user/hm74/NYCOpenData/")

# this block is for second run only
file_names = []
secondFile = open("secondRunFiles.txt", "r")
for i in secondFile.readlines():
    i = path+i.strip()
    file_names.append(i)

for file_name in file_names:
    try:
        profile_single_file(sc, file_name)
    except Exception:
        problems.append(str(file_name[23:]))
        pass

with open('problemFiles.txt', 'w') as f:
    for item in problems:
        f.write("%s\n" % item)
