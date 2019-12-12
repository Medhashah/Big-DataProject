import numpy as np
import json
import re
from pyspark import SparkContext
import csv
from csv import reader
import re
from pyspark.sql import SQLContext
import json
import pandas as pd

def get_type(data):
    if re.match("^\d+?\.\d+?$", data) is not None:
        return "REAL"
    elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None:
        return "INTEGER (LONG)"
    elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9])::([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
        return "DATE/TIME"
    else:
        return "TEXT"


def data_with_type(data):
    type_ = get_type(data)
    return data, type_

def sortDate(date):
    # rewrite the string to yyyymmdd then let spark sort
    date = date.split(" ")
    date = str(date[0]).split('/')
    return date[2]

def profile_single_file(sc, file):
    lines = sc.textFile(file, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    # split all lines with \t
    #lines = lines.map(lambda x: x.split('\t'))
    # get the header which is the column name
    header = lines.first()
    print("the column name are: ", header)
    header_list = list(header)
    # modify the dataset without the header row
    lines_without_header = lines.filter(lambda line: line != header)
    name = ["Complaint Type", "Incident Zip", "Created Date"]
    date_index = header_list.index(name[2])
    type_index = header_list.index(name[0])
    neigh_index = header_list.index(name[1])
    print(type_index)
    print(neigh_index)
    type_bor = lines_without_header.map(lambda x: (data_with_type(x[date_index]), x[neigh_index], x[type_index]))
    sort_type_bor = type_bor.map(lambda x: (sortDate(x[0][0]),x[1], x[2]))
    data_2010 = sort_type_bor.filter(lambda x: x[0] == '2010').map(lambda x: ((x[1],x[2]),1)).reduceByKey(lambda x,y :x+y).sortBy(lambda x: -x[1])
    data_2011 = sort_type_bor.filter(lambda x: x[0] == '2011').map(lambda x: ((x[1],x[2]),1)).reduceByKey(lambda x,y :x+y).sortBy(lambda x: -x[1])
    data_2012 = sort_type_bor.filter(lambda x: x[0] == '2012').map(lambda x: ((x[1],x[2]),1)).reduceByKey(lambda x,y :x+y).sortBy(lambda x: -x[1])
    data_2013 = sort_type_bor.filter(lambda x: x[0] == '2013').map(lambda x: ((x[1],x[2]),1)).reduceByKey(lambda x,y :x+y).sortBy(lambda x: -x[1])
    data_2014 = sort_type_bor.filter(lambda x: x[0] == '2014').map(lambda x: ((x[1],x[2]),1)).reduceByKey(lambda x,y :x+y).sortBy(lambda x: -x[1])
    data_2015 = sort_type_bor.filter(lambda x: x[0] == '2015').map(lambda x: ((x[1], x[2]), 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])
    data_2016 = sort_type_bor.filter(lambda x: x[0] == '2016').map(lambda x: ((x[1], x[2]), 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])
    data_2017 = sort_type_bor.filter(lambda x: x[0] == '2017').map(lambda x: ((x[1], x[2]), 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])
    data_2018 = sort_type_bor.filter(lambda x: x[0] == '2018').map(lambda x: ((x[1], x[2]), 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: -x[1])
    print(data_2010.take(5))
    return data_2010,data_2011,data_2012,data_2013,data_2014,data_2015,data_2016,data_2017,data_2018


sc = SparkContext()
data_2010,data_2011,data_2012,data_2013,data_2014,data_2015,data_2016,data_2017,data_2018 = profile_single_file(sc, "/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz")


def neigh_analysis(type_neigh):
    result = []
    for zipcode in range(10000, 10293):
        list_ = [str(zipcode)]
        type = type_neigh.filter(lambda x: x[0][0] == str(zipcode)).map(lambda x: (x[0][1], x[1])).sortBy(lambda x: -x[1])
        type = type.take(3)
        for i in range(len(type)):
            if type[i]!='':
                list_.append(list(type[i]))
        result.append(list_)
    print(result[0:5])
    title = ["neighbor", "1", '2', '3']
    df = pd.DataFrame(columns=title, data=result)
    # print(df)
    return df

result_2010 = neigh_analysis(data_2010)
result_2010.to_csv("2010_zip_result.csv",encoding = 'gbk')
result_2011 = neigh_analysis(data_2011)
result_2011.to_csv("2011_zip_result.csv",encoding = 'gbk')
result_2012 = neigh_analysis(data_2012)
result_2012.to_csv("2012_zip_result.csv",encoding = 'gbk')
result_2013 = neigh_analysis(data_2013)
result_2013.to_csv("2013_zip_result.csv",encoding = 'gbk')
result_2014 = neigh_analysis(data_2014)
result_2014.to_csv("2014_zip_result.csv",encoding = 'gbk')
result_2015 = neigh_analysis(data_2015)
result_2015.to_csv("2015_zip_result.csv",encoding = 'gbk')
result_2016 = neigh_analysis(data_2016)
result_2016.to_csv("2016_zip_result.csv",encoding = 'gbk')
result_2017 = neigh_analysis(data_2017)
result_2017.to_csv("2017_zip_result.csv",encoding = 'gbk')
result_2018 = neigh_analysis(data_2018)
result_2018.to_csv("2018_zip_result.csv",encoding = 'gbk')
