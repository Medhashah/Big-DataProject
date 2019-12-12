import sys
import numpy as np
import json
import re
from pyspark import SparkContext
import pandas as pd



def profile_single_file(sc, file):
    lines = sc.textFile(file, 1)

    # split all lines with \t
    lines = lines.map(lambda x: x.split('\t'))
    # get the header which is the column name
    header = lines.first()
    print("the column name are: ", header)
    header_list = list(header)
    # modify the dataset without the header row
    lines_without_header = lines.filter(lambda line: line != header)
    
    name = ["Complaint Type","Borough"]
    type_index = header_list.index(name[0])
    bor_index = header_list.index(name[1])
    print(type_index)
    print(bor_index)
    type_bor = lines_without_header.map(lambda x: ((x[bor_index],x[type_index]),1)).reduceByKey(lambda x,y: x+y)

    print(type_bor.take(5))
    return type_bor

def bor_analysis(type_bor):
    type_manhattan = type_bor.filter(lambda x: x[0][0] == 'MANHATTAN').sortBy(lambda x: -x[1])
    type_brooklyn = type_bor.filter(lambda x: x[0][0] == 'BROOKLYN').sortBy(lambda x: -x[1])
    type_bronx = type_bor.filter(lambda x: x[0][0] == 'BRONX').sortBy(lambda x: -x[1])
    type_island = type_bor.filter(lambda x: x[0][0] == 'STATEN ISLAND').sortBy(lambda x: -x[1])
    type_queens = type_bor.filter(lambda x: x[0][0] == 'QUEENS').sortBy(lambda x: -x[1])
    return type_manhattan.take(5), type_brooklyn.take(5), type_bronx.take(5), type_island.take(5), type_queens.take(5)


#data from 2004
def single_year_data(sc, file_path):
    type_bor = profile_single_file(sc, file_path)
    manhattan, brooklyn, bronx, island, queens = bor_analysis(type_bor)
    #five_frequency = [manhattan, brooklyn, bronx, island, queens]
    title = ["borough","1","2","3","4","5"]
    five_freq_manhattan = ['MANHATTAN']
    five_freq_brooklyn = ['BROOKLYN']
    five_freq_bronx = ['BRONX']
    five_freq_island = ['STATEN ISLAND']
    five_freq_queens = ['QUEENS']
    for i in range(len(manhattan)):
        five_freq_manhattan.append(manhattan[i][0][1])
        five_freq_brooklyn.append(brooklyn[i][0][1])
        five_freq_bronx.append(bronx[i][0][1])
        five_freq_island.append(island[i][0][1])
        five_freq_queens.append(queens[i][0][1])
    item = [five_freq_manhattan, five_freq_brooklyn, five_freq_bronx, five_freq_island, five_freq_queens]

    result = pd.DataFrame(columns = title, data = item)
    print(result)
    return result


sc = SparkContext()
2004_data = single_year_data(sc,"/user/hm74/NYCOpenData/sqcr-6mww.tsv.gz")
2004_data.to_csv("2004_result.csv",encoding = 'gbk')

2005_data = single_year_data(sc, "/user/hm74/NYCOpenData/sxmw-f24h.tsv.gz")
2005_data.to_csv("2005_result.csv",encoding = 'gbk')

2006_data = single_year_data(sc, "/user/hm74/NYCOpenData/hy4q-igkk.tsv.gz")
2006_data.to_csv("2006_result.csv",encoding = 'gbk')

2007_data = single_year_data(sc, "/user/hm74/NYCOpenData/aiww-p3af.tsv.gz")
2007_data.to_csv("2007_result.csv",encoding = 'gbk')

2008_data = single_year_data(sc, "/user/hm74/NYCOpenData/uzcy-9puk.tsv.gz")
2008_data.to_csv("2008_result.csv",encoding = 'gbk')

2009_data = single_year_data(sc, "/user/hm74/NYCOpenData/3rfa-3xsf.tsv.gz")
2009_data.to_csv("2009_result.csv",encoding = 'gbk')


