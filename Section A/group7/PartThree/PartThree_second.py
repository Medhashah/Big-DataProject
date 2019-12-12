import sys
import numpy as np
import json
import re
from pyspark import SparkContext
import pandas as pd

zip = list(range(10001, 10293))

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
    name = ["Complaint Type", "Incident Zip"]
    type_index = header_list.index(name[0])
    # neightborhood column will change here
    neigh_index = header_list.index(name[1])
    print(type_index)
    print(neigh_index)
    type_neigh = lines_without_header.map(lambda x: ((x[type_index], x[neigh_index]), 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])
    print(type_neigh.take(5))
    neigh_list = type_neigh.map(lambda x: x[0][1]).collect()
    return type_neigh, neigh_list


def neigh_analysis(type_neigh, neigh_list):
    result = []
    for zipcode in range(10000,10293):
        list_ = [str(zipcode)]
        type = type_neigh.filter(lambda x: x[0][1] == str(zipcode)).map(lambda x: (x[0][0], x[1])).sortBy(lambda x: -x[1])
        type = type.take(3)
        for i in range(len(type)):
            if type[i]!='':
                list_.append(list(type[i]))
        result.append(list_)
        #print(list_)
    
    print(result[0:3])
    title = ["neighbor", "1",'2','3']
    df = pd.DataFrame(columns=title, data=result)
    #print(df)
    return df




#data from 2004



sc = SparkContext()

type_neigh, neigh_list = profile_single_file(sc,"/user/hm74/NYCOpenData/sqcr-6mww.tsv.gz")
neigh_result_2004 = neigh_analysis(type_neigh, neigh_list)
#neigh_result_2004.to_csv("2004_result_question_two.csv",encoding = 'gbk')
neigh_result_2004.to_csv('2004_zip_result.csv', encoding = 'gbk')


type_neigh, neigh_list = profile_single_file(sc, "/user/hm74/NYCOpenData/sxmw-f24h.tsv.gz")
neigh_result_2005 = neigh_analysis(type_neigh, neigh_list)
#neigh_result_2005.to_csv("2005_result.csv",encoding = 'gbk')
neigh_result_2005.to_csv('2005_zip_result.csv', encoding = 'gbk')


type_neigh, neigh_list = profile_single_file(sc, "/user/hm74/NYCOpenData/hy4q-igkk.tsv.gz")
neigh_result_2006 = neigh_analysis(type_neigh, neigh_list)
#neigh_result_2006.to_csv("2006_result.csv",encoding = 'gbk')
neigh_result_2006.to_csv('2006_zip_result.csv', encoding = 'gbk')


type_neigh, neigh_list = profile_single_file(sc, "/user/hm74/NYCOpenData/aiww-p3af.tsv.gz")
neigh_result_2007 = neigh_analysis(type_neigh, neigh_list)
neigh_result_2007.to_csv("2007_zip_result.csv",encoding = 'gbk')

type_neigh, neigh_list = profile_single_file(sc, "/user/hm74/NYCOpenData/uzcy-9puk.tsv.gz")
neigh_result_2008 = neigh_analysis(type_neigh, neigh_list)
neigh_result_2008.to_csv("2008_zip_result.csv",encoding = 'gbk')

type_neigh, neigh_list = profile_single_file(sc, "/user/hm74/NYCOpenData/3rfa-3xsf.tsv.gz")
neigh_result_2009 = neigh_analysis(type_neigh, neigh_list)
neigh_result_2009.to_csv("2009_zip_result.csv",encoding = 'gbk')


