import sys
import numpy as np
import pandas as pd
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
import csv
from csv import reader
import re
from pyspark.sql import SQLContext
import json

def get_file_column_name():
	file_ = open('cluster3.txt')
	line = file_.readline()
	file_with_column = line.split(",")
	file_list = []
	column_list = []
	for file_name in file_with_column:
		file_name = eval(file_name)
		file_list.append(file_name[0:10]+'tsv.gz')
		column_list.append(file_name[10:-7])
	#print(file_list)
	#print(column_list)
	return file_list,column_list


def read_into_RDD(sc, file_name):
	#sqlContext = SQLContext(sc)
	#5694-9szk.Business_Website_or_Other_URL.txt.gz for test

	file_path = "/user/hm74/NYCColumns/"+str(file_name)
	lines = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
	data_collect = lines.map(lambda x: (x[0],x[1]))
	return data_collect.take(10)

def write_into_df(file_list,item):
	dataset = pd.DataFrame({'file': file_list, 'freq_item':item})
	#print(dataset)
	dataset.to_csv('true_type.csv', encoding='gbk')





item = []
sc = SparkContext()
file_list = open('cluster3.txt').readline().strip().replace(' ', '').split(",")

for i in range(274):
	column = read_into_RDD(sc, file_list[i])
	item.append(column)
write_into_df(file_list, item)

