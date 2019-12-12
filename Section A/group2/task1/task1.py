import sys
import pyspark
import string
import json
import re
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col
import pyspark.sql.functions as F
import numpy as np
from numpy import long
from numpy import double
from pyspark.sql.functions import udf
import datetime
from dateutil.parser import parse
import difflib
import math

import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def str_type3(string, count):
    if string is None:
        return ('None', (None, count))
    try:
        if '.' not in str(string):
            return ('INTEGER (LONG)', (long(string), count))
    except:
        string
    try:
        return ('REAL', (double(string), count))
    except:
        string
    try:
        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y", "%Y%m%d", "%m%d%Y"]:
            try:
                datetime.datetime.strptime(string, fmt).date()
                return ('DATE/TIME', (str(string), count))
            except:
                continue
    except:
        string
    return ('TEXT',(str(string), count))

emptyWordList = ["", "-", "no data", "n/a", "null", "na", "unspecified"]

def checkEmpty(x):
    if not x[0]:
        return ('number_empty_cells',x[1])
    mat = str(x[0])
    if mat in emptyWordList:
        return ('number_empty_cells',x[1])
    return ('number_non_empty_cells',x[1])

exceptList = []

def outErrorList(i):
    erlst = []
    print('processing file index {} excepted'.format(i))
    if os.path.isfile("./errorList.txt"):
        with open('./errorList.txt', 'r', encoding='UTF-8') as f:
            erStr = f.readlines()
            for line in erStr:
                line = line.replace("\n","")
                erlst.append(line)
        if str(i) not in erlst:
            with open('./errorList.txt', 'a', encoding='UTF-8') as f:
                line = str(i)+"\n"
                f.write(line)
    else:
        with open("./errorList.txt", 'w') as f:
            for i in exceptList:
                line = str(i)+"\n"
                f.write(line)

if __name__ == "__main__":

    directory = "/user/hm74/NYCOpenData"
    outDir = "./task1out"

    sc = SparkContext()
    fileNames = sc.textFile(directory+"/datasets.tsv").map(lambda x: x.split('\t')[0]).collect()
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    fNum = len(fileNames)
    for i in range(0, fNum):
        try:
            name = fileNames[i]
            outputDicts = {}
            print('*'*50)
            print('current step {}/{}'.format(i+1, fNum))
            outputDicts["dataset_name"] = name
            filePath = directory + "/" + name +".tsv.gz"
            fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t', multiLine = True).load(filePath).cache()
            print('creating dataframe for ' + name)
            # add to output json
            outputDicts["columns"] = []
            colCnt = 0
            colNum = len(fileDF.columns)
            print('finished creating dataframe')
            for c in fileDF.columns:
                colCnt += 1
                print('current step {}/{} col: {}/{}'.format(i+1, fNum, colCnt, colNum))
                pdict = {
                    "column_name": c
                }
                #1
                print('#1 number_distinct_values')
                disRDD = fileDF.select("`"+c+"`").rdd
                rddCol = disRDD.map(lambda x: (x[c], 1))
                disRDD = rddCol.reduceByKey(lambda x,y:(x+y))
                disRDD.cache()
                disCell = disRDD.count()
                pdict["number_distinct_values"] = int(disCell)
                print('#1 finished')
                #2
                print('#2 number_non_empty_cells & number_non_empty_cells')
                NEmptyRDD = disRDD.map(lambda x: checkEmpty(x)).reduceByKey(lambda x,y:(x+y))
                NEList = NEmptyRDD.collect()
                for ne in NEList:
                    pdict[ne[0]] = str(ne[1])
                if 'number_empty_cells' not in pdict:
                    pdict['number_empty_cells'] = 0
                if 'number_non_empty_cells' not in pdict:
                    pdict['number_non_empty_cells'] = 0
                print('#2 finished')
                #4
                print('#3 frequent_values')
                topRDD = disRDD.sortBy(lambda x: -x[1]).take(5)
                topList = []
                for index in range(len(topRDD)):
                    topList.append(str(topRDD[index][0]))
                pdict["frequent_values"] = topList
                print('#3 finished')
                #5
                print('#4 data types')

                new_disRDD = disRDD.map(lambda x: str_type3(x[0], x[1]))
                new_disRDD.cache()
                columnOfInteger = new_disRDD.filter(lambda x: x[0] == 'INTEGER (LONG)')
                columnOfInteger.cache()
                columnOfReal = new_disRDD.filter(lambda x: x[0] == 'REAL')
                columnOfReal.cache()
                columnOfDate = new_disRDD.filter(lambda x: x[0] == 'DATE/TIME')
                columnOfDate.cache()
                columnOfText = new_disRDD.filter(lambda x: x[0] == 'TEXT')
                columnOfText.cache()
                numOfInteger = columnOfInteger.count()
                numOfReal = columnOfReal.count()
                numOfDate = columnOfDate.count()
                numOfText = columnOfText.count()
                data_types = []
                if numOfInteger > 0:
                    integerType = {}
                    integerType['type'] = 'INTEGER (LONG)'
                    integerType['count'] = columnOfInteger.map(lambda x: x[1][1]).sum()
                    columnOfIntegerValue = columnOfInteger.map(lambda x: x[1][0])
                    integerType['max_value'] = columnOfIntegerValue.max()
                    integerType['min_value'] = columnOfIntegerValue.min()
                    sumOfTwo = (lambda x, y: (x[0] + y[0] * y[1], x[1] + y[1]))
                    countOfsum = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
                    columnOfIntegerValueAndCount = columnOfInteger.map(lambda x: x[1])
                    sumAndCount = columnOfIntegerValueAndCount.aggregate((0, 0), sumOfTwo, countOfsum)
                    integerType['mean'] = float(sumAndCount[0] / sumAndCount[1])
                    sumOfTwoSquare = (lambda x, y: (
                    x[0] + (y[0] - integerType['mean']) * (y[0] - integerType['mean']) * y[1], x[1] + y[1]))
                    countOfSumSquare = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
                    squareAndCount = columnOfIntegerValueAndCount.aggregate((0, 0), sumOfTwoSquare, countOfSumSquare)
                    integerType['stddev'] = math.sqrt(squareAndCount[0] / squareAndCount[1])
                    data_types.append(integerType)
                if numOfReal > 0:
                    realType = {}
                    realType['type'] = 'REAL'
                    realType['count'] = columnOfReal.map(lambda x: x[1][1]).sum()
                    columnOfRealValue = columnOfReal.map(lambda x: x[1][0])
                    realType['max_value'] = columnOfRealValue.max()
                    realType['min_value'] = columnOfRealValue.min()
                    sumOfTwo = (lambda x, y: (x[0] + y[0] * y[1], x[1] + 1))
                    countOfsum = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
                    columnOfRealValueAndCount = columnOfReal.map(lambda x: x[1])
                    sumAndCount = columnOfRealValueAndCount.aggregate((0, 0), sumOfTwo, countOfsum)
                    realType['mean'] = float(sumAndCount[0] / sumAndCount[1])
                    sumOfTwoSquare = (lambda x, y: (x[0] + (y[0] - realType['mean']) ** 2 * y[1], x[1] + 1))
                    countOfSumSquare = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
                    squareAndCount = columnOfRealValueAndCount.aggregate((0, 0), sumOfTwoSquare, countOfSumSquare)
                    realType['stddev'] = math.sqrt(squareAndCount[0] / squareAndCount[1])
                    data_types.append(realType)
                if numOfDate > 0:
                    dateType = {}
                    dateType['type'] = 'DATE/TIME'
                    dateType['count'] = columnOfDate.map(lambda x: x[1][1]).sum()
                    columnOfDateValue = columnOfDate.map(lambda x: x[1][0])
                    dateType['max_value'] = columnOfDateValue.max()
                    dateType['min_value'] = columnOfDateValue.min()
                    data_types.append(dateType)
                if numOfText > 0:
                    textType = {}
                    textType['type'] = 'TEXT'
                    textType['count'] = columnOfText.map(lambda x: x[1][1]).sum()
                    columnOfTextValue = columnOfText.map(lambda x: x[1]).flatMap(lambda x: [x[0] for i in range(x[1])])
                    textType['shortest_values'] = columnOfTextValue.sortBy(lambda x: len(x), True).take(5)
                    textType['longest_values'] = columnOfTextValue.sortBy(lambda x: len(x), False).take(5)
                    textType['average_length'] = columnOfTextValue.map(lambda x: len(x)).mean()
                    data_types.append(textType)

                pdict['data_types'] = data_types
                print('#4 finished')    
                #add to out dicts
                outputDicts["columns"].append(pdict)

                #uncache
                disRDD.unpersist()
                new_disRDD.unpersist()
                columnOfInteger.unpersist()
                columnOfReal.unpersist()
                columnOfDate.unpersist()
                columnOfText.unpersist()

            with open(outDir+"/"+name+"_generic.json", 'w') as fw:
                json.dump(outputDicts,fw)
            print('Finished output file: {}, the index is: {}'.format(name, i))
        except Exception as e:
            exceptList.append(i)
            outErrorList(i)