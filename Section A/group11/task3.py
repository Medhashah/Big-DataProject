import sys
from pyspark.sql import SparkSession
from csv import reader
from pyspark import SparkContext
import gzip
from pyspark.sql import *
from pyspark.sql.functions import *
import os
from os import listdir
from os.path import isfile, join
import math
import re
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from pylab import *
import numpy as np

sc = SparkContext()
spark = SparkSession.builder.appName("finak").config("spark.some.config.option", "some-value").getOrCreate()    

txt_name = "123/filename100.txt"
file_txt = open(txt_name,"r")
file_names = [line.replace("\n","") for line in file_txt.readlines()]


#method 1
#find outerlier
#My thought is using ML to find the outlier 
from gensim.models.doc2vec import TaggedDocument, Doc2Vec
from gensim.parsing.preprocessing import preprocess_string
from sklearn.base import BaseEstimator
from sklearn import utils as skl_utils
from tqdm import tqdm

import multiprocessing
import numpy as np


txt_name = "filenames.txt"
file_txt = open(txt_name,"r")
file_names = [line.replace("\n","") for line in file_txt.readlines()]


txt_name = "labelFile.txt"
file_txt = open(txt_name,"r")
labelFile = [line.replace("\n","") for line in file_txt.readlines()]

finalRdd= sc.emptyRDD 
for k in range(len(file_names)):
    try:
  		file= file_names[k]
  		labelColumn= labelFile[k].split(" ")
        df= spark.read.format('csv').options(header='true',inferschema='true', delimiter='\t').load(file)
        df.createOrReplaceTempView("df")
        rdd= df.rdd.map(list)
        for i in range(len(columns)):
        	labelType= labelColumn[i]
            col= columns[i]
            rSingleColumn= rdd.map(lambda x: x[i])
            columnLabel= rSingleColumn.map(lambda x: (labelType, x)).groupByKey()
            finalRdd= finalRdd.union(columnLabel)
    except Exception as e: 
        print(e) 

finalRdd= finalRdd.groupByKey()
finalRdd= finalRdd.map(lambda x: (x[1]))

val wordDataFrame = rdd.toDF("words")
val word2Vec = new Word2Vec()
  .setInputCol("words")
  .setOutputCol("result")
  .setVectorSize(3)
  .setMinCount(0)

model = word2Vec.fit(wordDataFrame)

train= {}
X= []
#get word->vector
dfWordVector= model.getVectors().rdd.map(list)
for key, value in dfWordVector:
	train[value]= key
	X.append(key)

import numpy as np
from sklearn.neighbors import LocalOutlierFactor


outer= []
# fit the model
clf = LocalOutlierFactor(n_neighbors=20)
y_pred = clf.fit_predict(X)
for i in range(len(y_pred)):
	if y_pred[i]== -1:
		outer.append(X[i])

outlier= []
for val in outer:
	v= X[val]
	outlier.append(v)

with open('outlier.txt', 'w') as fp:
	fp.write(outlier)

