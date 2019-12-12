import os
import os.path
from os import path
import pyspark
from pyspark import SparkContext
import json

from dateutil import parser

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, lower
from pyspark.sql import Row

from pyspark.sql.functions import isnan, when, count, col

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import levenshtein

from pyspark.sql import SQLContext

import re

from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, IndexToString, StringIndexer, Word2Vec
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.evaluation import MulticlassMetrics
from fuzzywuzzy import fuzz
import string

spark = SparkSession \
		.builder \
		.appName("Big data project") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

sqlContext=SQLContext(spark.sparkContext, sparkSession=spark, jsqlContext=None)

def getData(file):
    return spark.read.option("delimiter", "\\t").option("inferSchema", "true").option("header","true").csv(file, inferSchema=True)


def getDataCustom(file):
    return spark.read.option("delimiter", ",").option("inferSchema", "true").option("header","true").csv(file, inferSchema=True)


with open("/home/jys308/cluster1.txt","r") as f:
    content = f.readlines()

def max_vector(vector):
    max_val = float(max(vector))
    return max_val

max_vector_udf = udf(max_vector, FloatType())

files = content[0].strip("[]").replace("'","").replace(" ","").split(",")

web_regex = r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"
zip_regex = r"11422 11422-7903 11598 11678787 11678-23 11723 11898-111 22222222-6666 14567-999999 11111-2222"
latlong_regex = r'([0-9.-]+).+?([0-9.-]+)'
phone_regex = r'((\(\d{3}\) ?)|(\d{3}-))?\d{3}-\d{4}'
address_regex = r'^\d+\s[A-z]+\s[A-z]+'

########################################
# Main semantic type function
########################################

def REGEX(df):
    print("computing Regex for:", colName)
    #could improve the address regex
    columns = df.columns
    df = df.withColumn("true_type", when(df[columns[0]].rlike(web_regex), "website").otherwise(df["true_type"]))
    df = df.withColumn("true_type", when(df[columns[0]].rlike(zip_regex), "zip_code").otherwise(df["true_type"]))
    df = df.withColumn("true_type", when(df[columns[0]].rlike(latlong_regex), "lat_lon_cord").otherwise(df["true_type"]))
    df = df.withColumn("true_type", when(df[columns[0]].rlike(phone_regex), "phone_number").otherwise(df["true_type"]))
    df = df.withColumn("true_type", when(df[columns[0]].rlike(address_regex), "address").otherwise(df["true_type"]))
    return df

def NAME(df):
    print("computing names for:", colName)
    columns = df.columns
    #take column one and make predictions
    df = df.select(col(columns[0]).alias("TEXT"), col(columns[1]).alias("frequency"), col("true_type"))
    pred = model.transform(df.select('TEXT'))
    pred_categories = pred.select('TEXT', 'originalcategory', 'probability')
    new_df = df.join(pred_categories, on=['TEXT'], how='left_outer')
    # create new DF, filter only for high probability.
    new_df = new_df.withColumn("max_probability", max_vector_udf(new_df["probability"]))
    #new_df.select("probability").map(lambda x: x.toArray().max())
    new_df = new_df.withColumn("true_type", when( (new_df["max_probability"] >= 0.90) & \
            (new_df["true_type"].isNull()), new_df["originalcategory"]).otherwise(new_df["true_type"]))
    rdf = new_df.drop("originalcategory").drop("probability").drop("max_probability")
    return rdf

def leven_helper(df, ref_df, cut_off, type_str):
    #print("size of reference_df",ref_df.count())
    df_columns = df.columns
    # grab the non typed entries in the input df
    new_df = df.filter(df["true_type"].isNull())
    #crossjoin null values with reference columns
    ref_columns = ref_df.columns
    levy_df = new_df.crossJoin(ref_df)
    #compute levy distance
    levy_df = levy_df.withColumn("word1_word2_levenshtein",\
        levenshtein(lower(col(df_columns[0])), lower(col(ref_columns[0]))))
    #collect rows that were less than cutoff
    levy_df =  levy_df.filter(levy_df["word1_word2_levenshtein"] <= cut_off)
    levy_columns = levy_df.columns
    levy_df = levy_df.groupBy(levy_columns[0]).min("word1_word2_levenshtein")
    levy_columns = levy_df.columns
    levy_df = levy_df.select(col(levy_columns[0]), \
        col(levy_columns[1]).alias("min"))
    levy_columns = levy_df.columns
    levy_df = levy_df.drop("min")
    df.withColumn("true_type", when(col(df_columns[0]).isin(levy_df[levy_columns[0]]), "neighborhood").otherwise(df["true_type"]))
    levy_df = levy_df.collect()
    levy_df = [x[0] for x in levy_df]
    rdf = df.withColumn("true_type", when(df[df_columns[0]].isin(levy_df), "neighborhood").otherwise(df["true_type"]))
    return rdf


def LEVEN(df):
    print("Computing Levenshtein for:", colName)
    df1 = leven_helper(df, cities_df, 3, "city")
    df2 = leven_helper(df1, neighborhood_df, 4, "neighborhood")
    df3 = leven_helper(df2, borough_df, 0, "borough")
    df4 = leven_helper(df3, schoolname_df, 5, "school_name")
    df5 = leven_helper(df4, color_df, 2, "color")
    df6 = leven_helper(df5, carmake_df, 2, "car_make")
    df7 = leven_helper(df6, cityagency_df, 3, "city_agency")
    df8 = leven_helper(df7, areastudy_df, 5, "area_of_study")
    df9 = leven_helper(df8, subjects_df, 2, "subject_in_school")
    df10 = leven_helper(df9, schoollevels_df, 1, "school_level")
    df11 = leven_helper(df10, college_df, 3, "college_name")
    df12 = leven_helper(df11, vehicletype_df, 3, "vehicle_type")
    df13 = leven_helper(df12, typelocation_df, 5, "location_type")
    df14 = leven_helper(df13, parks_df, 5, "park_playground")
    df15 = leven_helper(df14, building_code_df, 0, "building_classification")
    return df15


def semanticType(colName, df):
    df = REGEX(df)
    df = LEVEN(df)
    df = NAME(df)
    return df

#################################
# Train Classifier
#################################


if path.exists("weights"):
    print("Found weights, loading them...")
    model = PipelineModel.load("weights")

else:
    print("Training Classifier...")

    address_df = getDataCustom('/user/jys308/Address_Point.csv')
    permit_df = getDataCustom('/user/jys308/Approved_Permits.csv')
    address_df.createOrReplaceTempView("address_df")
    permit_df.createOrReplaceTempView("permit_df")

    #we could add more data, but this is what we had time for

    full_st_name = spark.sql("SELECT DISTINCT FULL_STREE as TEXT FROM address_df")
    st_name = spark.sql("SELECT DISTINCT ST_NAME as TEXT FROM address_df")
    first_name = spark.sql("SELECT `Applicant First Name` as TEXT from permit_df")
    last_name = spark.sql("SELECT `Applicant Last Name` as TEXT from permit_df")
    business_name = spark.sql("SELECT DISTINCT `Applicant Business Name` as TEXT FROM permit_df")

    st_name = st_name.withColumn('category', lit('street_name'))
    full_st_name = full_st_name.withColumn('category', lit('street_name'))
    first_name = first_name.withColumn('category', lit('person_name'))
    last_name = last_name.withColumn('category', lit('person_name'))
    business_name = business_name.withColumn('category', lit('business_name'))

    train1 = business_name.union(st_name)
    train2 = first_name.union(last_name)
    train3 = train1.union(train2)
    fullData =  train3.union(full_st_name)
    fullData = fullData.dropna()

    #(trainingData, testData) = fullData.randomSplit([0.9, 0.1])

    #trainingData = trainingData.dropna()
    #testData = testData.dropna()

    indexer = StringIndexer(inputCol="category", outputCol="label").fit(fullData)
    tokenizer = RegexTokenizer(pattern=u'\W+', inputCol="TEXT", outputCol="words", toLowercase=False)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(maxIter=20, regParam=0.001)
    labelConverter = IndexToString(inputCol="prediction", outputCol="originalcategory", labels=indexer.labels)

    pipeline = Pipeline(stages=[indexer, tokenizer, hashingTF, idf, lr, labelConverter])
    model = pipeline.fit(fullData)

    print("Done training classifier")
    model.save("weights")
    #model.save("/home/jys308/weights")

    #pred = model.transform(testData)
    #pl = pred.select("label", "prediction").rdd.cache()
    #metrics = MulticlassMetrics(pl)
    #metrics.fMeasure()

#######################################
# Gathering Data for Levenshtein Distance checking
#######################################

print("Loading text files...")

###
# Cities
###
cities_file = open("/home/jys308/cities.txt")
cities_list = []

for line in cities_file:
    #line = line.split()
    cities_list.append(line[:-1])

cities_df = spark.createDataFrame(list(map(lambda x: Row(cities=x), cities_list)))

###
# Neighborhood
###

neighborhood_file = open("/home/jys308/neighborhood.txt")
neighborhood_list = []

for line in neighborhood_file:
    #line = line.split()
    neighborhood_list.append(line[:-1])

neighborhood_df = spark.createDataFrame(list(map(lambda x: Row(neighborhood=x), neighborhood_list)))

###
# Borough
###

borough_file = open("/home/jys308/boroughs.txt")
borough_list = []

for line in borough_file:
    #line = line.split()
    borough_list.append(line[:-1])

borough_df = spark.createDataFrame(list(map(lambda x: Row(borough=x), borough_list)))

###
# Schoolname
###

schoolname_file = open("/home/jys308/schoolname.txt")
schoolname_list = []

for line in schoolname_file:
    #line = line.split()
    schoolname_list.append(line[:-1])

schoolname_df = spark.createDataFrame(list(map(lambda x: Row(schoolname=x), schoolname_list)))

###
# Color
###

color_file = open("/home/jys308/colors.txt")
color_list = []

for line in color_file:
    #line = line.split()
    color_list.append(line[:-1])

color_df = spark.createDataFrame(list(map(lambda x: Row(color=x), color_list)))

###
# Carmake
###

carmake_file = open("/home/jys308/carmake.txt")
carmake_list = []

for line in carmake_file:
    #line = line.split()
    carmake_list.append(line[:-1])

carmake_df = spark.createDataFrame(list(map(lambda x: Row(carmake=x), carmake_list)))

###
# City Agency
###

cityagency_file = open("/home/jys308/cityagency.txt")
cityagency_list = []

for line in cityagency_file:
    #line = line.split()
    cityagency_list.append(line[:-1])

cityagency_df = spark.createDataFrame(list(map(lambda x: Row(cityagency=x), cityagency_list)))

###
# Area Study
###

areastudy_file = open("/home/jys308/areastudy.txt")
areastudy_list = []

for line in areastudy_file:
    #line = line.split()
    areastudy_list.append(line[:-1])

areastudy_df = spark.createDataFrame(list(map(lambda x: Row(areastudy=x), areastudy_list)))

###
# Subjects
###

subjects_file = open("/home/jys308/subjects.txt")
subjects_list = []

for line in subjects_file:
    #line = line.split()
    subjects_list.append(line[:-1])

subjects_df = spark.createDataFrame(list(map(lambda x: Row(subjects=x), subjects_list)))

###
# School Levels
###

schoollevels_file = open("/home/jys308/schoollevels.txt")
schoollevels_list = []

for line in schoollevels_file:
    #line = line.split()
    schoollevels_list.append(line[:-1])

schoollevels_df = spark.createDataFrame(list(map(lambda x: Row(schoollevels=x), schoollevels_list)))

###
# College
###

college_file = open("/home/jys308/college.txt", encoding="utf-8")
college_list = []

for line in college_file:
    #line = line.split(',')
    #if line[0] == 'US':
    college_list.append(line[:-1])

college_df = spark.createDataFrame(list(map(lambda x: Row(college=x), college_list)))

###
# Vehicle Type
###

vehicletype_file = open("/home/jys308/vehicletype.txt")
vehicletype_list = []

for line in vehicletype_file:
    #line = line.split()
    vehicletype_list.append(line[:-1])

vehicletype_df = spark.createDataFrame(list(map(lambda x: Row(vehicletype=x), vehicletype_list)))

###
# Type Location
###
typelocation_file = open("/home/jys308/typelocation.txt")
typelocation_list = []

for line in typelocation_file:
    line = line[3:-1]
    typelocation_list.append(line)

typelocation_df = spark.createDataFrame(list(map(lambda x: Row(typelocation=x), typelocation_list)))

###
# Parks
###

parks_file = open("/home/jys308/parks.txt")
parks_list = []

for line in parks_file:
    #line = line.split()
    parks_list.append(line[:-1])

parks_df = spark.createDataFrame(list(map(lambda x: Row(parks=x), parks_list)))

###
# Building Codes
###

building_codes_file = open("/home/jys308/building_codes.txt")
building_codes_list = []

for line in building_codes_file:
    line = line.split()
    building_codes_list.append(line[0])

building_code_df = spark.createDataFrame(list(map(lambda x: Row(building_codes=x), building_codes_list)))

####

print("Done Loading Text Files")

#######################################
# Iterate through all columns
#######################################

length = [5308, 10720, 1925, 480, 4, 2, 8, 21520, 788, 6553, 192, 4, 8532, 7062, 12126, 47, 6242, 1693, 6983, 1848, 4, 1797, 19, 71, 4, 2508, 1500, 3111, 2488, 1236, 723, 332, 5965, 4, 17, 7342, 4008, 74, 1543, 1564, 2013, 23730, 2146, 381, 5701, 3187, 6511, 44, 11641, 4, 14899, 5146, 31270, 11, 43, 23, 3, 1608, 1603, 700, 4, 4, 7, 4, 1727, 7, 20958, 52535, 1740, 11, 8, 323, 15045, 21, 11, 1734, 45, 5, 471, 116, 14361, 862, 37, 4103, 1603, 5701, 8, 21, 3, 6, 19, 3, 65, 4, 2364, 5933, 7, 3538, 26, 20, 975, 3976, 21, 432, 1638, 2872, 20007, 689, 9638, 950, 10, 44, 196, 4, 19, 1478, 107, 1701, 32, 38, 152, 2372, 1821, 1200, 20941, 6344, 8771, 4845, 73, 13, 6322, 2895, 558170, 17252, 18032, 479, 4, 846, 8, 4, 20473, 7, 68, 118, 4, 36, 319, 23, 22640, 18636, 38, 4, 4, 1249, 3672, 1518, 4, 10597, 14, 2165, 4, 1755, 27, 6602, 6872, 723, 943, 45, 510, 27, 4292, 1734, 1419, 122763, 7731, 8060, 30, 8657, 15841, 9837, 4150, 6, 5383, 1162, 3744, 190, 2284, 14667, 2532, 2597, 28, 1739, 1733, 43, 1060, 260972, 6031, 384841, 46, 1105, 5, 14035, 1452, 944, 7, 6983, 21, 6, 1630, 1867, 1296, 3, 85, 7921, 13632, 4, 358, 4, 653, 4, 4, 482235, 7935, 1648, 1548, 41441, 1644, 4, 208, 17, 3192, 12177, 11, 2, 1654, 2, 241, 14, 4, 11645, 5, 12082, 2, 35, 375499, 2496, 52, 13063, 3412, 1074, 2369, 1041, 4668, 48, 360, 190, 4, 62, 19, 4, 27, 1848, 6031, 23, 6345, 57, 2577, 1569, 2679, 4892, 1565, 61]

files_and_length = []

for (i,j) in zip(files, length):
    files_and_length.append((i,j))

files_and_length.sort(key = lambda x: x[1])

#index 5 is school levels
#index 25 is subjects
#index 55 is neighborhoods

#index = 55

for file in files_and_length:
    print("This is the index of sorted list", files_and_length.index(file))
    file = file[0]
    fileData = file.split(".")
    fileName = fileData[0]
    colName = fileData[1]
    df = spark.read.option("delimiter", "\\t").option("header","true") \
        .option("inferSchema","true").csv("/user/hm74/NYCColumns/" + file)
    colNamesList = [i.replace(".","").replace(" ","_") for i in df.columns]
    df = df.toDF(*colNamesList) #change name in DF
    #colName = colNamesList[0] #change colName we have
    colName = colName.replace(".","").replace(" ", "_")
    df = df.withColumn('true_type', lit(None))
    df = semanticType(colName, df)
    df_columns = df.columns
    list_row_type = df.groupBy("true_type").sum().collect()
    list_row_type = [[i[0],i[1]] for i in list_row_type]
    predicted_types = {"predicted_types": [x[0] for x in list_row_type]}
    this_column = {"column_name": colName, "semantic_types": [{"semantic_type": x[0], "label": "string", "count": x[1]} for x in list_row_type]}
    master_list = [predicted_types, this_column]
    #print("Working on", colName)
    #print("This is column number", files.index(file))
    #process dictionary to record to json
    with open(str(file) +'.json', 'w') as fp:
        json.dump(master_list, fp)


#largest file index is 132