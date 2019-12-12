import os
import os.path
from os import path
import pyspark
from pyspark import SparkContext
import json

from dateutil import parser

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql import Row

from pyspark.sql.functions import isnan, when, count, col

from pyspark.ml.linalg import Vectors
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

files = content[0].strip("[]").replace("'","").replace(" ","").split(",")

########################################
# Main semantic type function
########################################

def semanticType(colName, df):
    """
    The semantic types:
    1) Person name (Last name, First name, Middle name, Full name) NAME (DONE)
    2) Business name NAME (DONE)
    3) Phone Number REGEX (DONE)
    4) Address REGEX (DONE)
    5) Street name NAME (DONE)
    6) City LEVEN
    7) Neighborhood LEVEN
    8) LAT/LON coordinates REGEX (DONE)
    9) Zip code REGEX (DONE)
    10) Borough LEVEN
    11) School name (Abbreviations and full names) LEVEN
    12) Color LEVEN
    13) Car make LEVEN
    14) City agency (Abbreviations and full names) LEVEN
    15) Areas of study (e.g., Architecture, Animal Science, Communications) LEVEN
    16) Subjects in school (e.g., MATH A, MATH B, US HISTORY) LEVEN
    17) School Levels (K-2, ELEMENTARY, ELEMENTARY SCHOOL, MIDDLE) LEVEN
    18) College/University names LEVEN
    19) Websites (e.g., ASESCHOLARS.ORG) REGEX (DONE)
    20) Building Classification (e.g., R0-CONDOMINIUM, R2-WALK-UP) LEVEN
    21) Vehicle Type (e.g., AMBULANCE, VAN, TAXI, BUS) LEVEN
    22) Type of location (e.g., ABANDONED BUILDING, AIRPORT TERMINAL, BANK, CHURCH, CLOTHING/BOUTIQUE) LEVEN
    23) Parks/Playgrounds (e.g., CLOVE LAKES PARK, GREENE PLAYGROUND) LEVEN

    We will check the column name and it's levenshtein distance with the list of semantic types. We will call the    function for that semantic type.

    input: df with 2 columns. 1st column is the data and 2nd is the count
    output: dictionary with keys as semantic types and values as count
    """

    types = {}



    def REGEX(df):
        print("computing Regex for:", colName)
        types = {}
        ########################
        # There are five types that we will find with regex
        ########################
        web_regex = r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"

        zip_regex = r"11422 11422-7903 11598 11678787 11678-23 11723 11898-111 22222222-6666 14567-999999 11111-2222"

        latlong_regex = r'([0-9.-]+).+?([0-9.-]+)'

        phone_regex = r'((\(\d{3}\) ?)|(\d{3}-))?\d{3}-\d{4}'

        #could improve the address regex

        address_regex = r'^\d+\s[A-z]+\s[A-z]+'

        columns = df.columns

        df_web = df.filter(df[columns[0]].rlike(web_regex))
        df_zip = df.filter(df[columns[0]].rlike(zip_regex))
        df_latlong =  df.filter(df[columns[0]].rlike(latlong_regex))
        df_phone =  df.filter(df[columns[0]].rlike(phone_regex))
        df_address =  df.filter(df[columns[0]].rlike(address_regex))

	#Get rows from df_web, which will be webaddress, and sum the second column
	#to get the semantic type WEBSITE.
        #only sum and add type if it exists

        if len(df_web.take(1)) > 0:
	    #not sure which is faster
            web_frequency = df_web.groupBy().sum().collect()[0][0]
            types['web'] = web_frequency
            #web_frequency = df_web.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]


        if len(df_zip.take(1)) > 0:
            zip_frequency = df_zip.groupBy().sum().collect()[0][0]
            types['zip'] = zip_frequency

        if len(df_latlong.take(1)) > 0:
            latlong_frequency = df_latlong.groupBy().sum().collect()[0][0]
            types['latlong'] = latlong_frequency

        if len(df_phone.take(1)) > 0:
            phone_frequency = df_phone.groupBy().sum().collect()[0][0]
            types['phone'] = phone_frequency

        if len(df_address.take(1)) > 0:
            address_frequency = df_address.groupBy().sum().collect()[0][0]
            types['address'] = address_frequency


        return types

    def NAME(df):
        print("computing names for:", colName)
        types = {}
        columns = df.columns
        #take column one and make predictions
        df = df.select(col(columns[0]).alias("TEXT"), col(columns[1]).alias("frequency"))
        pred = model.transform(df.select('TEXT'))
        pred_categories = pred.select('TEXT', 'originalcategory')
        new_df = df.join(pred_categories, on=['TEXT'], how='left_outer')

        street_name_df = new_df.filter(new_df['originalcategory'] == 'STREETNAME')
        human_df = new_df.filter(new_df['originalcategory'] == 'HUMANNAME')
        business_df = new_df.filter(new_df['originalcategory'] == 'BUSINESSNAME')

        if len(street_name_df.take(1)) > 0:
            stname_frequency = street_name_df.groupBy().sum().collect()[0][0]
            types['stname'] = stname_frequency

        if len(human_df.take(1)) > 0:
            human_frequency = human_df.groupBy().sum().collect()[0][0]
            types['humanname'] = human_frequency

        if len(business_df.take(1)) > 0:
            business_frequency = business_df.groupBy().sum().collect()[0][0]
            types['businessname'] = business_frequency

        return types

    def LEVEN(df):
        print("Computing Levenshtein for:", colName)
        types = {}
        df_columns = df.columns
        ###############
        # Cities
        ###############
        cities_columns = cities_df.columns
        cities_crossjoin = df.crossJoin(cities_df)
        cities_levy = cities_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('cities')))
        cities_count =  cities_levy.filter(cities_levy["word1_word2_levenshtein"] <= 2)
        if len(cities_count.take(1)) > 0:
            cities_frequency = cities_count.groupBy().sum().collect()[0][0]
            types['cities'] = cities_frequency

        ###############
        # Neighborhoods
        ###############
        neighborhood_columns = neighborhood_df.columns
        neighborhood_crossjoin = df.crossJoin(neighborhood_df)
        neighborhood_levy = neighborhood_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('neighborhood')))
        neighborhood_count =  neighborhood_levy.filter(neighborhood_levy["word1_word2_levenshtein"] <= 2)
        if len(neighborhood_count.take(1)) > 0:
            neighborhood_frequency = neighborhood_count.groupBy().sum().collect()[0][0]
            types['neighborhood'] = neighborhood_frequency

        ###############
        # Borough
        ###############
        borough_columns = borough_df.columns
        borough_crossjoin = df.crossJoin(borough_df)
        borough_levy = borough_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('borough')))
        borough_count =  borough_levy.filter(borough_levy["word1_word2_levenshtein"] <= 2)
        if len(borough_count.take(1)) > 0:
            borough_frequency = borough_count.groupBy().sum().collect()[0][0]
            types['borough'] = borough_frequency

        ###############
        # School Name
        ###############
        schoolname_columns = schoolname_df.columns
        schoolname_crossjoin = df.crossJoin(schoolname_df)
        schoolname_levy = schoolname_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('schoolname')))
        schoolname_count =  schoolname_levy.filter(schoolname_levy["word1_word2_levenshtein"] <= 2)
        if len(schoolname_count.take(1)) > 0:
            schoolname_frequency = schoolname_count.groupBy().sum().collect()[0][0]
            types['schoolname'] = schoolname_frequency

        ###############
        # Color
        ###############
        color_columns = color_df.columns
        color_crossjoin = df.crossJoin(color_df)
        color_levy = color_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('color')))
        color_count =  color_levy.filter(color_levy["word1_word2_levenshtein"] <= 2)
        if len(color_count.take(1)) > 0:
            color_frequency = color_count.groupBy().sum().collect()[0][0]
            types['color'] = color_frequency

        ###############
        # Carmake
        ###############
        carmake_columns = carmake_df.columns
        carmake_crossjoin = df.crossJoin(carmake_df)
        carmake_levy = carmake_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('carmake')))
        carmake_count =  carmake_levy.filter(carmake_levy["word1_word2_levenshtein"] <= 2)
        if len(carmake_count.take(1)) > 0:
            carmake_frequency = carmake_count.groupBy().sum().collect()[0][0]
            types['carmake'] = carmake_frequency


        ###############
        # City Agency
        ###############
        cityagency_columns = cityagency_df.columns
        cityagency_crossjoin = df.crossJoin(cityagency_df)
        cityagency_levy = cityagency_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('cityagency')))
        cityagency_count =  cityagency_levy.filter(cityagency_levy["word1_word2_levenshtein"] <= 2)
        if len(cityagency_count.take(1)) > 0:
            cityagency_frequency = cityagency_count.groupBy().sum().collect()[0][0]
            types['cityagency'] = cityagency_frequency

        ##############
        # Area of Study
        ##############
        areastudy_columns = areastudy_df.columns
        areastudy_crossjoin = df.crossJoin(areastudy_df)
        areastudy_levy = areastudy_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('areastudy')))
        areastudy_count =  areastudy_levy.filter(areastudy_levy["word1_word2_levenshtein"] <= 2)
        if len(areastudy_count.take(1)) > 0:
            areastudy_frequency = areastudy_count.groupBy().sum().collect()[0][0]
            types['areastudy'] = areastudy_frequency

        ##############
        # Subjects
        ##############
        subjects_columns = subjects_df.columns
        subjects_crossjoin = df.crossJoin(subjects_df)
        subjects_levy = subjects_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('subjects')))
        subjects_count =  subjects_levy.filter(subjects_levy["word1_word2_levenshtein"] <= 2)
        if len(subjects_count.take(1)) > 0:
            subjects_frequency = subjects_count.groupBy().sum().collect()[0][0]
            types['subjects'] = subjects_frequency

        ##############
        # School Levels
        ##############
        schoollevels_columns = schoollevels_df.columns
        schoollevels_crossjoin = df.crossJoin(schoollevels_df)
        schoollevels_levy = schoollevels_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('schoollevels')))
        schoollevels_count =  schoollevels_levy.filter(schoollevels_levy["word1_word2_levenshtein"] <= 2)
        if len(schoollevels_count.take(1)) > 0:
            schoollevels_frequency = schoollevels_count.groupBy().sum().collect()[0][0]
            types['schoollevels'] = schoollevels_frequency

        ##############
        # Colleges
        ##############
        colleges_columns = college_df.columns
        college_crossjoin = df.crossJoin(college_df)
        college_levy = college_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('college')))
        college_counts = college_levy.filter(college_levy["word1_word2_levenshtein"] <= 2)
        if len(college_counts.take(1)) > 0:
            college_frequency = college_counts.groupBy().sum().collect()[0][0]
            types['college'] = college_frequency

        ##############
        # Vehicle Type
        ##############
        vehicletype_columns = vehicletype_df.columns
        vehicletype_crossjoin = df.crossJoin(vehicletype_df)
        vehicletype_levy = vehicletype_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('vehicletype')))
        vehicletype_counts = vehicletype_levy.filter(vehicletype_levy["word1_word2_levenshtein"] <= 2)
        if len(vehicletype_counts.take(1)) > 0:
            vehicletype_frequency = vehicletype_counts.groupBy().sum().collect()[0][0]
            types['vehicletype'] = vehicletype_frequency

        ##############
        # Type of Location
        ##############
        typelocation_columns = typelocation_df.columns
        typelocation_crossjoin = df.crossJoin(typelocation_df)
        typelocation_levy = typelocation_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('typelocation')))
        typelocation_counts = typelocation_levy.filter(typelocation_levy["word1_word2_levenshtein"] <= 2)
        if len(typelocation_counts.take(1)) > 0:
            typelocation_frequency = typelocation_counts.groupBy().sum().collect()[0][0]
            types['typelocation'] = typelocation_frequency

        ##############
        # Parks
        ##############
        parks_columns = parks_df.columns
        parks_crossjoin = df.crossJoin(parks_df)
        parks_levy = parks_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('parks')))
        park_counts = parks_levy.filter(parks_levy['word1_word2_levenshtein'] <= 2)
        if len(park_counts.take(1)) > 0:
            #will this indexing cause issues if first column is integer schema?
            parks_frequency = park_counts.groupBy().sum().collect()[0][0]
            types['parks'] = parks_frequency

	################
	# Building Codes
	################
        building_columns = building_code_df.columns
        building_crossjoin = df.crossJoin(building_code_df)
        building_code_levy = building_crossjoin.withColumn("word1_word2_levenshtein",levenshtein(col(df_columns[0]), col('building_codes')))
        building_counts = building_code_levy.filter(building_code_levy['word1_word2_levenshtein'] <= 1)
        if len(building_counts.take(1)) > 0:
            building_code_frequency = building_counts.groupBy().sum().collect()[0][0]
            types['building_code'] = building_code_frequency
        return types

    types_names = {}
    types_regex = {}
    types_leven = {}

    #check levenshtein distance with NAME
    if fuzz.partial_ratio(colName.lower(), 'name') > 0.75 or fuzz.partial_ratio(colName.lower(), 'street') > 0.75:
        types_names = NAME(df)

    types_regex = REGEX(df)

    types_leven = LEVEN(df)

    #merge all three dictionaries

    types = {**types_names, **types_regex, **types_leven}

    return types

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

    st_name = st_name.withColumn('category', lit('STREETNAME'))
    full_st_name = full_st_name.withColumn('category', lit('STREETNAME'))
    first_name = first_name.withColumn('category', lit('HUMANNAME'))
    last_name = last_name.withColumn('category', lit('HUMANNAME'))
    business_name = business_name.withColumn('category', lit('BUSINESSNAME'))

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

    model.save("/home/jys308/weights")

    #pred = model.transform(testData)
    #pl = pred.select("label", "prediction").rdd.cache()
    #metrics = MulticlassMetrics(pl)
    #metrics.fMeasure()

#######################################
# Gathering Data for Levenshtein Distance checking
#######################################

"""
1) City
2) Neighborhood
3) Borough
4) School Name
5) Color
6) Car Make
7) City Agency
8) Areas of Study
9) Subjects in School
10) School levels.
11) College Universities
12) Building Classification (DONE)
13) Vehicle Type
14) Type of Location
15) Parks/Playgrounds
"""
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


for file in files_and_length:
    print("This is the index of current column in sorted list::", files_and_length.index(file))
    file = file[0]
    fileData = file.split(".")
    fileName = fileData[0]
    colName = fileData[1]
    df = spark.read.option("delimiter", "\\t").option("header","true").option("inferSchema","true").csv("/user/hm74/NYCColumns/" + file)
    colNamesList = [i.replace(".","").replace(" ","_") for i in df.columns] # . and space not supported in column name by pyspark
    df = df.toDF(*colNamesList) #change name in DF
    #colName = colNamesList[0] #change colName we have
    colName = colName.replace(".","").replace(" ", "_")
    types = semanticType(colName, df)
    print("Working on", colName)
    print("This is column number", files.index(file))
    #process dictionary to record to json
    with open(str(file) +'.json', 'w') as fp:
        json.dump(types, fp)


#largest file index is 132


"""

files = files[:1]
for file in files:
    fileData = file.split(".")
    fileName = fileData[0]
    colName = fileData[1]
    df = spark.read.option("delimiter", "\\t").option("header","true").option("inferSchema","true").csv("/user/hm74/NYCColumns/" + file)
    print("Working on", colName)
    print("This is column number", files.index(file))
    types = semanticType(colName, df)
    with open('semantic_file_test.txt', 'w') as file:
        file.write(json.dumps(types))
"""
