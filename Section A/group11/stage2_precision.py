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


fileName= "label.csv"

#filename columnname label
predict_column_label= {}
#filename label
totalNumberTrueLable= {}
#labelFIle
labelFileCollect= []
#filecolumn
fileColumnLabel= []
#file arr
fileNameArr= []
import csv
with open (fileName,'r') as csv_file:
#
    reader =csv.reader(csv_file)
    next(reader) # skip first row
    for line in reader:
        file_name= line[0]
        tempName= "/user/hm74/NYCOpenData/"+file_name+".tsv.gz"
        if tempName not in fileNameArr:
            fileNameArr.append(tempName)
        labelFileCollect.append(file_name)
        column_name= line[1]
        fileColumnKey= file_name+" "+column_name
        fileColumnLabel.append(fileColumnKey)

        label1= line[2]
        label2= line[3]
        key1= file_name+" "+column_name+ " "+str(label1)
        key2= file_name+" "+column_name+ " "+str(label2)
        predict_column_label[key1]= 1
        predict_column_label[key2]= 1
        key3= file_name+" "+str(label1)
        key4= file_name+" "+str(label2)
        if key3 in totalNumberTrueLable.keys():
            totalNumberTrueLable[key3]+= 1
        else:
            totalNumberTrueLable[key3]= 1
        if key4 in totalNumberTrueLable.keys():
            totalNumberTrueLable[key4]+= 1
        else:
            totalNumberTrueLable[key4]= 1


def columnValueLabel(x, columnNameType):
#
    x= str(x)
    x= x.upper()
    arr= []
    if columnNameType!= "empty":
        temp= (columnNameType, 1)
        arr.append(temp)
#
    res= re.search(r"^[A-Z]+,?\s+(?:[A-Z]*\.?\s*)? [A-Z]+$", x)
    if res:
        if columnNameType!= "Person name":
            temp= ("Person name", 1)
            arr.append(temp)
#
    res= re.search(r"(LLC|INC|CORP)", x)
    if res:
        if columnNameType!= "Business name":
            temp= ("Business name", 1)
            arr.append(temp)
#
    res= re.search(r"^(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})$", x)
    if res:
        if columnNameType!= "Phone Number":
            temp= ("Phone Number", 1)
            arr.append(temp)
#
    res= re.search(r"^[0-9]* (.*), (.*) [A-Z]{2} [0-9]{5}(-[0-9]{4})?$", x)
    if res:
        if columnNameType!= "Address":
            temp= ("Address", 1)
            arr.append(temp)
#
    res= re.search(r"^(\d+).*?\s+(ST)$", x)
    if res:
        if columnNameType!= "Street name":
            temp= ("Street name", 1)
            arr.append(temp)
#
    res= re.search(r"^([-+]?([1-8]?\d(\.\d+)?|90(\.0+)?))?(,)?(\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?))?$", x)
    if res:
        if columnNameType!= "LAT/LON":
            temp= ("LAT/LON", 1)
            arr.append(temp)
#
    res= re.search(r"^\d{5}$", x)
    if res:
        if columnNameType!= "Zip code":
            temp= ("Zip code", 1)
            arr.append(temp)
#
    res= re.search(r"^[\w\W]* ((SCHOOL|CENTER)[\w -]*)[\w\W]*$", x)
    if res:
        if columnNameType!= "School name":
            temp= ("School name", 1)
            arr.append(temp)
#
    res= re.search(r"(ELEMENTARY|MIDDLE|JUNIOR)\s*(SCHOOL)", x)
    if res:
        if columnNameType!= "School Levels":
            temp= ("School Levels", 1)
            arr.append(temp)
#
    res= re.search(r"^[\w\W]* ((UNIVERSITY|LAW SCHOOL|COLLEGE|ACADEMY)[\w -]*)[\w\W]*$", x)
    if res:
        if columnNameType!= "College/University names ":
            temp= ("College/University names", 1)
            arr.append(temp)
#
    res= re.search(r"((HTTP|HTTPS)\:\/\/)?[A-Z0-9\.\/\?\:@\-_=#]+\.([A-Z]){2,6}([A-Z0-9\.\&\/\?\:@\-_=#])*", x)
    if res:
        if columnNameType!= "Websites":
            temp= ("Websites", 1)
            arr.append(temp)
#
    res= re.search(r"(PARK|PLAYGROUND|GYM)", x)
    if res:
        if columnNameType!= "Parks/Playgrounds":
            temp= ("Parks/Playgrounds", 1)
            arr.append(temp)
#
    res= re.search(r"NEIGHBORHOOD", x)
    if res:
        if columnNameType!= "Neighborhood":
            temp= ("Neighborhood", 1)
            arr.append(temp)
#
    res= re.search(r"CITY", x)
    if res:
        if columnNameType!= "City":
            temp= ("City", 1)
            arr.append(temp)
#
    res= re.search(r"BOROUGH", x)
    if res:
        if columnNameType!= "Borough":
            temp= ("Borough", 1)
            arr.append(temp)
#
    res= re.search(r"CAR.*MAKE", x)
    if res:
        if columnNameType!= "Car make":
            temp= ("Car make", 1)
            arr.append(temp)
#
    res= re.search(r"(CITY)?.*AGENCY", x)
    if res:
        if columnNameType!= "City agency":
            temp= ("City agency", 1)
            arr.append(temp)
#
    res= re.search(r"(AMBULANCE|VAN|TAXI|TAXI|SUV|BUS)", x)
    if res:
        if columnNameType!= "Vehicle Type":
            temp= ("Vehicle Type", 1)
            arr.append(temp)
#
    res= re.search(r"(BUILDING|AIRPORT|TERMINAL|BANK|CHURCH|CLOTHING|BOUTIQUE)", x)
    if res:
        if columnNameType!= "Type of location":
            temp= ("Type of location", 1)
            arr.append(temp)
#  
    res= re.search(r"(ARCHITECTURE|ANIMAL SCIENCE|COMMUNICATIONS|COMPUTER SCIENCE|INTEREST)", x)
    if res:
        if columnNameType!= "Areas of study":
            temp= ("Areas of study", 1)
            arr.append(temp)
#   
    res= re.search(r"(MATH|HISTORY|CHEMISTRY|PHYSIC|ART|READING|LANGUAGE|PYSCHOLOGY)", x)
    if res:
        if columnNameType!= "Subjects in school":
            temp= ("Subjects in school", 1)
            arr.append(temp)   
#
    tp = r"\d{2}\:\d{2}\:\d{2}"
    time_pattern = re.compile(tp)
    time_list = time_pattern.findall(x)
    if(len(time_list)!=0):
        if columnNameType!= "Time":
            temp= ("Time", 1)
            arr.append(temp)
#
    tp = r"\d{2}\:\d{2}"
    time_pattern = re.compile(tp)
    time_list = time_pattern.findall(x)
    if(len(time_list)!=0):
#
        if columnNameType!= "Time":
            temp= ("Time", 1)
            arr.append(temp)
#
    dp = r"\d{4}.\d{2}.\d{2}"
    date_pattern = re.compile(dp)
    date_list = date_pattern.findall(x)
    if(len(date_list)!= 0):
#
        if columnNameType!= "Date":
            temp= ("Date", 1)
            arr.append(temp)
#
    dp = r"\d{2}.\d{2}.\d{4}"
    date_pattern = re.compile(dp)
    date_list = date_pattern.findall(x)
    if(len(date_list)!= 0):
#
        if columnNameType!= "Date":
            temp= ("Date", 1)
            arr.append(temp)

    if len(arr)== 0:
        if columnNameType== "empty":
            temp= ("Other", 1)
            arr.append(temp)
#
    return arr
#

def columnName(x):
    x= x.upper()
#
    res= re.search(r"BUSINESS.*NAME", x)
    if res:
        return "Business name"
#
    res= re.search(r"(LAST|FIRST|MIDDLE|FULL).*NAME", x)
    if res:
        return "Person name"
#
    res= re.search(r"PHONE.*NUMBER", x)
    if res:
        return "Phone Number"
#       
    res= re.search(r"TIME", x)
    if res:
        return "Time"
#
    res= re.search(r"DATE", x)
    if res:
        return "Date"
#
    res= re.search(r"ADDRESS", x)
    if res:
        return "Address"
#
    res= re.search(r"STREET.*NAME", x)
    if res:
        return "Street name"
#
    res= re.search(r"(LATITUDE|LONGITUDE|LATITUDE.*LONGITUDE|LONGITUDE.*LATITUDE)", x)
    if res:
        return "LAT/LON"
#
    res= re.search(r"ZIP", x)
    if res:
        return "Zip code"
#
    res= re.search(r".*SCHOOL", x)
    if res:
        return "School name"
#
    res= re.search(r"(ELEMENTARY|MIDDLE|HIGH|JUNIOR).*(SCHOOL)", x)
    if res:
        return "School Levels"
#
    res= re.search(r".*(UNIVERSITY|LAW SCHOOL|COLLEGE|ACADEMY).*", x)
    if res:
        return "College/University names"
#
    res= re.search(r"(WEBSITE|WEB)", x)
    if res:
        return "Websites"
#
    res= re.search(r"CITY", x)
    if res:
        return "City"
#
    res= re.search(r"NEIGHBORHOOD", x)
    if res:
        return "Neighborhood"
#
    res= re.search(r"BOROUGH", x)
    if res:
        return "Borough"
#
    res= re.search(r"CAR.*MAKE", x)
    if res:
        return "Car make"
#
    res= re.search(r"(CITY)?.*AGENCY", x)
    if res:
        return "City agency"
#
    res= re.search(r"(PARK|PLAYGROUND|GYM)", x)
    if res:
        return "Parks/Playgrounds"
#
    res= re.search(r"VEHICLE.*TYPE", x)
    if res:
        return "Vehicle Type"
#
    res= re.search(r"TYPE.*LOCATION", x)
    if res:
        return "Type of location"
#
    res= re.search(r"SUBJECT.*SCHOOL", x)
    if res:
        return "Subjects in school"
#
    res= re.search(r"(AREA.*STUDY|INTEREST)", x)
    if res:
        return "Areas of study"
#
    res= re.search(r"COLOR", x)
    if res:
        return "Color"
#
    return "empty"
#

label_count= {}
label_heter= 0
label_homo= 0

dict_dataset= []
fp=  open('data/task2_strategy1.json', 'w') 
print(fileNameArr)
for file in fileNameArr:
#
    print(file)
    try:
        resDict= {}
        file_name_use_true= file.split("/")[4].split(".")[0]
        resDict["filename"]= file_name_use_true+".tsv"
        df= spark.read.format('csv').options(header='true',inferschema='true', delimiter='\t').load(file)
        df.createOrReplaceTempView("df")
        rdd= df.rdd.map(list)
        print("size: "+str(rdd.count()))
        columns= df.columns
        #label->count for this file
        correct_label= {}
        #label->you predict as this label 
        total_number_predict_as_type= {}

        resDict["colunms"]= []
        for i in range(len(columns)):
            col= columns[i]
            column_dict= {}
            column_dict["column_name"]= col 
            column_dict["semantic_types"]= []
            predictFileColumn= file_name_use_true+" "+col
            columNameLabel= columnName(col)
            rSingleColumn= rdd.map(lambda x: x[i])
            columnLabel= rSingleColumn.flatMap(lambda x: columnValueLabel(x, columNameLabel)).reduceByKey(lambda x, y: x+y)
            columnLabel= columnLabel.sortBy(lambda x: x[1], False)

            arr= columnLabel.take(2)
            for pair in arr:
                pairDict= {}
                key= pair[0]
                value= pair[1]
                pairDict["semantic_type"]= key 
                pairDict["count"]= value 
                column_dict["semantic_types"].append(pairDict)

            resDict["colunms"].append(column_dict)
            #pair is (label, count) for this file
            #we first check whether this cuolumn is labeled by our group
            if predictFileColumn in fileColumnLabel:
                for pair in arr:
                    labelName= pair[0]
                    key= file_name_use_true+" "+col+" "+labelName
                    #if file+coulmn+predict exist in our label.json file
                    #it means we find correct prediction
                    if key in predict_column_label.keys():
                        if labelName in correct_label.keys():
                            correct_label[labelName]+=1
                        else:
                            correct_label[labelName]= 1

                    if labelName in total_number_predict_as_type.keys():
                        total_number_predict_as_type[labelName]+=1
                    else:
                        total_number_predict_as_type[labelName]= 1


            if len(arr)> 1:
                label_heter+= 1
            else:
                label_homo+= 1

            for pair in arr:
                key= pair[0]
                if key in label_count.keys(): 
                    label_count[key]+=1
                else:
                    label_count[key]= 1

        resDict["recall_precision"]= []
        for keyLabel in total_number_predict_as_type.keys():
            recallDict= {}
            recallDict["label"]= keyLabel
            if keyLabel in correct_label.keys():
                predict_correct= correct_label[keyLabel]
            else:
                predict_correct= 0

            total_predict_as_label= total_number_predict_as_type[keyLabel]

            newKey= file_name_use_true+" "+keyLabel

            if newKey in totalNumberTrueLable.keys():
                total_true_as_label= totalNumberTrueLable[newKey]
            else:
                total_true_as_label= 0

            if total_predict_as_label== 0:
                precision= 0
            else:
                precision= predict_correct/total_predict_as_label

            if  total_true_as_label== 0:
                recall= 0
            else:
                recall= predict_correct/total_true_as_label
            print("precision: "+str(precision)+" recall: "+str(recall))
            recallDict["precision"]= precision
            recallDict["recall"]= recall
            resDict["recall_precision"].append(recallDict)

        dict_dataset.append(resDict)
        print("finish one file\n\n")
    except Exception as e: 
        print(e)


import json
json.dump(dict_dataset, fp,default=str)


with open("data/count1.txt", 'w') as ff:
    ff.write("heter: "+str(label_heter)+"\n")
    ff.write("homo: "+str(label_homo)+"\n")
    for key in label_count.keys():
        value= label_count.get(key)
        ff.write("label: "+str(key)+" count: "+str(value)+"\n")


label_list= []
keys= label_count.keys()
label_list = [key for key in keys]

plt.figure()
plt.bar(range(len(label_list)), [label_count.get(xtick, 0) for xtick in label_list], align='center',yerr=0.000001)
plt.xticks(range(len(label_list)), label_list)
plt.xlabel('Label')
plt.ylabel('Frequency')
plt.savefig("labe_count.jpg")



plt.figure()
label_list2 = ["hetergous, homogenous"]
label_value2= []
label_value2.append(label_heter)
label_value2.append(label_homo)


plt.subplot(2, 1, 2)
plt.bar(range(len(label_list2)), label_value1, align='center',yerr=0.000001)
plt.xticks(range(len(label_list2)), label_list2)
plt.xlabel('Label_type')
plt.ylabel('Frequency')

plt.savefig("heterogous.jpg")
