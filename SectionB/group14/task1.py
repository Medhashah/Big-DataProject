from pyspark.sql import functions, types
from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
import json
from dateutil.parser import *
import io

sc = SparkContext()
spark = SparkSession \
        .builder \
        .appName("project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

datasets = (spark.read.format('csv')
            .options(inferschema='true', sep='\t')
            .load('/user/hm74/NYCOpenData/datasets.tsv')
            .toDF('filename', 'title'))

## Check if a string mathces with specific formats. Return true if the string matches date format
def check_date(string):
    fmts = ('%b %d','%b-%d', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S','%Y-%m-%d %I:%M:%S %p', '%Y%m%d', '%m/%d/%y', '%m/%d/%Y','%m/%d/%Y %H:%M:%S','%m/%d/%Y %I:%M:%S %p','%b %d,%Y', '%m/%d/%Y %H:%M','%m/%d/%Y %I:%M %p','%Y-%m-%d %H:%M','%Y-%m-%d %I:%M %p')
    for i in range(len(fmts)):
        try:
            date_obj = datetime.datetime.strptime(string, fmts[i])
            if date_obj.year >= 1900 and date_obj.year < 2050:
                return True
        except ValueError:      
            continue
    return False

## Check the data type of an input string. Return the data type
def predict_data_type(input):
    if input is not None:
        if check_date(str(input)):
            return 'date'
        input = str(input).replace(' ', '').lower()
        if input.replace(',', '').isdecimal():
            return 'integer'
        if input.replace('.', '').replace(',', '').replace('%', '').isdecimal():
            return 'real'
        if input == 'null':
            return 'null'
        return 'text'
    else:
        return 'null'

## find the format of date object. Return the position index of the format in the list
def date_format(string):
    fmts = ('%b %d','%b-%d', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S','%Y-%m-%d %I:%M:%S %p', '%Y%m%d', '%m/%d/%y', '%m/%d/%Y','%m/%d/%Y %H:%M:%S','%m/%d/%Y %I:%M:%S %p','%b %d,%Y', '%m/%d/%Y %H:%M','%m/%d/%Y %I:%M %p','%Y-%m-%d %H:%M','%Y-%m-%d %I:%M %p')
    for i in range(len(fmts)):
        try:
            date_obj = datetime.datetime.strptime(string, fmts[i])
            if date_obj.year >= 1900 and date_obj.year < 2050:
                return i
        except ValueError:      
            continue
    return -1

## Remove the comma and percentage symbol from real objest. Return the decimal part
def remove_format(input):
    if input is not None:
        input = str(input).replace(',', '').replace('%', '')
        if input.isdecimal():
            return float(input)
        else:
            return input
        
## Return the length of input
def length(input):
    if input is not None:
        return len(input)
    else:
        return 0

## Check if the input is None Type. Return null if it is None Type
def check_null(input):
    if input is None:
        return 'null'
    else:
        return input

filenames = datasets.select("filename").rdd.flatMap(lambda x: x).collect()
for i in range(0,len(filenames)):
    filename = filenames[i]
    
    dataset = (spark.read.format('csv')
               .options(header='true', inferschema='true', sep='\t')
               .load('/user/hm74/NYCOpenData/{}.tsv.gz'.format(filename)))

    output = {'dataset_name': '', 'columns': [], 'key_column_candidates': []}
    output['dataset_name'] = filename
    
    ## Go over each column
    for column in dataset.schema:
        dataset.createOrReplaceTempView('dataset')
        dataType = column.dataType
        name = column.name
        column_output = {
                'column_name': '',
                'number_non_empty_cells': 0,
                'number_empty_cells': 0,
                'number_distinct_values': 0,
                'frequent_values': [],
                'data_types': []
            }
        ## Find column name, numver of non empty cells, number of empty cells, number of distinct values and top 5 frequent values
        column_output['column_name'] = name
        column_output['number_non_empty_cells'] =  dataset.filter(dataset["`"+name+"`"].isNotNull()).count()
        column_output['number_empty_cells'] = dataset.filter(dataset["`"+name+"`"].isNull()).count()
        column_output['number_distinct_values'] = dataset.select(dataset["`"+name+"`"]).distinct().count()
        column_output['frequent_values'] = [x[0] for x in (dataset.groupBy(dataset["`"+name+"`"]).count()
                                                           .orderBy(functions.desc('count'))
                                                           .select(dataset["`"+name+"`"])
                                                           .limit(5).collect())]

        if column_output['number_non_empty_cells'] != 0:
            col_index = dataset.columns.index(name)
            ## Convert the column of dataset to a dataframe named 'data_types'
            ## Dataframe 'data_types' has 3 column, named 'name', 'data_type', 'count'
            ## If column.dataType is String type, the column would be grouped by its value
            ## 'name' column stores the value
            ## 'data_type' column stores the datatype of value
            ## 'count' column stores only 1's if column.dataType is Integer or Double
            ## 'count' column stores the count number of the value if column.dataType is String
            if str(column.dataType) == "IntegerType" or str(column.dataType) == "LongType":
                ## If datatype is Integer, all of the values in the column are integers
                data_types = spark.sql("SELECT `"+name+"`, 'integer', 1 FROM dataset")
                data_types = data_types.toDF('name','data_type','count')
            elif str(column.dataType) == "DoubleType" or str(column.dataType) == "FloatType":
                ## If datatype is Float, all of the values in the column are reals
                data_types = spark.sql("SELECT `"+name+"`, 'real', 1 FROM dataset")
                data_types = data_types.toDF('name','data_type','count')
            else:
                ## If datatype is String, group it by its value
                ## Predict data types for each distinct value
                distinct_data = spark.sql("SELECT `"+name+"`, count(*) FROM dataset group by `"+name+"`")
                try:
                    data_types = distinct_data.rdd.map(lambda x: (x[0], predict_data_type(x[0]),x[1])).toDF(sampleRatio=0.2)
                except:
                    data_types = distinct_data.rdd.map(lambda x: (x[0], predict_data_type(x[0]),x[1])).toDF()
                data_types = data_types.withColumnRenamed('_1', 'name').withColumnRenamed('_2', 'data_type').withColumnRenamed('_3', 'count')      
            data_types.createOrReplaceTempView('data_types')
            
            ## Count the number of each data type
            try:
                integer_count = spark.sql("SELECT SUM(count) FROM data_types WHERE data_type = 'integer' group by data_type").collect()[0][0]
            except: 
                integer_count=0
                
            try: 
                real_count = spark.sql("SELECT SUM(count) FROM data_types WHERE data_type = 'real' group by data_type").collect()[0][0]
            except: 
                real_count = 0
                
            try:
                date_count = spark.sql("SELECT SUM(count) FROM data_types WHERE data_type = 'date' group by data_type").collect()[0][0]
            except: 
                date_count = 0
                
            try:
                text_count = spark.sql("SELECT SUM(count) FROM data_types WHERE data_type = 'text' group by data_type").collect()[0][0]
            except: 
                text_count=0
                
            ## Compute the maximum, minimum, mean and stddev for integer type
            if integer_count != 0:
                data_output = {
                "type": "INTEGER (LONG)",
                "count": 0,
                "max_value": 0,
                "min_value": 0,
                "mean": 0,
                "stddev": 0,
                        }
                data_output['count'] = integer_count
                data_output['max_value'] = spark.sql("SELECT MAX(name) FROM data_types WHERE data_type = 'integer'").collect()[0][0]
                data_output['min_value'] = spark.sql("SElECT MIN(name) FROM data_types WHERE data_type = 'integer'").collect()[0][0]
                data_output['mean'] = spark.sql("SElECT SUM(name*count)/"+str(integer_count)+" FROM data_types WHERE data_type = 'integer'").collect()[0][0]
                data_output['stddev'] = spark.sql("SElECT SQRT(SUM((POWER((name-("+str(data_output['mean'])+")),2))*count)/"+str(integer_count)+" ) FROM data_types WHERE data_type = 'integer'").collect()[0][0]
                column_output['data_types'].append(data_output)
            ## Compute the maximum, minimum, mean and stddev for real type
            ## Go through all of the real data, and remove the comma and percentage symbol
            ## Make a new dataframe callee 'types'. The format is the same as data_types
            if real_count != 0:
                data_output = {
                            "type": "REAL",
                            "count": 0,
                            "max_value": 0,
                            "min_value": 0,
                            "mean": 0,
                            "stddev": 0,
                        }
                data_output['count'] = real_count
                real_values = spark.sql("SELECT name, data_type, count from data_types WHERE data_type = 'real'")
                try:
                    values = real_values.rdd.map(lambda x: (remove_format(x[0]), x[1],x[2])).toDF(sampleRatio=0.2)
                except:
                    values = real_values.rdd.map(lambda x: (remove_format(x[0]), x[1],x[2])).toDF()
                types = values.withColumnRenamed('_1', 'name').withColumnRenamed('_2', 'data_type').withColumnRenamed('_3', 'count')
                types.createOrReplaceTempView('types')
                data_output['max_value'] = spark.sql("SELECT MAX(name) FROM types WHERE data_type = 'real'").collect()[0][0]
                data_output['min_value'] = spark.sql("SElECT MIN(name) FROM types WHERE data_type = 'real'").collect()[0][0]
                data_output['mean'] = spark.sql("SElECT SUM(name*count)/"+str(real_count)+" FROM types WHERE data_type = 'real'").collect()[0][0]
                data_output['stddev'] = spark.sql("SElECT SQRT(SUM((POWER((name-("+str(data_output['mean'])+")),2))*count)/"+str(real_count)+" ) FROM types WHERE data_type = 'real'").collect()[0][0]
                column_output['data_types'].append(data_output)
            ## Compute the maximum, minimum for date type
            ## Convert the string to date object using the format that is found using date_format(), then sort it
            if date_count != 0:
                data_output = {
                            "type": "DATE/TIME",
                            "count": 0,
                            "max_value": '',
                            "min_value": '',
                        }
                date_values = spark.sql("SELECT name FROM data_types WHERE data_type = 'date'")      
                index = date_format(str(date_values.collect()[0][0]))
                if (str(column.dataType) == "StringType"):
                    fmts = ("'MMM dd'","'MMM-dd'", "'yyyy-MM-dd'", "'yyyy-MM-dd HH:mm:ss'","'yyyy-MM-dd hh:mm:ss aa'", "'yyyyMMdd'", "'MM/dd/yyyy'", "'MM/dd/yy'","'MM/dd/yyyy HH:mm:ss'","'MM/dd/yyyy hh:mm:ss aa'","'MMM dd,yyyy'","'MM/dd/yyyy HH:mm'","'MM/dd/yyyy hh:mm aa'", "'yyyy-MM-dd HH:mm'","'yyyy-MM-dd hh:mm aa'")
                    fmt = fmts[index]
                    temp = "SELECT name FROM data_types WHERE data_type = 'date' order by TO_DATE(CAST(UNIX_TIMESTAMP(name,"+fmt+") AS TIMESTAMP)) "
                else:
                    temp = "SELECT name FROM data_types WHERE data_type = 'date' order by name"                
                db = spark.sql(temp)
                data_output['count'] = date_count
                data_output['max_value'] = (db.collect())[-1][0]
                data_output['min_value'] = (db.collect())[0][0]
                column_output['data_types'].append(data_output)
            ## Compute the shortest length, longest length and average length of for string type
            ## Make a new dataframe callee 'text_values'. It has 3 column, 'name', 'count', 'wordCount'
            if text_count != 0:
                data_output = {
                            "type": "TEXT",
                            "count": 0,
                            "shortest_values": [],
                            "longest_values": [],
                            "average_length": 0,
                        }
                data_output['count'] = text_count
                text_values = spark.sql("SELECT name,count FROM data_types WHERE data_type = 'text'")      
                text_values = text_values.withColumn('wordCount', functions.length('name'))
                text_values.createOrReplaceTempView('text_values')
                length = [x[0] for x in (text_values.orderBy("wordCount").select(text_values["name"]).collect())] 
                count_length = [x[0] for x in (text_values.orderBy("wordCount").select(text_values["count"]).collect())] 
                shortest = []
                num = 0
                for i in range(len(length)):
                    for j in range(int(count_length[i])):
                        shortest.append(length[i])
                        num+=1
                        if num > 4:
                            break
                    if num > 4:
                        break                
                longest = []
                num = 0
                for i in range(len(length)-1,-1,-1):
                    for j in range(int(count_length[i])):
                        longest.append(length[i])
                        num+=1
                        if num > 4:
                            break
                    if num > 4:
                        break    
                data_output['shortest_values'] = shortest[:5]
                data_output['longest_values'] = longest[:5]
                data_output['average_length'] = spark.sql("SElECT SUM(wordCount*count)/"+str(text_count)+" FROM text_values").collect()[0][0]
                column_output['data_types'].append(data_output)
        else:
            data_types = None
        output['columns'].append(column_output)
    
    json.dump(output, open('{}.spec.json'.format(filename), 'w+'), default=str)