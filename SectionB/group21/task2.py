# You need to import everything below
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import split, lit, udf
from pyspark.sql import functions


from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StringIndexer, OneHotEncoder, VectorAssembler, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics

import math
import random
import json

import lime 
from lime import lime_text
from lime.lime_text import LimeTextExplainer

import numpy as np

import csv

path_prefix = ''# '/user/zl3107/'   --conf spark.executor.memoryOverhead=6G --executor-memory 12G  --conf spark.driver.maxResultSize=0

trial_num = 272  # number of cloumns

sc = SparkContext.getOrCreate()
#sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("proj") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

########################################################################################################
# Load data

def csv_read(path):
    data = []
    with open(path,'r',encoding='utf-8') as f:
        reader = csv.reader(f,dialect='excel')
        for row in reader:
            data.append(row)
    return data


data = csv_read( path_prefix + 'label.csv')    #  column_name and label(s)    '/user/zl3107/label.csv'
data1 = data

categories = ['person_name','business_name','phone_number', 'address', 'street_name', 'city', 'neighborhood', 
'lat_lon_cord', 'zip_code', 'borough', 'school_name', 'color', 'car_make', 'city_agency', 'area_of_study', 
'subject_in_school', 'school_level', 'college_name', 'website', 'building_classification', 'vehicle_type', 
'location_type', 'park_playground', 'other'] # 23 + 1

LabeledItem = pyspark.sql.Row('item','num' )
    
def prepareDF(document):
    rdds = sc.textFile(path_prefix + 'NYCColumns/' +  document).map(lambda x: LabeledItem( x.split('\t')[0] , x.split('\t')[1] ))
    return rdds.toDF()



df_set = []

for x in data:                             # load each column
    column_id = data.index(x)
#    print(x)
    df = prepareDF(x[0]).withColumn('category1',lit(x[1])).withColumn('category2',lit(x[2])).withColumn('column_id',lit( column_id ))#.withColumn('label',lit( float(categories.index(x[1])) ))
    df = df.withColumn('number', df['num'].cast(DoubleType()))
    df_set.append(df)

print('data loadding complete')

###############################################################################
# build train set and test set

def get_train_set(trial_num ):
    label_set = set()
    train_set = []
    l = list(range(trial_num ))
    while len(label_set) < len(categories) - 2 :   # make sure each category appears at least once (except college_label)
        train_set.clear()
        train_set = random.sample(l, math.ceil(trial_num *0.5) )
        label_set.clear()
        for x in train_set:
            if data[x][2] == ''  or  data[x][1] == 'city' :
                label_set.add(data[x][1])
        if len(label_set) == len(categories) - 1:
            return train_set
###################
        # print(train_set, label_set)
    return train_set



l = list(range(trial_num))
# train_set = get_train_set(trial_num )                         #### index
train_set = random.sample(l, math.ceil(trial_num *0.5) )

test_set = l

# test_set = []
# for x in l:
#     if y not in train_set:
#         test_set.append(y)

train_sub_set = []
for x in train_set:
    if data[x][2]=='' or data[x][1] == 'city':
        train_sub_set.append(x)

print('pre_df complete')


##

train_df = df_set[train_sub_set[0] ]
for x in range(1,len(train_sub_set)):
    train_df = train_df.union(df_set[x])
    # print('union column ' + str(x) + 'complete')

test_df = df_set[ test_set[0] ]
for x in range(1,len(test_set)):
    test_df = test_df.union(df_set[x])
    # print('union column ' + str(x) + 'complete')


########################################################################################################
# Build pipeline and run
label_stringIdx   = StringIndexer(inputCol="category1", outputCol="label")
tokenizer = RegexTokenizer(pattern=u'\W+', inputCol="item", outputCol="words", toLowercase=False) 
count_vectors = CountVectorizer(inputCol='words', outputCol='features', vocabSize=1000000, minDF=5)
lr = LogisticRegression(maxIter=20, regParam=0.001,weightCol="number")


# Builing model pipeline
pipeline = Pipeline(stages=[label_stringIdx, tokenizer, count_vectors,lr])
# pipeline = Pipeline(stages=[tokenizer, count_vectors,lr])

# Train model on training set
model = pipeline.fit(train_df)   

# Model prediction on test set
pred = model.transform(test_df)  


print('inital prediction complete')

##############################################################################################
mapping = {}  # label to category

tmp_row = pred.select('label','category1').drop_duplicates().collect() #.groupby('label','category1').agg(functions.count("label")).select('label','category1').collect() 

for x in tmp_row:
    mapping[x[0]] = x[1]


prefix = 'similar_to_'

def write_json(name,l):
    for x in l:
        jsons = {'column_name': name , 'semantic_types':l}
        filename= name[:-3] + 'json'
        with open(filename,'w') as file_obj:
            json.dump(jsons,file_obj)


def if_change_to_other(probability,words,most_often_label,second_often_label,most_count_label,second_count_label,double_label): 
    pred_label = 0.0
    max_probability = max(probability)
    for x in range(len(probability)):
        if max_probability == probability[x]:
            pred_label = float(x)
            break
    # if len(words) == 1 and words[0] == 'BROOKLYN':
    #     pass
    if double_label == True and probability[int(round(second_often_label))] > 0.5 * max_probability and probability[int(round(second_often_label))] > probability[int(round(most_often_label))]:
        return second_often_label
    elif probability[int(round(most_often_label))] > 0.5 * max_probability and (double_label == False or pred_label != second_often_label):
        return most_often_label
    return pred_label


if_change_to_other_udf = udf(if_change_to_other, DoubleType())



for x in data[0:trial_num]: ##########################################################   testtest    150 178
    print('column '+ str(data.index(x) ) + ' start')
    # result_df = pred.filter( pred['column_id'] == data.index(x))
    result_df = model.transform(df_set[data.index(x) ])  
#    item_number = result_df.count()
    tmp_df = result_df.groupby(result_df.prediction ).agg(functions.sum("number"),functions.count("prediction"))
#############################                    find out most often and second most often items (weight)
    if tmp_df.select('prediction').distinct().count() == 1:
        label0 = mapping[tmp_df.select('prediction').distinct().take(1)[0][0]]
        data1[data.index(x)].append(label0)
        data1[data.index(x)].append('')
        write_json(x[0],  [ {'semantic_type':  label0 , 'label': '', 'count': tmp_df.select('sum(number)').distinct().take(1)[0][0] } ] )
        continue
    most_often = tmp_df.orderBy("sum(number)", ascending=False).take(2)
    if most_often.count()==0:
        continue
    most_often_label = most_often[0][0]
    most_often_frequency = most_often[0][1]
    second_often_label = most_often[1][0]
    second_often_frequency = most_often[1][1]
################
    most_often_category = mapping[most_often_label]
    second_often_category = mapping[second_often_label]
#######################                       find out most often and second most often items (unweight)
    most_count = tmp_df.orderBy("count(prediction)", ascending=False).take(2)
    most_count_label = most_count[0][0]
    most_count_cnt = most_count[0][2]
    second_count_label = most_count[1][0]
    second_count_cnt = most_count[1][2]
##########################
    most_count_category = mapping[most_count_label]
    second_count_category = mapping[second_count_label]
######################
    double_label = False      ##  whether contain two labels
#############################
    if most_often_label == most_count_label and second_count_cnt > 0.5 * most_count_cnt and second_often_frequency > 0.5 * most_often_frequency:
        double_label = True
    elif most_often_label == second_count_label and most_count_label == second_often_label and second_often_frequency > 0.05 * most_often_frequency :
        double_label = True
###################             ## mark the label of the column
    data1[data.index(x)].append(most_often_category)
    if double_label == True:
        data1[data.index(x)].append(second_often_category)
    else:
        data1[data.index(x)].append('')
################################               ## refine the labels of each item in the column
    result_df1 = result_df.withColumn('re_label1',if_change_to_other_udf(result_df.probability, result_df.words,\
        lit(most_often_label) ,lit(second_often_label), lit(most_count_label),lit(second_count_label), lit(double_label) ))
####################################            ## output to a json file
    result_df2 = result_df1.select('re_label1','number').groupby(result_df1.re_label1).agg(functions.sum("number")).select('re_label1','sum(number)')
#    result_df2.show()
    result_rows = result_df2.collect()
    semantic_types = []
    for y in result_rows:
        semantic_type = {}
        if y[0] == most_count_label or (y[0] == second_often_label and double_label == True):
            semantic_type['semantic_type'] = mapping[y[0]]
            semantic_type['label'] = ''
        else:
            semantic_type['semantic_type'] = 'other'
            semantic_type['label'] = prefix + mapping[y[0]]
        semantic_type['count'] = y[1]
        semantic_types.append(semantic_type)
    write_json(x[0],semantic_types)
    print('column '+ str(data.index(x) ) + ' complete')


################################################################################################

precision = []
recall = []

for x in range(0, len(categories)-1):
    label = categories[x]
    correct_cnt = 0      
    predict_cnt = 0
    actual_num = 0
    for y in data1[0:trial_num]:    #######################################################  testtest
        tmp1 = 0
        tmp2 = 0
        if y[1] == label or y[2] == label:
            tmp1 = 1
        if y[3] == label or y[4] == label:
            tmp2 = 1
        correct_cnt += tmp1 & tmp2
        predict_cnt += tmp2
        actual_num += tmp1
    if predict_cnt == 0:
        precision.append(-1)
    else:
        precision.append(correct_cnt/predict_cnt)
    if actual_num == 0:
        recall.append(-1)
    else:
        recall.append(correct_cnt/actual_num)

out=open(path_prefix + 'precision_recall','w+') 
for x in range(0, len(categories)-1):
    print( categories[x] + '  precision: ' + str(precision[x]) + ' recall: ' + str(recall[x]) + '\n'   ,file=out)


out.close()


#################################### if miss any columns
cntt = 0
# num = 198
for num in range(0,trial_num):
    if os.path.exists( data[num][0][:-3] +  'json'):
        cntt += 1
        # print(num)
        continue
################################
    result_df = model.transform(df_set[num])  
    tmp_df = result_df.groupby(result_df.prediction ).agg(functions.sum("number"),functions.count("prediction"))
    tmp_df.count()
    #############################                    find out most often and second most often items (weight)
    if tmp_df.select('prediction').distinct().count() == 1:
        label0 = mapping[tmp_df.select('prediction').distinct().take(1)[0][0]]
    # data1[data.index(x)].append(label0)
    # data1[data.index(x)].append('')
        write_json(x[0],  [ {'semantic_type':  label0 , 'label': '', 'count': tmp_df.select('sum(number)').distinct().take(1)[0][0] } ] )
        continue
##############################
    most_often = tmp_df.orderBy("sum(number)", ascending=False).take(2)
    # if most_often.count()==0:
    #     continue
    most_often_label = most_often[0][0]
    most_often_frequency = most_often[0][1]
    second_often_label = most_often[1][0]
    second_often_frequency = most_often[1][1]
    ################
    most_often_category = mapping[most_often_label]
    second_often_category = mapping[second_often_label]
    #######################                       find out most often and second most often items (unweight)
    most_count = tmp_df.orderBy("count(prediction)", ascending=False).take(2)
    most_count_label = most_count[0][0]
    most_count_cnt = most_count[0][2]
    second_count_label = most_count[1][0]
    second_count_cnt = most_count[1][2]
    ##########################
    most_count_category = mapping[most_count_label]
    second_count_category = mapping[second_count_label]
    ######################
    double_label = False      ##  whether contain two labels
    #############################
    # if most_often_label == most_count_label and second_count_cnt > 0.5 * most_count_cnt and second_often_frequency > 0.5 * most_often_frequency:
    #     double_label = True
    # elif most_often_label == second_count_label and most_count_label == second_often_label and second_often_frequency > 0.05 * most_often_frequency :
    #     double_label = True
    # ###################             ## mark the label of the column
    # data1[data.index(x)].append(most_often_category)
    # if double_label == True:
    #     data1[data.index(x)].append(second_often_category)
    # else:
    #     data1[data.index(x)].append('')
    ################################               ## refine the labels of each item in the column
    result_df1 = result_df.withColumn('re_label1',if_change_to_other_udf(result_df.probability, result_df.words,\
        lit(most_often_label) ,lit(second_often_label), lit(most_count_label),lit(second_count_label), lit(double_label) ))
    ####################################            ## output to a json file
    result_df2 = result_df1.select('re_label1','number').groupby(result_df1.re_label1).agg(functions.sum("number")).select('re_label1','sum(number)')
    #    result_df2.show()
    result_rows = result_df2.collect()
    semantic_types = []
    for y in result_rows:
        semantic_type = {}
        if y[0] == most_count_label or (y[0] == second_often_label and double_label == True):
            semantic_type['semantic_type'] = mapping[y[0]]
            semantic_type['label'] = ''
        else:
            semantic_type['semantic_type'] = 'other'
            semantic_type['label'] = prefix + mapping[y[0]]
        semantic_type['count'] = y[1]
        semantic_types.append(semantic_type)
###############################
    write_json(data[num][0],semantic_types)
