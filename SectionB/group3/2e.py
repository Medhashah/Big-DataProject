import csv
import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()


######################## Util ########################

def filter_ground_truth(semantic_type):
    return {k: v for [k, v] in ground_truth.items() if semantic_type in v}


######################## Main ########################

semantic_types_threshold = {
    'person_name': 0.5,
    'business_name': 0.5,
    'phone_number': 0.9,
    'address': 0.5,
    'street_name': 0.5,
    'city': 0.5,
    'neighborhood': 0.8,
    'lat_lon_cord': 0.9,
    'zip_code': 0.9,
    'borough': 0.9,
    'school_name': 0.45,
    'color': 0.7,
    'car_make': 0.9,
    'city_agency': 0.6,
    'area_of_study': 0.9,
    'subject_in_school': 0.9,
    'school_level': 0.9,
    'college_name': 0.5,
    'website': 0.9,
    'building_classification': 0.9,
    'vehicle_type': 0.5,
    'location_type': 0.9,
    'park_playground': 0.3,
    'other': 0.5,
}

correct_postive = {semantic_type: 0 for semantic_type in semantic_types_threshold}
predict_postive = {semantic_type: 0 for semantic_type in semantic_types_threshold}
truth_postive = {semantic_type: 0 for semantic_type in semantic_types_threshold}

# 1. list the working subset
cluster = json.loads(spark.read.text('/user/ql1045/proj-in/cluster2.txt').collect()[0][0].replace("'", '"'))
ground_truth = {
    filename: {
        # label.filter(e => e)
        semantic_type.strip()
        for semantic_type in labels if semantic_type
    }
    # for each line
    for [filename, *labels] in (spark.read.format('csv')
                                .options(header='true', inferschema='true')
                                .load('/user/ql1045/proj-in/DF_Label.csv')
                                .toLocalIterator())
}

# 2. for each working dataset
for filename in cluster:
    # filename = cluster[0]
    [dataset_name, column_name] = filename.split('.')[0:2]
    print(u'>> entering {}'.format(filename))

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))

    # 2.2 count dataset rows
    invalid_words = ['UNSPECIFIED', 'UNKNOWN', 'UNKNOW', '-', 'NA', 'N/A', '__________']
    dataset = dataset.filter(~dataset.value.isin(invalid_words))
    dataset_count = dataset.select(F.sum('count')).collect()[0][0]

    # 2.3 load the corresponding semantic profile
    with open('task2/{}.json'.format(dataset_name+'.'+column_name)) as f:
        output = json.load(f)

    # 2.4 exceeds threshold => attach label
    labels = []
    for entry in output['semantic_types']:
        semantic_type = entry['semantic_type']
        count = entry['count']
        if count > semantic_types_threshold[semantic_type] * dataset_count:
            labels.append(semantic_type)

    # 2.5 evaluate
    for semantic_type in labels:
        predict_postive[semantic_type] += 1
        if semantic_type in ground_truth[filename]:
            correct_postive[semantic_type] += 1
    for semantic_type in ground_truth[filename]:
        truth_postive[semantic_type] += 1

print(json.dumps(correct_postive))
print(json.dumps(predict_postive))
print(json.dumps(truth_postive))
