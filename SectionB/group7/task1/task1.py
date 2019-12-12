from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime
import json

def identify_type(value):
    try:
        converted = datetime.strptime(value, '%Y%m')
        return 'DATE/TIME'
    except:
        pass
    try:
        converted = int(value)
        return 'INTEGER'
    except:
        pass
    try:
        converted = float(value)
        return 'REAL'
    except:
        pass
    try:
        converted = datetime.strptime(value, '%Y-%m-%d')
        return 'DATE/TIME'
    except:
        pass
    try:
        converted = datetime.strptime(value, '%m/%d/%Y')
        return 'DATE/TIME'
    except:
        pass
    try:
        converted = datetime.strptime(value, '%m/%Y')
        return 'DATE/TIME'
    except:
        pass
    try:
        converted = datetime.strptime(value, '%Y.%m')
        return 'DATE/TIME'
    except:
        pass
    try:
        converted = datetime.strptime(value, '%Y.%m.%d')
        return 'DATE/TIME'
    except:
        pass
    return 'TEXT'


def parse_datetime(str):
    converted = None
    try:
        converted = datetime.strptime(str, '%Y%m')
    except:
        pass
    try:
        converted = datetime.strptime(str, '%Y-%m-%d')
    except:
        pass
    try:
        converted = datetime.strptime(str, '%m/%d/%Y')
    except:
        pass
    try:
        converted = datetime.strptime(str, '%m/%Y')
    except:
        pass
    try:
        converted = datetime.strptime(str, '%Y.%m')
    except:
        pass
    try:
        converted = datetime.strptime(str, '%Y.%m.%d')
    except:
        pass
    return converted


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task1 <input-csv>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder \
                        .appName("big-data project") \
                        .getOrCreate()
    input = spark.read.csv(sys.argv[1], sep='\t', header=True, multiLine=True)
    headers = input.columns
    data = input.rdd
    result = {'dataset_name': sys.argv[1].split('/')[-1]}
    columns = []
    for i in range(len(headers)):
        columnName = headers[i]
        values = data.map(lambda x: x[columnName])
        nonEmptyNum = values.filter(lambda x: x != None).count()
        EmptyNum = values.filter(lambda x: x == None).count()
        distinctNum = values.distinct().count()
        topFiveFrequent = values.map(lambda x: (x, 1)) \
                                .reduceByKey(lambda v1, v2: v1 + v2) \
                                .sortBy(lambda x: -x[1]) \
                                .map(lambda x: x[0]) \
                                .take(5)
        values = values.filter(lambda x: x != None)
        dataTypes = values.map(lambda x: identify_type(x)).distinct().collect()
        column = {
                    'column_name': columnName,
                    'number_non_empty_cells': nonEmptyNum,
                    'number_empty_cells': EmptyNum,
                    'number_distinct_values': distinctNum,
                    'frequent_values': topFiveFrequent
                 }
        dataTypesObj = []
        if 'INTEGER' in dataTypes:
            integers = values.filter(lambda x: identify_type(x) == 'INTEGER') \
                             .map(lambda x: int(x))
            dataType = {
                          'type': 'INTEGER (LONG)',
                          'count': integers.count(),
                          'max_value': integers.max(),
                          'min_value': integers.min(),
                          'mean': integers.mean(),
                          'stddev': integers.stdev()
                       }
            dataTypesObj.append(dataType)
        if 'REAL' in dataTypes:
            reals = values.filter(lambda x: identify_type(x) == 'REAL') \
                          .map(lambda x: float(x))
            dataType = {
                          'type': 'REAL',
                          'count': reals.count(),
                          'max_value': reals.max(),
                          'min_value': reals.min(),
                          'mean': reals.mean(),
                          'stddev': reals.stdev()
                       }
            dataTypesObj.append(dataType)
        if 'DATE/TIME' in dataTypes:
            datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME') \
                              .map(lambda x: parse_datetime(x))
            dataType = {
                          'type': 'DATE/TIME',
                          'count': datetimes.count(),
                          'max_value': str(datetimes.max()),
                          'min_value': str(datetimes.min())
                       }
            dataTypesObj.append(dataType)
        if 'TEXT' in dataTypes:
            texts = values.filter(lambda x: identify_type(x) == 'TEXT') \
                          .distinct() \
                          .map(lambda x: (x, len(x)))
            fiveLongest = texts.sortBy(lambda x: -x[1]) \
                               .map(lambda x: x[0]) \
                               .take(5)
            fiveShortest = texts.sortBy(lambda x: x[1]) \
                                .map(lambda x: x[0]) \
                                .take(5)
            averageLength = texts.map(lambda x: x[1]).mean()
            dataType = {
                          'type': 'TEXT',
                          'count': texts.count(),
                          'shortest_values': fiveShortest,
                          'longest_values': fiveLongest,
                          'average_length': averageLength
                       }
            dataTypesObj.append(dataType)
        column['data_types'] = dataTypesObj
        columns.append(column)
    result['columns'] = columns

    rdd = spark.sparkContext.parallelize([json.dumps(result)])
    rdd.saveAsTextFile(sys.argv[1].split('/')[-1] + '.json')
