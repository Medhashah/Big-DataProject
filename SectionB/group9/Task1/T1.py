# -*- coding: UTF-8 -*-
import json
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime


def determineType(x):
    try:
        someType = datetime.strptime(x, '%Y-%m-%d')
        return 'DATE/TIME'
    except:
        pass

    try:
        someType = int(x)
        return 'INTEGER/LONG'
    except:
        pass

    try:
        someType = float(x)
        return 'REAL'
    except:
        pass

    return 'TEXT'


def isNull(x):
    return x == '' or x is None


def isNotNull(x):
    return x != '' or x is None


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


def profile(dataset):
    output = dict()
    rawData = spark.read.options(header='true', inferschema='true', delimiter='\t').csv(
        dataRootPath + dataset + ".tsv.gz", multiLine=True)
    print("Start profiling: %s" % dataset)
    output['dataset_name'] = dataset
    output['columns'] = list()
    output['key_column_candidates'] = list()
    headers = rawData.columns
    i = 0
    all = len(headers)
    for column in headers:
        columnDict = dict()
        columnPreprocessed = column.replace(".", "").replace("`", "")
        data = rawData.withColumnRenamed(column, columnPreprocessed)
        columnDict['column_name'] = column
        try:
            colData = data.select(columnPreprocessed).rdd.map(lambda x: str(x[0])).cache()
        except:
            colData = data.rdd.map(lambda x: str(x[i])).cache()

        emptyCells = colData.filter(lambda x: isNull(x)).count()
        nonEmptyCells = colData.filter(lambda x: isNotNull(x)).count()
        distinctValues = colData.distinct().count()
        topFiveFreqValues = colData.map(lambda x: (x, 1)) \
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda x: x[1], False) \
            .map(lambda x: x[0]) \
            .take(5)
        types = colData.map(lambda x: determineType(x)).distinct().collect()
        dataTypes = list(dict())

        if 'INTEGER/LONG' in types:
            selfDict = dict()
            values = colData.filter(lambda x: determineType(x) == 'INTEGER/LONG').map(
                lambda x: int(x.replace(",", ""))).cache()
            selfDict['type'] = 'INTEGER (LONG)'
            selfDict['count'] = values.count()

            # need null check otherwise json file will have NaN
            max = values.max()
            min = values.min()
            mean = values.mean()
            stdev = values.stdev()
            selfDict['max_value'] = max if max is not None else ""
            selfDict['min_value'] = min if min is not None else ""
            selfDict['mean'] = mean if mean is not None else ""
            selfDict['stddev'] = stdev if stdev is not None else ""
            dataTypes.append(selfDict)

        if 'REAL' in types:
            selfDict = dict()
            values = colData.filter(lambda x: determineType(x) == 'REAL').map(lambda x: float(x)).cache()

            selfDict['count'] = values.count()
            max = values.max()
            min = values.min()
            mean = values.mean()
            stdev = values.stdev()
            selfDict['type'] = 'REAL'

            selfDict['max_value'] = max if max is not None else ""
            selfDict['min_value'] = min if min is not None else ""
            selfDict['mean'] = mean if mean is not None else ""
            selfDict['stddev'] = stdev if stdev is not None else ""
            dataTypes.append(selfDict)

        if 'DATE/TIME' in types:
            selfDict = dict()
            values = colData.filter(lambda x: determineType(x) == 'DATE/TIME') \
                .map(lambda x: datetime.strptime(x, '%Y-%m-%d'))
            selfDict['type'] = 'DATE/TIME'
            selfDict['count'] = values.count()
            max = values.max()
            min = values.min()

            selfDict['max_value'] = max if max is not None else ""
            selfDict['min_value'] = min if min is not None else ""
            dataTypes.append(selfDict)

        if 'TEXT' in types:
            selfDict = dict()
            values = colData.filter(lambda x: determineType(x) == 'TEXT').map(lambda x: x.encode('utf-8')).cache()
            selfDict['type'] = 'TEXT'
            count = values.count()
            selfDict['count'] = count
            selfDict['shortest_values'] = colData.distinct().takeOrdered(5, key=lambda x: (len(x), x))
            selfDict['longest_values'] = colData.distinct().takeOrdered(5, key=lambda x: (-len(x), x))
            totalLength = colData.map(lambda x: len(x)).sum()
            selfDict["average_length"] = float(totalLength) / float(count) if count > 0 else 0
            dataTypes.append(selfDict)

        if 'DATE/TIME' not in types:
            if distinctValues == emptyCells + nonEmptyCells:
                output['key_column_candidates'].append(column)

        columnDict['number_non_empty_cells'] = nonEmptyCells
        columnDict['number_empty_cells'] = emptyCells
        columnDict['number_distinct_values'] = distinctValues
        columnDict['frequent_values'] = topFiveFreqValues
        columnDict['data_types'] = dataTypes
        output['columns'].append(columnDict)
        print("{0} is done...{1}".format(i, all))
        i += 1

    with open('./T1data/%s.json' % dataset, 'w') as f:
        json.dump(output, f, cls=JsonEncoder)

    print("%s is finished" % dataset)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("task1_final").config("spark.some.config.option", "some-value").getOrCreate()
    dataSetsPath = "/user/hm74/NYCOpenData/datasets.tsv"
    datasets = spark.read.options(header=True, inferschema=True, delimiter='\t').csv(dataSetsPath).rdd.map(lambda x: x[0]).collect()
    dataRootPath = "/user/hm74/NYCOpenData/"
    resPath = "/home/yw4002/finalpj/T1data/"
    big = []
    for i in range(0, len(datasets)):
        if datasets[i] in big:
            continue
        if not os.path.exists(resPath + datasets[i] + ".json"):
            profile(datasets[i])
        else:
            print("%s already done." % datasets[i])
