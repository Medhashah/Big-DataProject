from __future__ import print_function

import sys
import json
from pyspark import SparkContext
from datetime import datetime


def identify_type(value):
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
        return 'DATE/TIME/1'
    except:
        pass

    try:
        converted = datetime.strptime(value, '%m-%d-%Y')
        return 'DATE/TIME/2'
    except:
        pass

    try:
        converted = datetime.strptime(value, '%m/%d/%YT%H:%M:%S')
        return 'DATE/TIME/3'
    except:
        pass

    try:
        converted = datetime.strptime(value, '%m/%d/%Y %H:%M:%S')
        return 'DATE/TIME/6'
    except:
        pass

    try:
        converted = datetime.strptime(value, '%m/%d/%Y %H:%M:%S %p')
        return 'DATE/TIME/7'
    except:
        pass

    try:
        converted = datetime.strptime(value, '%m/%d/%YT%H:%M:%S %p')
        return 'DATE/TIME/8'
    except:
        pass

    try:
        converted = datetime.strptime(value, '%H::%M::%S')
        return 'DATE/TIME/4'
    except:
        pass

    try:
        converted = datetime.strptime(value, '%H::%M')
        return 'DATE/TIME/5'
    except:
        pass
    return 'TEXT'


if __name__ == "__main__":
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
    if len(sys.argv) != 1:
        print("Usage: task1", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    file = sc.textFile("/user/hm74/NYCOpenData/datasets.tsv", 1)
    file_list = file.map(lambda x: x.split("\t")).map(lambda x: x[0]).map(lambda x: x + ".tsv.gz").collect()
    for k in range(0, 1900):
        item = file_list[k]
        input = sc.textFile('/user/hm74/NYCOpenData/' + item, 1)
        headers = input.first().split('\t')
        data = input.zipWithIndex().filter(lambda tup: tup[1] > 1) \
            .map(lambda x: x[0]) \
            .map(lambda x: x.split('\t')) \
            .filter(lambda x: len(x) == len(headers)) \
            .cache()
        result = sc.parallelize([])
        list = []
        for i in range(len(headers)):
            column = headers[i]
            values = data.map(lambda x: x[i])
            dataTypes = values.map(lambda x: identify_type(x)).distinct().collect()

            nonEmptyNum = values.filter(lambda x: x.strip() != '').count()
            EmptyNum = values.filter(lambda x: x.strip() == '').count()
            distinctNum = values.distinct().count()
            topFiveFrequent = values.map(lambda x: (x, 1)) \
                .reduceByKey(lambda v1, v2: v1 + v2) \
                .sortBy(lambda x: -x[1]) \
                .map(lambda x: x[0]) \
                .take(5)

            values = values.filter(lambda x: x.strip() != '')
            extra = {}

            if 'INTEGER' in dataTypes or 'REAL' in dataTypes:
                integers = values.filter(lambda x: identify_type(x) == 'INTEGER' or identify_type(x) == 'REAL') \
                    .map(lambda x: float(x))
                if 'INTEGER' in dataTypes:
                    extra['type'] = "INTEGER (LONG)"
                else:
                    extra['type'] = "REAL"
                extra['count'] = values.count()
                extra['max_value'] = integers.max()
                extra['min_value'] = integers.min()
                extra['mean'] = integers.mean()
                extra['stddev'] = integers.stdev()
            if 'DATE/TIME' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME') \
                    .map(lambda x: datetime.strptime(x, '%Y-%m-%d'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/1' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/1') \
                    .map(lambda x: datetime.strptime(x, '%m/%d/%Y'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/2' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/2') \
                    .map(lambda x: datetime.strptime(x, '%m-%d-%Y'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/3' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/3') \
                    .map(lambda x: datetime.strptime(x, '%m/%d/%YT%H:%M:%S'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/6' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/6') \
                    .map(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/4' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/4') \
                    .map(lambda x: datetime.strptime(x, '%H::%M::%S'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/5' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/5') \
                    .map(lambda x: datetime.strptime(x, '%H::%M'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/7' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/7') \
                    .map(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S %p'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'DATE/TIME/8' in dataTypes:
                datetimes = values.filter(lambda x: identify_type(x) == 'DATE/TIME/8') \
                    .map(lambda x: datetime.strptime(x, '%m/%d/%YT%H:%M:%S %p'))
                extra['type'] = "DATE/TIME"
                extra['count'] = values.count()
                extra['max_date'] = str(datetimes.max())
                extra['min_date'] = str(datetimes.min())
            if 'TEXT' in dataTypes:
                texts = values.filter(lambda x: identify_type(x) == 'TEXT') \
                    .distinct() \
                    .map(lambda x: (x, len(x)))
                total_length = texts.values().sum()

                fiveLongest = texts.sortBy(lambda x: -x[1]) \
                    .map(lambda x: x[0]) \
                    .take(5)
                fiveShortest = texts.sortBy(lambda x: x[1]) \
                    .map(lambda x: x[0]) \
                    .take(5)
                if values.count() != 0:
                    averageLength = total_length / values.count()
                else:
                    averageLength = 0
                extra['type'] = "TEXT"
                extra['count'] = values.count()
                extra['shortest_values'] = fiveShortest
                extra['longest_values'] = fiveLongest
                extra['average_length'] = averageLength
            sub_dict = {
                "column_name": column,
                "number_non_empty_cells": nonEmptyNum,
                "number_empty_cells": EmptyNum,
                "number_distinct_values": distinctNum,
                "frequent_values": topFiveFrequent,
                "data_types": str(extra)
            }
            list.append(sub_dict)
        if (len(list) != 0):
            dict = {}
            dict["dataset_name"] = item
            dict["columns"] = list
            with open("./task1/" + item + ".json", "w") as fp:
                json.dump(dict, fp)
        else:
            print(k)
    sc.stop()
