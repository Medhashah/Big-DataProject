from __future__ import print_function

import json
import sys

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: task3", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    file = sc.textFile("/user/hm74/NYCOpenData/datasets.tsv", 1)
    file_list = file.map(lambda x: x.split("\t")).map(lambda x: x[0]).map(lambda x: x + ".tsv.gz").collect()
    for k in range(752, 760):
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
        column = headers[16]
        values = data.map(lambda x: ((x[16], x[5]), 1))
        dataTypes = values.filter(lambda x: x[0][0] == 'BROOKLYN' \
                                            or x[0][0] == 'NEW YORK' \
                                            or x[0][0] == 'QUEENS' \
                                            or x[0][0] == 'BRONX' \
                                            or x[0][0] == 'STATEN ISLAND') \
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda x: -x[1]) \
            .collect()
        BROOKLYN = []
        NEWYORK = []
        QUEENS = []
        BRONX = []
        STATEN = []
        for item in dataTypes:
            if (item[0][0] == 'BROOKLYN'):
                if (len(BROOKLYN) == 3):
                    continue
                BROOKLYN.append(item)
            if (item[0][0] == 'NEW YORK'):
                if (len(NEWYORK) == 3):
                    continue
                NEWYORK.append(item)
            if (item[0][0] == 'QUEENS'):
                if (len(QUEENS) == 3):
                    continue
                QUEENS.append(item)
            if (item[0][0] == 'BRONX'):
                if (len(BRONX) == 3):
                    continue
                BRONX.append(item)
            if (item[0][0] == 'STATEN ISLAND'):
                if (len(STATEN) == 3):
                    continue
                STATEN.append(item)
        dict = {}
        dict['BROOKLYN'] = BROOKLYN
        dict['NEWYORK'] = NEWYORK
        dict['QUEENS'] = QUEENS
        dict['BRONX'] = BRONX
        dict['STATEN'] = STATEN
        year = 2004 + k - 752
        with open("./task3/" + str(year) + ".json", "w") as fp:
            json.dump(dict, fp)
    sc.stop()
