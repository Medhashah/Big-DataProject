# -*- coding: UTF-8 -*-
import datetime
import json
import os
import re

from dateutil.parser import *
from pyspark.sql import SparkSession

MIN_SIZE = 500000


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)


def type_int(x):
    int_pattern = re.compile(r'^\d+$')
    if int_pattern.match(x.replace(",", "")):
        try:
            x = int(x.replace(",", ""))
            if x > 2147483647 or x < -2147483648:
                return False
            return True
        except:
            return False
    return False


def type_float(x):
    float_pattern = re.compile(r'^\d+\.\d*$')
    if float_pattern.match(x.replace(",", "")):
        try:
            float(x.replace(",", ""))
            return True
        except:
            return False
    return False


def type_str(x):
    if type_int(x) or type_float(x):
        return False
    if len(x) < 6:
        return True
    try:
        tmp = parse(x)
        if tmp.year < 2020 and tmp.year > 1990:
            return False
        else:
            return True
    except:
        return True


def get_type(x):
    if x is None or x == "":
        return ("NONE", (1, 0, 0, 0))
    elif type_int(x):
        tmp = int(x.replace(",", ""))
        return ("INTEGER", (1, tmp, tmp, tmp))
    elif type_float(x):
        tmp = float(x.replace(",", ""))
        return ("REAL", (1, tmp, tmp, tmp))
    if len(x) >= 6:
        try:
            tmp = parse(x)
            if tmp.year < 2020 and tmp.year > 1990 and tmp.hour < 24 and tmp.hour >= 0:
                return ("DATE/TIME", (1, tmp, tmp, 0))
        except:
            pass
    return ("TEXT", (1, 0, 0, len(x)))


def reduce_key(a, b):
    # [list of values], count, min, max, sum
    # value_list = a[0] + b[0]
    count = a[0] + b[0]
    try:
        if isinstance(a[1], datetime.datetime):
            if a[1].timestamp() < b[1].timestamp():
                min_value = a[1]
            else:
                min_value = b[1]
        else:
            min_value = min(a[1], b[1])
        if isinstance(a[2], datetime.datetime):
            if a[1].timestamp() > b[2].timestamp():
                max_value = a[2]
            else:
                max_value = b[2]
        else:
            max_value = min(a[2], b[2])
    except:
        min_value = a[1]
        max_value = a[2]
    sum = a[3] + b[3]
    return (count, min_value, max_value, sum)


def profile(dataset):
    print("%s start processing................................." % dataset)
    output = dict()
    output["dataset_name"] = dataset
    output["columns"] = []
    output["key_column_candidates"] = []
    dataset_df = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(
        data_dir + dataset + ".tsv.gz").cache()
    print("%s data load ok" % dataset)
    df_count = dataset_df.count()
    print("%s has %d rows" % (dataset, df_count))
    if df_count == 0:
        print("%s has no data" % dataset)
        return df_count
    if df_count > MIN_SIZE:
        print("%s is a large dataset skip now" % dataset)
        return df_count
    columns = dataset_df.columns
    print("%s has %d columns" % (dataset, len(columns)))
    for column_name in columns:
        # data init
        column = dict()
        column["data_types"] = []
        # start column
        valid_column_name = column_name.replace(".", "").replace("`", "")
        dataset_df = dataset_df.withColumnRenamed(column_name, valid_column_name)
        print("start column %s" % column_name)
        # get col
        col_rdd = dataset_df.select(valid_column_name.replace(".", "")).rdd.map(lambda x: str(x[0])).cache()
        col_basic_rdd = col_rdd.map(get_type).reduceByKey(reduce_key).cache()
        # get col stat
        number_empty_cells = col_basic_rdd.filter(lambda x: x[0] == "NONE").map(lambda x: x[1][0]).collect()
        number_empty_cells = number_empty_cells[0] if len(number_empty_cells) > 0 else 0
        number_non_empty_cells = col_basic_rdd.filter(lambda x: x[0] != "NONE").map(lambda x: x[1][0]).reduce(
            lambda a, b: a + b)
        number_distinct_values = col_rdd.distinct().count()
        frequent_values = col_rdd.map(lambda x: (x, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda x: -x[1]) \
            .map(lambda x: x[0]) \
            .take(5)
        column["column_name"] = column_name
        column["number_non_empty_cells"] = number_non_empty_cells
        column["number_empty_cells"] = number_empty_cells
        column["number_distinct_values"] = number_distinct_values
        column["frequent_values"] = frequent_values
        # INTEGER type
        int_rdd = col_basic_rdd.filter(lambda x: x[0] == 'INTEGER').map(lambda x: x[1])
        if int_rdd.count() > 0:
            data_type = dict()
            data_type["type"] = "INTEGER"
            count = int_rdd.map(lambda x: x[0]).collect()[0]
            if col_basic_rdd.count() == 1 and number_empty_cells == 0 and number_distinct_values == count:
                output["key_column_candidates"].append(column_name)
            min_value = int_rdd.map(lambda x: x[1]).collect()[0]
            max_value = int_rdd.map(lambda x: x[2]).collect()[0]
            mean = int_rdd.map(lambda x: float(x[3]) / float(x[0])).collect()[0]
            std = col_rdd.filter(type_int).map(lambda x: int(x.replace(",", ""))).stdev()
            data_type["count"] = count
            data_type["min_value"] = min_value
            data_type["max_value"] = max_value
            data_type["mean"] = mean
            data_type['stddev'] = std
            column["data_types"].append(data_type)
        # REAL type
        real_rdd = col_basic_rdd.filter(lambda x: x[0] == "REAL").map(lambda x: x[1])
        if real_rdd.count() > 0:
            data_type = dict()
            data_type["type"] = "REAL"
            count = real_rdd.map(lambda x: x[0]).collect()[0]
            min_value = real_rdd.map(lambda x: x[1]).collect()[0]
            max_value = real_rdd.map(lambda x: x[2]).collect()[0]
            mean = real_rdd.map(lambda x: float(x[3]) / float(x[0])).collect()[0]
            std = col_rdd.filter(type_float).map(lambda x: float(x.replace(",", ""))).stdev()
            data_type["count"] = count
            data_type["min_value"] = min_value
            data_type["max_value"] = max_value
            data_type["mean"] = mean
            data_type["stddev"] = std
            column["data_types"].append(data_type)
        # DATE/TIME type
        date_rdd = col_basic_rdd.filter(lambda x: x[0] == "DATE/TIME").map(lambda x: x[1])
        if date_rdd.count() > 0:
            data_type = dict()
            data_type["type"] = "DATE/TIME"
            count = date_rdd.map(lambda x: x[0]).collect()
            min_value = date_rdd.map(lambda x: x[1]).collect()
            max_value = date_rdd.map(lambda x: x[2]).collect()
            data_type["count"] = count
            data_type["min_value"] = str(min_value)
            data_type["max_value"] = str(max_value)
            column["data_types"].append(data_type)
        # TEXT type
        text_rdd = col_basic_rdd.filter(lambda x: x[0] == "TEXT").map(lambda x: x[1])
        if text_rdd.count() > 0:
            data_type = dict()
            data_type["type"] = "TEXT"
            count = text_rdd.map(lambda x: x[0]).collect()[0]
            if col_basic_rdd.count() == 1 and number_non_empty_cells == 0 and count == number_distinct_values:
                output["key_column_candidates"].append(column_name)
            mean = text_rdd.map(lambda x: float(x[3]) / float(x[0])).collect()[0]
            data_type["count"] = count
            data_type["average_length"] = mean
            all_text_rdd = col_rdd.filter(type_str).cache()
            data_type["shortest_values"] = all_text_rdd.distinct() \
                .takeOrdered(5, key=lambda x: (len(x), x))
            data_type["longest_values"] = all_text_rdd.distinct() \
                .takeOrdered(5, key=lambda x: (-len(x), x))
            column["data_types"].append(data_type)
        # Output result
        output["columns"].append(column)
        print("column %s ok" % column_name)
    with open("./task1_data/%s.json" % dataset, 'w+') as fp:
        json.dump(output, fp, cls=MyEncoder)
    print("%s processed OK" % dataset)
    return df_count


if __name__ == "__main__":
    # init
    spark = SparkSession \
        .builder \
        .appName("bigdata_project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # get file and dir
    file = "/user/hm74/NYCOpenData/datasets.tsv"
    data_dir = file[:file.rfind("/") + 1]
    data_sets = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(file).rdd.map(
        lambda x: x[0]).collect()
    # create result dir
    mkdir("./task1_data")
    # run profile for each dataset
    user = 'yp1207'
    directory = 'project_pycharm'
    my_dir = '/home/%s/%s/task1_data/' % (user, directory)
    # load dataset size
    size_dict = dict()
    if os.path.exists("./dataset_attr.txt"):
        with open("./dataset_attr.txt", "r") as size_file:
            for line in size_file:
                if len(line.split(",")) == 2 and "error" not in line.split(",")[1]:
                    size_dict[line.split(",")[0]] = int(line.split(",")[1])
                else:
                    size_dict[line.split(",")[0]] = "error"
    # run dataset
    has_not_done = True
    # break into 3 parts for 3 members to run
    part = len(data_sets) // 3
    part1 = data_sets[: part]
    part2 = data_sets[part: part * 2]
    part3 = data_sets[part * 2:]
    while has_not_done:
        not_done = 0
        for dataset in data_sets:
            with open("./dataset_attr.txt", 'a') as attr_file:
                if not os.path.exists(my_dir + dataset + ".json"):
                    not_done += 1
                    if dataset in size_dict and size_dict[dataset] == 'error':
                        print("%s has error\n" % dataset)
                        continue
                    try:
                        if dataset in size_dict and size_dict[dataset] > MIN_SIZE:
                            continue
                        count = profile(dataset)
                        if dataset not in size_dict:
                            size_dict[dataset] = count
                            attr_file.write("%s,%s\n" % (dataset, count))
                    except:
                        attr_file.write("%s,error\n" % dataset)
                        print("%s has error\n" % dataset)
                else:
                    if dataset not in size_dict:
                        dataset_df = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(
                            data_dir + dataset + ".tsv.gz")
                        df_count = dataset_df.count()
                        attr_file.write("%s,%s\n" % (dataset, df_count))
                        print("%s has %s rows" % (dataset, df_count))
                    print("%s already processed" % dataset)
        if not_done == 0:
            has_not_done = False
        MIN_SIZE += 500000
