from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col
from csv import reader
from pyspark.sql.functions import desc
from dateutil.parser import parse
import datetime
import pytz
import time
import re


def is_int(string):
    try:
        if isinstance(string, int):
            return True
        int(string)
        return True
    except:
        return False


def is_real(string):
    try:
        if isinstance(string, float):
            return True
        float(string)
        return True
    except:
        return False


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.
    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        if isinstance(string, datetime.datetime):
            return True
        pytz.utc.localize(parse(string, fuzzy=fuzzy)) > datetime.datetime.now(datetime.timezone.utc)
        return True

    except:
        return False


NULL = "NULL"
INTEGER = "INTEGER (LONG)"
REAL = "REAL"
DATE = "DATE/TIME"
TEXT = "TEXT"
tablename = 'table'

# typeChecker takes a row as input
# ROW._value is unique for the column
# row._count is how many row._value does the column has
# return is of format: (TYPE, (VALUE, COUNT, TYPE))
def typeChecker(row):
    val = row._value
    count = row._count
    if val is None:
        return (NULL, (None, count, NULL))
    elif is_int(val):
        return (INTEGER, (int(val), count, INTEGER))
    elif is_real(val):
        return (REAL, (float(val), count, REAL))
    elif is_date(val) and not (str(val).count('-') == 1 or str(val).count('/') == 1):
        copied = val
        if isinstance(val, datetime.datetime):
            return (DATE, ((val, copied), count, DATE))
        return (DATE, ((parse(val), copied), count, DATE))
    else:
        return (TEXT, (val, count, TEXT))

# val_count: is the value return by typeChecker
# based on the TYPE, return different output
# this output will be used later in
#   mergeVal
#   mergeTuple
#   calculate the detailed stats of a column
def createTuple(val_count):
    val, count, row_type = val_count
    if row_type == INTEGER:
        # (type, count, sum, sum**2, min, max)
        return (row_type, count, val * count, val * val * count, val, val)
    elif row_type == REAL:
        # (type, count, sum, sum**2, min, max)
        return (row_type, count, val * count, val * val * count, val, val)
    elif row_type == DATE:
        # (type, count, min, max)
        return (row_type, count, val, val)
    elif row_type == TEXT:
        t_shortest = [val]
        t_longest = [val]
        # (type, count, shortest_five, longest_five, total_length)
        return (row_type, count, t_shortest, t_longest, len(val) * count)
    else:
        # NULL
        return (row_type)


def length(x):
    return len(x)

# r_tuple: is the value returned by typeChecker
# l: is the value returned by createTuple
# this output will be used later in
#   mergeTuple
#   calculate the detailed stats of a column
def mergeVal(l, r_tuple):
    r_val, r_count, r_type = r_tuple
    if r_type == INTEGER:
        l_type, l_count, l_sum, l_sum_square, l_min, l_max = l
        t_sum = l_sum + r_val * r_count
        t_sum_square = l_sum_square + r_val * r_val * r_count
        t_min = min(l_min, r_val)
        t_max = max(l_max, r_val)
        t_count = l_count + r_count
        # (type, count, sum, sum**2, min, max)
        return (l_type, t_count, t_sum, t_sum_square, t_min, t_max)
    elif r_type == REAL:
        l_type, l_count, l_sum, l_sum_square, l_min, l_max = l
        t_sum = l_sum + r_val * r_count
        t_sum_square = l_sum_square + r_val * r_val * r_count
        t_min = min(l_min, r_val)
        t_max = max(l_max, r_val)
        t_count = l_count + r_count
        # (type, count, sum, sum**2, min, max)
        return (l_type, t_count, t_sum, t_sum_square, t_min, t_max)
    elif r_type == DATE:
        l_type, l_count, l_min, l_max = l
        t_min = l_min
        t_max = l_max
        if l_min[0] > r_val[0]:
            t_min = r_val
        if l_max[0] < r_val[0]:
            t_max = r_val
        # (type, count, min, max)
        return (r_type, l_count + r_count, t_min, t_max)
    elif r_type == "TEXT":
        # (type, count, shortest_five, longest_five, total_length)
        l_type, l_count, l_shortest_five, l_longest_five, l_total_length = l
        l_shortest_five.append(r_val)
        l_longest_five.append(r_val)
        l_shortest_five.sort(key=length)
        l_longest_five.sort(key=length, reverse=True)
        if len(l_shortest_five) > 5:
            l_shortest_five.pop()
            l_longest_five.pop()
        return (r_type, l_count + r_count, l_shortest_five, l_longest_five, l_total_length + r_count * len(r_val))
    else:
        return (r_type)

# l and r both are the values 
# returned by createTuple or MergeVal
def mergeTuple(l, r):
    tuple_type = l[0]
    if tuple_type == INTEGER:
        # (type, count, sum, sum**2, min, max)
        return (tuple_type, l[1] + r[1], l[2] + r[2], l[3] + r[3], min(l[4], r[4]), max(l[5], r[5]))
    elif tuple_type == REAL:
        # (type, count, sum, sum**2, min, max)
        return (tuple_type, l[1] + r[1], l[2] + r[2], l[3] + r[3], min(l[4], r[4]), max(l[5], r[5]))
    if tuple_type == DATE:
        # (type, count, min, max)
        t_min = l[2]
        t_max = l[3]
        if t_min[0] > r[2][0]:
            t_min = r[2]
        if t_max[0] < r[3][0]:
            t_max = r[3]
        return (tuple_type, l[1] + r[1], t_min, t_max)
    if tuple_type == "TEXT":
        # (type, count, shortest_five, longest_five, total_length)
        t_shortest_five = l[2] + r[2]
        t_longest_five = l[3] + r[3]
        t_shortest_five.sort(key=length)
        t_longest_five.sort(key=length, reverse=True)
        while len(t_shortest_five) > 5:
            t_shortest_five.pop()
            t_longest_five.pop()
        return (tuple_type, l[1] + r[1], t_shortest_five, t_longest_five, l[4] + r[4])
    else:
        return (tuple_type)


if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession \
        .builder \
        .appName("final-project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    s_min = int(sys.argv[1])
    s_max = int(sys.argv[2])
    # load table into database
    # Setup
    filepath = 'dataset_index.txt'
    with open(filepath) as fp:
        line = fp.readline()
        while line:
            try:
                start_time = time.time()
                arr = line.split(",")
                index = int(arr[0].strip())
                if index < int(s_min) or index > int(s_max):
                    line = fp.readline()
                    continue
                argv1 = arr[1].replace("'","").strip()
                argv2 = arr[2].replace("'","").strip()
                line = fp.readline()
                table = spark.read.format('csv')\
                    .options(delimiter="\t", header='true', inferschema='true')\
                    .load(argv1)
                # create table
                table.createOrReplaceTempView(tablename)
                # the name of the dataset correlating to the tsv file
                filename = argv2
                inFile = argv1.split('.', 1)[0]
                print("profiling file:" + filename)

                columns = {}  # a list of columns
                key_column_candidates = []  # all the columns that could be primary key candidates

                # go through the table and for each column, create the column_specification content
                df = spark.sql("select * from %s" % (tablename))
                cols = df.columns  # array of all column names
                # total_row, which is used to calculate empty cells
                total_row = df.count()
                print("there are " + str(len(cols)) + " columns in this table") 
                for raw_col in cols:  # for each column
                    # sanitize colname
                    col = '`' + raw_col + '`'
                    print("started one col")
                    # dataframe that is not empty
                    #column_df = \
                    #    spark.sql('SELECT `%s` as _value FROM `%s` WHERE `%s` is NOT NULL'
                    #              % (col, tablename, col))
                    column_df = df.filter(df[col].isNotNull())
                    column = {}
                    ##########################################################################
                    column['column_name'] = raw_col  # the name of the column
                    """ 1.1 DONE: get the number of non empty cells (type: integer) """
                    column['number_non_empty_cells'] = column_df.count()
                    print("1.1")
                    """ 1.2 DONE: get the numebr of empty cells (type: integer) """
                    column['number_empty_cells'] = total_row - \
                        column['number_non_empty_cells']

                    """ 1.3 DONE: number of distinct values in the column (type: integer) """
                    #distinct_df = \
                    #    spark.sql('SELECT `%s` as _value, count(*) as _count \
                    #               FROM `%s`                                 \
                    #               GROUP BY _value' % (col, tablename))
                    distinct_df = df.groupBy(col).count().toDF('_value','_count')
                    num_distinct_count = distinct_df.count()
                    column['number_distinct_values'] = num_distinct_count
                    print("1.3")
                    """ 1.6 Extra Credit TODO: For each table T, indicate columns that are candidates for being keys of T"""
                    # only columns that have #unique val == total val can be a Key Candidate
                    if num_distinct_count == total_row:
                        key_column_candidates.append(col)

                    """ 1.4 DONE: top 5 most frequent values of this column(type: array) """
                    # may need to get rid of the NULL?
                    #most_frequent = \
                    #    spark.sql('SELECT `%s` as _value, count(*) FROM `%s` \
                    #                GROUP BY `%s`                            \
                    #                ORDER BY count(*) DESC                   \
                    #                LIMIT 5' % (col, tablename, col))        \
                    #    .collect()
                    most_frequent = distinct_df.sort(desc("_count")).take(5)
                    column['frequent_values'] = [row._value for row in most_frequent]

                    """ 1.5 DONE: for every data type that this column have, output the required values in data_types """
                    # detailed_distinct_df is of format: 
                    # (TYPE, (VALUE, COUNT, TYPE))
                    # key: TYPE
                    # value: (VALUE, COUNT, TYPE)
                    detailed_distinct_df = distinct_df.rdd.map(lambda s: typeChecker(s))
                    print("1.5")
                    # detailed_distinct_stats = [(TYPE, (DETAILED_STATS))]
                    # A list of detailed stats of all the datatypes this column has
                    detailed_distinct_stats = detailed_distinct_df \
                        .combineByKey(createTuple, mergeVal, mergeTuple) \
                        .collect()

                    data_types = []
                    for a_type_stats in detailed_distinct_stats:
                        row_type = a_type_stats[0]
                        data = a_type_stats[1]
                        if row_type == NULL:
                            continue
                        json_res = {}
                        json_res["type"] = row_type
                        json_res["count"] = data[1]
                        if row_type == INTEGER or row_type == REAL:
                            # (type, count, sum, sum**2, min, max)
                            json_res["max_value"] = data[5]
                            json_res["min_value"] = data[4]
                            n = data[1]
                            sumX = data[2]
                            mean = sumX / n
                            sumSqaured = data[3]
                            stddev = ((sumSqaured - n*mean*mean)/n) ** 0.5
                            json_res["mean"] = mean
                            json_res["stddev"] = stddev
                        elif row_type == DATE:
                            # (type, count, min, max)
                            json_res["max_value"] = str(data[3][1])
                            json_res["min_value"] = str(data[2][1])
                        elif row_type == TEXT:
                            # (type, count, shortest_five, longest_five, total_length)
                            json_res["shortest_values"] = data[2]
                            json_res["longest_values"] = data[3]
                            json_res["average_length"] = data[4] / data[1]
                        data_types.append(json_res)
                    column['data_types'] = data_types
                    columns[raw_col] = column

                # assembling the json file
                data = {}  # the base for the json file
                # assign dataset_name value to the json file
                data['dataset_name'] = filename
                data['columns'] = columns  # assign columns value to the json file
                # assign key_column_candidates to the json file
                data['key_column_candidates'] = key_column_candidates
                data['time_elapsed'] = time.time() - start_time
                print("time to run this file is:")
                print(time.time() - start_time)
                # write the json file to output, 
                # name the output file as inFile.json, 
                # inFile being the name of the file
                output_file = inFile.split("/")[-1]+".json"
                output_dir = 'raw_output/'
                with open(output_dir + output_file, 'w') as outfile:
                    json.dump(data, outfile, default=str)
            except Exception as e:
                print("there was an error")
                print(str(e).encode('utf-8'))
                line = fp.readline()    
    sc.stop()
