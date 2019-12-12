import math
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from collections import Counter
from pyspark import SparkContext, RDD
from csv import reader
import itertools

from dateutil import parser


def is_col_name_like_date(s: str, col: str):
    col_name = col.lower()
    if 'date' in col_name or 'year' in col_name \
            or 'time' in col_name or 'month' in col_name \
            or 'day' in col_name or 'period' in col_name:
        return True
    else:
        return False


def is_date(s: str, col: str):
    s = str(s)
    try:
        parser.parse(s, fuzzy=False)
        return True
    except ValueError:
        return False


def mapd(x: List):
    """
    TODO: check date type
    :param x:
    :return:    """
    # [col_idx, (value, type)]
    res = (x[0], [x[1], None])
    if (x[1] == ''):
        res[1][1] = 'empty'
    elif (is_int(x[1])):
        res[1][1] = 'int'
        res[1][0] = int(res[1][0])
    elif (is_float(x[1])):
        res[1][1] = 'real'
        res[1][0] = float(res[1][0])
    elif (is_date(x[1], x[0])):
        res[1][1] = 'date'
    else:
        res[1][1] = 'text'

    # if(is_col_name_like_date(x[1],x[0])):
    #     res[1][1] = 'date'
    return res


def is_int(s: str):
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_float(value: str):
    if '.' not in value:
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


def generate_meta(spark: SparkSession, path: str):
    # read dataframe
    sc = SparkContext.getOrCreate()
    # Add index to each row, [([...], 0),([...], 1)...]
    rdd = sc.textFile(path).mapPartitions(lambda x: reader(x, delimiter='\t')).zipWithIndex()
    header = rdd.filter(lambda x: x[1] == 0) \
        .map(lambda x: (x[0])).collect()[0]  # extract the first part, ignore idx
    rows = rdd.filter(lambda x: x[1] != 0).map(lambda x: x[0])
    file_name = path.split('/')[-1]
    metadata = {
        'dataset_name': file_name,
        'key_column_candidates': header
    }
    N = len(header)
    # Transform to [(col_idx, value),(col_idx, value)...]
    items = rows.flatMap(
        lambda x, h=header: [(h[i], x[i]) for i in range(N)])

    # Transform to [(col_idx, (value, type)),(col_idx, (value, type))...]
    mapped_items = items.map(mapd)
    col_map = {}
    for col in header:
        col_map[col] = {}

    res_top5 = generate_distinct_top5(items)
    res_counting = generate_null_empty(mapped_items).map(lambda x: [x[0], (x[1][0], x[1][1])])
    # [(col,non-empty, empty, total, distinct_num, top5:(col_name,freq))]
    flat_res = res_counting.join(res_top5).map(lambda x: (x[0], (*x[1][0], *x[1][1])))

    # calculate data_types_profiling
    res_data_type = generate_data_types_profiling(mapped_items)
    # group!
    grouped_res_data_type = res_data_type.groupByKey().mapValues(lambda x: list(x))
    # flat!
    final_res = flat_res.join(grouped_res_data_type).map(lambda x: (x[0], (*x[1][0], *x[1][1]))).collect()
    columns = []
    for res in final_res:
        column_data = {
            'column_name': res[0],
            'number_non_empty_cells': res[1][0],
            'number_empty_cells': res[1][1],
            'number_distinct_values': res[1][2],
            'frequent_values': [x[0] for x in res[1][3]],
        }
        data_types = res[1][-1]
        data_types_list = []
        for item in data_types:
            if item[0] == 'int' or item[0] == 'real':
                data = {
                    'type': "INTEGER (LONG)" if item[0] == 'int' else 'REAL',
                    'count': item[1],
                    'max_value': item[2],
                    'min_value': item[3],
                    'mean': item[4],
                }
            elif item[0] == 'text':
                data = {
                    'type': 'TEXT',
                    'count': item[1],
                    'shortest_values': item[2],
                    'longest_values': item[3],
                    'average_length': item[4],
                }
            else:
                data = {
                    'type': 'DATE/TIME',
                    'count': item[1],
                    'max_value': item[2],
                    'min_value': item[3],
                }
            data_types_list.append(data)
        column_data['data_types'] = data_types_list
        columns.append(column_data)
    metadata['columns'] = columns
    return metadata



def generate_null_empty(mapped_items: RDD) -> RDD:
    """
    :param mapped_items: [(col,(value, type)), ...]
    :return: [(col1,[non-empty, empty, total]), (col2,[null-empty, empty, total])]
    """

    def seqFunc(local, x):
        res = [i for i in local];
        if (x[1] != 'empty'):
            res[0] = local[0] + 1
        else:
            res[1] = local[1] + 1
        res[2] = local[2] + 1
        return res

    combFunc = (lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
    count = mapped_items.aggregateByKey((0, 0, 0), seqFunc, combFunc)
    return count


def generate_distinct_top5(items: RDD) -> RDD:
    """
    :param items: [(col,value),...]
    :return: [(col,(distinct_num, [top5...])),(col,(distinct_num, [top5...])),...]
    """
    freq_items = items.map(lambda x: ((x[0], x[1]), 1)) \
        .aggregateByKey((0, 0),
                        (lambda x, y: (0, x[1] + 1)),
                        (lambda x, y: (x[1] + y[1]))) \
        .map(lambda x: ((x[0][0]), (x[0][1], x[1][1])))
    sorted_grouped_freq_items = freq_items.sortBy(lambda x: x[1][1], ascending=False).groupByKey()
    res = sorted_grouped_freq_items.mapValues(lambda x: (len(x), list(itertools.islice(x, 5))))
    return res


def generate_num_statistic(col_num_type_items: RDD) -> RDD:
    """
    Generate statistic for num of type columns and real of type columns.
    :param col_num_type_items: [(('Wave_Number', 'int'), 3),(('Week_Number', 'int'), 40)...]
    :return: ['Wave_Number', 'int'], [count, max_value, min_value, mean, std])
    """

    def seqFunc(local, x):
        max_value = x if x > local[0] else local[0]
        min_value = x if x < local[1] else local[1]
        return (max_value, min_value, local[2] + x, local[3] + 1)

    combFunc = (lambda x, y: (max(x[0], y[0]), min(x[1], y[1]), x[2] + y[2], x[3] + y[3]))
    num_statistic = col_num_type_items.aggregateByKey((float('-inf'), float('inf'), 0, 0), seqFunc,
                                                      combFunc)
    num_statistic = num_statistic.map(lambda x: (x[0], [*x[1], x[1][2] / x[1][3]]))
    # [(('col_name', 'num_type'),(value, mean))...]
    col_num_mean_items = col_num_type_items.join(num_statistic.map(lambda x: (x[0], x[1][4])))
    result_dev = col_num_mean_items.aggregateByKey((0,), lambda local, x: (
        local[0] + (x[0] - x[1]) ** 2,), (lambda x, y: (x[0] + y[0])))
    result_std = result_dev.map(lambda x: (x[0], math.sqrt(x[1][0])))

    # ['Wave_Number', 'int'], [max_value, min_value, sum, count, mean, std])
    tmp = num_statistic.join(result_std).map(lambda x: [x[0], [*x[1][0], x[1][1]]])

    #  ['Wave_Number', 'int'], [count, max_value, min_value, mean, std])
    return tmp.map(lambda x: (x[0], [x[1][3], x[1][0], x[1][1], x[1][4], x[1][5]]))


def generate_text_statistic(col_text_type_items: RDD) -> RDD:
    """
    :param col_text_type_items: columns of text type
        (('Wave_Number', 'text'),'ACTIVE EXPRESS CAR & LIMO 2')...)

    :return: ((col_name,'text'),(count, shortest,longest,avg_len))
    """

    def seqFunc(local, x):
        #     Not includes empty text
        if local[0] == '#':
            shortest = x
        else:
            shortest = x if len(x) < len(local[0]) else local[0]
        longest = x if len(x) > len(local[1]) else local[1]
        total_len = local[2] + len(x)
        count = local[3] + 1
        return (shortest, longest, total_len, count)

    combFunc = (
        lambda x, y: (
            x[0] if len(x[0]) < len(y[0]) else y[0], x[1] if len(x[1]) > len(y[1]) else y[1],
            x[2] + y[2], x[3] + y[3]))
    shortest_and_longest = col_text_type_items.aggregateByKey(('#', '', 0, 0), seqFunc, combFunc)
    statistic = shortest_and_longest.map(
        lambda x: (x[0], (x[1][0], x[1][1], x[1][2] / x[1][3], x[1][3])))
    return statistic.map(lambda x: (x[0], [x[1][3], x[1][0], x[1][1], x[1][2]]))


def generate_date_statistic(items: RDD):
    """

    :param items: ((col,type),value), (col,type),value))
    :return: ((col,type),(count, max_value,min_value))
    """

    def seqFunc(local, x):
        cur_date = parser.parse(str(x))
        if local[0] == '#':
            max_value = x
        else:
            local_max = parser.parse(str(local[0]))
            max_value = x if cur_date > local_max else local[0]

        if local[1] == '#':
            min_value = x
        else:
            local_min = parser.parse(str(local[1]))
            min_value = x if cur_date < local_min else local[1]

        return (max_value, min_value, local[2] + 1)

    def combFunc(x, y):
        return (x[0] if parser.parse(str(x[0])) > parser.parse(str(y[0])) else y[0],
                x[1] if parser.parse(str(x[1])) < parser.parse(str(y[1])) else y[1], x[2] + y[2])

    date_statistic = items.aggregateByKey(('#', '#', 0), seqFunc, combFunc)
    return date_statistic.map(lambda x: (x[0], [x[1][2], x[1][0], x[1][1]]))


def generate_data_types_profiling(mapped_items: RDD):
    """

    :param mapped_items: [(col,value),...]
    :return: the output of the "data_types"
    """
    col_type_items = mapped_items.map(lambda x: ((x[0], x[1][1]), x[1][0]))
    col_date_type_items = col_type_items.filter(lambda x: x[0][1] == 'date')
    col_num_type_items = col_type_items.filter(lambda x: x[0][1] == 'int' or x[0][1] == 'real')
    col_text_type_items = col_type_items.filter(lambda x: x[0][1] == 'text')
    res_date = generate_date_statistic(col_date_type_items)
    res_num = generate_num_statistic(col_num_type_items)
    res_text = generate_text_statistic(col_text_type_items)
    # [(col),[type,...]]
    res_data_type = (res_date.union(res_num)).union(res_text).map(
        lambda x: (x[0][0], [x[0][1], *x[1]]))
    grouped_res_data_type = res_data_type.groupByKey().mapValues(lambda x: list(x))
    return grouped_res_data_type
