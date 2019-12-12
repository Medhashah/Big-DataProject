from itertools import islice
import sys
import os
import json
import time
import csv

from dateutil import parser
from pyspark import SparkContext, SparkConf


DATA_TYPE_INTEGER = 0
DATA_TYPE_REAL = 1
DATA_TYPE_DATETIME = 2
DATA_TYPE_TEXT = 3


def inspect_type(cell):
    try:
        value = long(cell)
    except:
        try:
            value = float(cell)
        except:
            try:
                value = parser.parse(cell).replace(tzinfo=None)
            except:
                return DATA_TYPE_TEXT, cell
            return DATA_TYPE_DATETIME, value
        return DATA_TYPE_REAL, value
    return DATA_TYPE_INTEGER, value


def map_column(cell):
    type_name, value = inspect_type(cell)
    return cell, type_name, value


input_file = sys.argv[1]
output_dir = sys.argv[2]


os.system('mkdir -p %s' % output_dir)

sc = SparkContext()
sc.setLogLevel("ERROR")

for line in open(input_file):
    start = time.time()
    input_file = '/user/hm74/NYCOpenData/%s' % line.strip()
    dataset_name = input_file.split('/')[-1]
    output_file = '%s/%s.json' % (output_dir, dataset_name)
    output = {
        'dataset_name': dataset_name,
        'columns': [],
        'key_column_candidates': [],
    }

    print('Processing %s ...' % dataset_name)

    rdd = sc.textFile(input_file)
    rdd = rdd.mapPartitions(lambda x: csv.reader(x, delimiter="\t"))
    column_names = rdd.first()
    print('columns[%d]' % len(column_names))
    rdd = rdd.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    number_all = rdd.count()

    for i, column_name in enumerate(column_names):
        print('%d/%d' % (i+1, len(column_names)))
        rdd_col = rdd.map(lambda x: x[i])
        print('%d/%d - all count' % (i+1, len(column_names)))
        print('%d/%d - non empty count' % (i+1, len(column_names)))
        number_non_empty_cells = rdd_col.filter(lambda x: x != '').count()
        number_empty_cells = number_all - number_non_empty_cells
        print('%d/%d - value2cnt' % (i+1, len(column_names)))
        rdd_value2cnt = rdd_col.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
        number_distinct_values = rdd_value2cnt.count()
        print('%d/%d - frequent_values' % (i+1, len(column_names)))
        frequent_values = rdd_value2cnt.sortBy(lambda x: x[1], False).keys().take(5)

        data_types = []
        print('%d/%d - map column' % (i+1, len(column_names)))
        rdd_cell_dtype_value = rdd_col.map(map_column)

        # INTEGER (LONG)
        print('%d/%d - checking type of INTEGER (LONG) ...' % (i+1, len(column_names)))
        rdd_int = rdd_cell_dtype_value.filter(lambda x: x[1] == DATA_TYPE_INTEGER)\
                    .map(lambda x: x[2])
        count = rdd_int.count()
        if (count > 0):
            data_types.append({
                'type': 'INTEGER (LONG)',
                'count': count,
                'max_value': rdd_int.max(),
                'min_value': rdd_int.min(),
                'mean': rdd_int.mean(),
                'stddev': rdd_int.stdev(),
            })
            if count == number_all and count == number_distinct_values:
                output['key_column_candidates'].append(column_name)


        # REAL
        print('%d/%d - checking type of REAL ...' % (i+1, len(column_names)))
        rdd_real = rdd_cell_dtype_value.filter(lambda x: x[1] == DATA_TYPE_REAL)\
                    .map(lambda x: x[2])
        count = rdd_real.count()
        if (count > 0):
            data_types.append({
                'type': 'REAL',
                'count': count,
                'max_value': rdd_real.max(),
                'min_value': rdd_real.min(),
                'mean': rdd_real.mean(),
                'stddev': rdd_real.stdev(),
            })


        # DATE/TIME
        print('%d/%d - checking type of DATE/TIME ...' % (i+1, len(column_names)))
        rdd_datetime = rdd_cell_dtype_value.filter(lambda x: x[1] == DATA_TYPE_DATETIME)\
                        .map(lambda x: (x[0], x[2]))
        count = rdd_datetime.count()
        if (count > 0):
            data_types.append({
                'type': 'DATE/TIME',
                'count': count,
                'max_value': rdd_datetime.max(key=lambda x: x[1])[0],
                'min_value': rdd_datetime.min(key=lambda x: x[1])[0],
            })
            if count == number_all and count == number_distinct_values:
                output['key_column_candidates'].append(column_name)

        # TEXT
        print('%d/%d - checking type of TEXT ...' % (i+1, len(column_names)))
        rdd_text_len = rdd_cell_dtype_value.filter(lambda x: x[1] == DATA_TYPE_TEXT)\
                    .map(lambda x: (x[2], len(x[2])))
        count = rdd_text_len.count()
        if (count > 0):
            rdd_text_len_uniq = rdd_text_len.distinct()
            data_types.append({
                'type': 'REAL',
                'count': count,
                'shortest_values': rdd_text_len_uniq.sortBy(lambda x: x[1]).keys().take(5),
                'longest_values': rdd_text_len_uniq.sortBy(lambda x: x[1], False).keys().take(5),
                'average_length': rdd_text_len_uniq.map(lambda x: x[1]).mean(),
            })

        output['columns'].append({
            'column_name': column_name,
            'number_non_empty_cells': number_non_empty_cells,
            'number_empty_cells': number_empty_cells,
            'number_distinct_values': number_distinct_values,
            'frequent_values': frequent_values,
            'data_types': data_types,
        })

    with open(output_file, 'w') as fout:
        fout.write(json.dumps(output, indent=4))

    print('%s: time cost %d seconds' % (dataset_name, time.time() - start))
