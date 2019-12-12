#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import csv
import datetime
import gzip
import json
import math
import os
import sys

from dateutil import parser
import numpy as np


class DataTypeBase(object):

    def __init__(self, type_name):
        self.type = type_name
        self.count = 0
        self.values = []

    def add_value(self, value, cell):
        self.values.append(value)
        self.count += 1

    def to_dict(self):
        d = copy.copy(self.__dict__)
        d.pop('values')
        return d


class DataTypeNumeric(DataTypeBase):

    def __init__(self, type_name):
        super(DataTypeNumeric, self).__init__(type_name)
        self.max_value = 0
        self.min_value = 0
        self.mean = 0
        self.stddev = 0

    def gen_statistics(self):
        self.max_value = np.max(self.values)
        self.min_value = np.min(self.values)
        self.mean = np.mean(self.values)
        self.stddev = np.std(self.values)


class DataTypeDateTime(DataTypeBase):

    def __init__(self, type_name):
        super(DataTypeDateTime, self).__init__(type_name)
        self.raw_values = []
        self.max_value = ''
        self.min_value = ''

    def add_value(self, value, cell):
        self.values.append(value)
        self.raw_values.append(cell)
        self.count += 1

    def gen_statistics(self):
        self.max_value = self.raw_values[np.argmax(self.values)]
        self.min_value = self.raw_values[np.argmin(self.values)]

    def to_dict(self):
        d = super(DataTypeDateTime, self).to_dict()
        d.pop('raw_values')
        return d


class DataTypeText(DataTypeBase):

    def __init__(self, type_name):
        super(DataTypeText, self).__init__(type_name)
        self.shortest_values = []
        self.longest_values = []
        self.average_length = 0

    def gen_statistics(self):
        values_sort_by_len = sorted(set(self.values), key=lambda x: len(x))
        self.shortest_values = values_sort_by_len[:5]
        self.longest_values = values_sort_by_len[-5:]
        self.average_length = sum([len(v) for v in self.values]) / float(len(self.values))


class Column(object):

    def __init__(self, column_name):
        self.column_name = column_name
        self.number_non_empty_cells = 0
        self.number_empty_cells = 0
        self.number_distinct_values = 0
        self.frequent_values = []
        self.data_types = []

        self.data_by_type = {
            'INTEGER (LONG)': DataTypeNumeric('INTEGER (LONG)'),
            'REAL': DataTypeNumeric('REAL'),
            'DATE/TIME': DataTypeDateTime('DATE/TIME'),
            'TEXT': DataTypeText('TEXT'),
        }

        self.value2cnt = {}

    def to_dict(self):
        return {
            'column_name': self.column_name,
            'number_non_empty_cells': self.number_non_empty_cells,
            'number_empty_cells': self.number_empty_cells,
            'number_distinct_values': self.number_distinct_values,
            'frequent_values': self.frequent_values,
            'data_types': [dt.to_dict() for dt in self.data_types]
        }


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
                return 'TEXT', cell
            return 'DATE/TIME', value
        return 'REAL', value
    return 'INTEGER (LONG)', value


def dump_json(dataset_name, columns, key_column_candidates):
    output = {
        'dataset_name': dataset_name,
        'key_column_candidates': key_column_candidates,
        'columns': [col.to_dict() for col in columns],
    }
    return json.dumps(output, indent=4)


def main(input_file, out_file):
    dataset_name = input_file.split('/')[-1]
    columns = []
    key_column_candidates = []
    if dataset_name.endswith('.gz'):
        fin = gzip.open(input_file, 'rt', encoding='utf-8')
    else:
        fin = open(input_file, encoding='utf-8')
    print('%s opened' % input_file, flush=True)
    for i, cells in enumerate(csv.reader(fin, delimiter="\t")):
        if i % 10000 == 0:
            print(i, flush=True)
        if i == 0:
            columns = [Column(column_name) for column_name in cells]
            continue
        for j, cell in enumerate(cells):
            col = columns[j]
            if cell != '':
                col.number_non_empty_cells += 1
            else:
                col.number_empty_cells += 1
            type_name, value = inspect_type(cell)
            data_by_type = col.data_by_type[type_name]
            data_by_type.add_value(value, cell)
            col.value2cnt[cell] = columns[j].value2cnt.get(cell, 0) + 1

    for i, col in enumerate(columns):
        print('%d/%d' % (i, len(columns)), flush=True)
        col.number_distinct_values = len(col.value2cnt)
        col.frequent_values = [v for v,_ in sorted(col.value2cnt.items(), key=lambda x: x[1])[-5:]]
        for _, dt in col.data_by_type.items():
            if len(dt.values) == 0:
                continue
            dt.gen_statistics()
            col.data_types.append(dt)

        if len(col.data_types) == 1 \
                and col.data_types[0].type in ('INTEGER (LONG)', 'DATE/TIME') \
                and col.number_non_empty_cells + col.number_empty_cells == col.number_distinct_values:
            key_column_candidates.append(col.column_name)

    with open(output_file, 'w') as fout:
        fout.write(dump_json(dataset_name, columns, key_column_candidates))


if __name__ == '__main__':
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)
