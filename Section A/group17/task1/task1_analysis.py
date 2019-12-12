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

import numpy as np
import matplotlib.pyplot as plt


def plot_missing_values(metadatas):
    missing_percentages = []
    for metadata in metadatas:
        for column in metadata['columns']:
            num_all = column['number_non_empty_cells'] + column['number_empty_cells']
            if num_all == 0:
                continue
            missing_percentages.append(column['number_empty_cells'] / float(num_all))
    print('Average percentage of missing values: %.4f' % np.mean(missing_percentages))
    plt.hist([x for x in missing_percentages if x != 0], bins=100)
    plt.xlabel('Percentage of missing values (>0)')
    plt.ylabel('Frequency')


def plot_heterogeneous_columns(metadatas):
    cnt_columns_heterogeneous = {}
    for metadata in metadatas:
        for column in metadata['columns']:
            num_data_types = len(column['data_types'])
            cnt_columns_heterogeneous[num_data_types] = cnt_columns_heterogeneous.get(num_data_types, 0) + 1
    plt.figure()
    cnt_heterogeneou = list(cnt_columns_heterogeneous.keys())
    cnt_columns = list(cnt_columns_heterogeneous.values())
    plt.bar(cnt_heterogeneou, cnt_columns, align='center')
    plt.xticks(cnt_heterogeneou, ['%s' % i for i in cnt_heterogeneou])
    plt.xlabel('Number of types of a column')
    plt.ylabel('Number of columns')
    plt.xlim(0, 5)


def plot_column_num_each_type(metadatas):
    type2columncnt = {}
    for metadata in metadatas:
        for column in metadata['columns']:
            for data_type in column['data_types']:
                _type = data_type['type']
                type2columncnt[_type] = type2columncnt.get(_type, 0) + 1
    plt.figure()
    types = list(type2columncnt.keys())
    cnts = list(type2columncnt.values())
    plt.bar(range(len(types)), cnts, align='center')
    plt.xticks(range(len(types)), types)
    plt.ylabel('Number of columns')


def main(datasets_file, json_files_dir):
    metadatas = []
    for line in open(datasets_file):
        dataset_name = line.strip()
        json_file = '%s/%s.json' % (json_files_dir, dataset_name)
        with open(json_file) as fin_json:
            metadatas.append(json.load(fin_json))
    plot_missing_values(metadatas)
    plot_heterogeneous_columns(metadatas)
    plot_column_num_each_type(metadatas)
    plt.show()


if __name__ == '__main__':
    datasets_file = sys.argv[1]
    json_files_dir = sys.argv[2]
    main(datasets_file, json_files_dir)
