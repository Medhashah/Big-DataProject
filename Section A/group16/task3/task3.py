#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import gzip
import json
import math
import os
import sys
import csv


DATA_TYPE_INTEGER = 0
DATA_TYPE_REAL = 1
DATA_TYPE_TEXT = 2


def inspect_type(cell):
    try:
        value = long(cell)
    except:
        try:
            value = float(cell)
        except:
            return DATA_TYPE_TEXT, cell
        return DATA_TYPE_REAL, value
    return DATA_TYPE_INTEGER, value


def detect_abnormal_number(value2cnt, beta=100):
    # detect by Z-score
    abnormals = []
    value_sum = sum(float(v) * cnt for (v, _), cnt in value2cnt.items())
    value_cnt = sum(cnt for _, cnt in value2cnt.items())
    mean = value_sum / value_cnt
    std = math.sqrt(sum((v - mean) ** 2 * cnt for (v, _), cnt in value2cnt.items()) / value_cnt)
    if std == 0:
        return []
    for (v, item), cnt in value2cnt.items():
        if abs((v - mean) / std) > beta:
            abnormals.append(item)
    return abnormals


def detect_abnormal_text(value2cnt):
    valuelen2cnt = {(len(v),_): cnt for (v, _), cnt in value2cnt.items()}
    abnormals = detect_abnormal_number(valuelen2cnt)
    return abnormals


def detect_abnormal(type2valuecount):
    abnormals = []
    type2uniqcount = {_type: len(value2cnt) for _type, value2cnt in type2valuecount.items()}
    type_counts = sorted(type2uniqcount.items(), key=lambda x: x[1])
    dominant_type, dominant_cnt = type_counts[-1]

    # detect abnormals by data type
    if len(type2valuecount) > 1:
        alpha = 0.01
        for i in range(len(type2valuecount) - 1):
            _type, cnt = type_counts[i]
            if cnt < alpha * dominant_cnt:
                abnormals.extend([x for _, x in type2valuecount[_type].keys()])

    value2cnt = type2valuecount[dominant_type]
    if dominant_type in (DATA_TYPE_INTEGER, DATA_TYPE_REAL):
        value_abnormals = detect_abnormal_number(value2cnt)
    elif dominant_type == DATA_TYPE_TEXT:
        value_abnormals = detect_abnormal_text(value2cnt)

    abnormals.extend(value_abnormals)
    return abnormals



def main(columns_file, colstxt_itemcount_dir):
    dataset2cols = {}
    for line in open(columns_file):
        # {prefix}.{column}.txt.gz
        prefix, column_name, _, _ = line.strip().split('.')
        colstxt_itemcount_file = '%s/%s.%s.txt' % (colstxt_itemcount_dir, prefix, column_name)
        type2valuecount = {
            DATA_TYPE_INTEGER: {},
            DATA_TYPE_REAL: {},
            DATA_TYPE_TEXT: {},
        }
        with open(colstxt_itemcount_file, encoding='utf8') as fin:
            for _line in fin:
                xs = _line.split('\t')
                item = '\t'.join(xs[:-1])
                count = int(xs[-1])
                data_type, value = inspect_type(item)
                type2valuecount[data_type][(value, item)] = type2valuecount[data_type].get((value, item), 0) + count
        abnormals = detect_abnormal(type2valuecount)
        if len(abnormals) == 0:
            continue
        print('%s.%s:' % (prefix, column_name))
        print('NULL & outliers: %s\n' % abnormals)



if __name__ == '__main__':
    columns_file = sys.argv[1]
    colstxt_itemcount_dir = sys.argv[2]
    main(columns_file, colstxt_itemcount_dir)
