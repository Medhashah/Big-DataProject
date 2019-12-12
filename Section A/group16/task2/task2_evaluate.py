#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import gzip
import json
import math
import os
import sys
import csv
import numpy as np


from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import precision_recall_fscore_support


def read_columns_type(file):
    labels = []
    with open(file) as fin:
        for line in fin:
            column_types, column = line.split('\t')
            if column_types == '':
                labels.append('NULL')
            else:
                labels.append(','.join(sorted(column_types.split(','))))
    return labels


def main(columns_type_file, columns_type_predicted_file):
    labels = read_columns_type(columns_type_file)
    labels_hat = read_columns_type(columns_type_predicted_file)
    p = precision_score(labels, labels_hat, average='weighted')
    r = recall_score(labels, labels_hat, average='weighted')
    print(labels,labels_hat)
    #print(len(labels),len(labels_hat))
    #print(precision_recall_fscore_support(np.array(labels), np.array(labels_hat), average='weighted'))
    print('Precision: %.4f' % p)
    print('Recall: %.4f' % r)


if __name__ == '__main__':
    columns_type_file = sys.argv[1]
    columns_type_predicted_file = sys.argv[2]
    main(columns_type_file, columns_type_predicted_file)
