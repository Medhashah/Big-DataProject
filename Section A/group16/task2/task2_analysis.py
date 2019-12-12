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


def plot_heterogeneous_columns(metadatas):
    cnt_columns_heterogeneous = {}
    for metadata in metadatas:
        num_data_types = len(metadata['semantic_types'])
        cnt_columns_heterogeneous[num_data_types] = cnt_columns_heterogeneous.get(num_data_types, 0) + 1
    plt.figure()
    cnt_heterogeneou = list(cnt_columns_heterogeneous.keys())
    cnt_columns = list(cnt_columns_heterogeneous.values())
    plt.bar(cnt_heterogeneou, cnt_columns, align='center')
    plt.xticks(cnt_heterogeneou, ['%s' % i for i in cnt_heterogeneou])
    plt.xlabel('Number of semantic types of a column')
    plt.ylabel('Number of columns')
    plt.xlim(0, 5)


def plot_column_num_each_type(metadatas,filenames):
    type2columncnt = {}
    i = 0
    for metadata in metadatas:
        temp = 0
        
        for semantic_type in metadata['semantic_types']:
            semantic_type = semantic_type['semantic_type']
            if semantic_type == 'other':
                continue
            temp+= 1
            
        if temp > 1:
            type2columncnt[filenames[i]] = temp
        i+= 1
    
    plt.figure()
    types = list(type2columncnt.keys())
    cnts = list(type2columncnt.values())
    plt.barh(range(len(types)), cnts, align='center')
    plt.yticks(range(len(types)), types)
    plt.yticks(fontsize=10)
    plt.xlabel('Columns of multiple values')
    plt.xlim(0, 3)
    plt.tight_layout()


def main(datasets_file, json_files_dir):
    metadatas = []
    filenames = []
   
  
    #print(dic)




    for line in open(datasets_file):
        dataset_name = line.strip().replace('.txt.gz', '')
        json_file = '%s/%s.json' % (json_files_dir, dataset_name)
        with open(json_file) as fin_json:
            metadatas.append(json.load(fin_json))
            filenames.append(dataset_name)
     

    plot_heterogeneous_columns(metadatas)
    plot_column_num_each_type(metadatas,filenames)
    plt.show()


if __name__ == '__main__':
    datasets_file = sys.argv[1]
    json_files_dir = sys.argv[2]
    main(datasets_file, json_files_dir)
