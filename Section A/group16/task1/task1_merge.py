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


def main(datasets_file, output_files_dir):
    datasets = []
    for line in open(datasets_file):
        dataset_name = line.strip()
        json_file = '%s/%s.json' % (output_files_dir, dataset_name)
        with open(json_file) as fin_json:
            datasets.append(json.load(fin_json))
    with open('task1.json', 'w') as fout:
        fout.write(json.dumps({'datasets': datasets}, indent=4))


if __name__ == '__main__':
    datasets_file = sys.argv[1]
    output_files_dir = sys.argv[2]
    main(datasets_file, output_files_dir)
