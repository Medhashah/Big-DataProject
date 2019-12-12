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


NYC_OPENDATA_FILE = '/home/hl3475/NYCOpenData/%s.tsv.gz'


def get_dataset2cols(columns_file):
    dataset2cols = {}
    with open(columns_file) as fin:
        for line in fin:
            dataset, col, _, _ = line.split('.')
            if dataset not in dataset2cols:
                dataset2cols[dataset] = set()
            dataset2cols[dataset].add(col)
    return dataset2cols

W = set(str(i) for i in range(10))

def cvt_col_name(src):
    dst = []
    for c in src:
        if c not in W and not ((c >= 'A' and c <= 'Z') or (c >= 'a' and c <='z')) and c != '_':
            c = '_'
        dst.append(c)
    return ''.join(dst)


def main(input_columns_file, out_columns_dir):
    os.system('mkdir -p %s' % out_columns_dir)
    dataset2cols = get_dataset2cols(input_columns_file)
    for k, (dataset, cols) in enumerate(dataset2cols.items()):
        print('%d/%d %s' % (k, len(dataset2cols), dataset))
        input_file = NYC_OPENDATA_FILE % dataset
        output_fds = {c: open('%s/%s.%s.txt' % (out_columns_dir, dataset, c), 'w', encoding='utf8') for c in cols}
        idx = {}
        print(cols)
        fin = gzip.open(input_file, 'rt', encoding='utf-8')
        for i, cells in enumerate(csv.reader(fin, delimiter="\t")):
            if i == 0:
                try:
                    print(cells)
                except:
                    print('fail to print columns')
                for m, cell in enumerate(cells):
                    c = cvt_col_name(cell)
                    if c in cols:
                        idx[c] = m
                assert(len(idx) == len(cols))
                continue
            for c, fd in output_fds.items():
                fd.write('%s\n' % cells[idx[c]])
        for _, fd in output_fds.items():
            fd.close()
        fin.close()


if __name__ == '__main__':
    input_columns_file = sys.argv[1]
    out_columns_dir = sys.argv[2]
    main(input_columns_file, out_columns_dir)
