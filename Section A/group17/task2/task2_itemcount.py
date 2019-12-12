import os
import sys
import time

from pyspark import SparkContext, SparkConf

HDFS_COLS_TXT = '/user/hl3475/final_project/task2/colstxt/%s'

input_file = sys.argv[1]
output_dir = sys.argv[2]

os.system('mkdir -p %s' % output_dir)

sc = SparkContext()
sc.setLogLevel("ERROR")

for line in open(input_file):
    start = time.time()
    input_file = HDFS_COLS_TXT % line.strip().replace('.gz', '')
    dataset_name = input_file.split('/')[-1]
    output_file = '%s/%s' % (output_dir, dataset_name)

    print('Processing %s ...' % dataset_name)

    rdd = sc.textFile(input_file)
    rdd_value2cnt = rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    rdd_value2cnt = rdd_value2cnt.sortBy(lambda x: x[1], False)

    with open(output_file, 'w', encoding='utf8') as fout:
        for value, cnt in rdd_value2cnt.collect():
            fout.write('%s\t%d\n' % (value, cnt))

    print('%s: time cost %d seconds' % (dataset_name, time.time() - start))
