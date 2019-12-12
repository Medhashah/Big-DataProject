from datetime import datetime
import json
import time
from task1 import rdd_util

from pyspark import SparkContext
import pandas as pd

# file_path = sys.argv[1]
from pyspark.sql import SparkSession
import logging

OUTPUT = './output/meta.json'
OUTPUT_DIR = './tmp_result/'
LOG = './log/process.log'
ERROR_FILE_LIST = './log/error_file_list'
logging.basicConfig(filename='./log/spark_log.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    spark = SparkSession \
        .builder \
        .appName("project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    file_list = "file_list/file_list_sorted.csv"
    df = pd.read_csv(file_list, sep=' ', names=['path', 'size'])
    res = []
    sc = SparkContext.getOrCreate()
    for idx, row in df.iterrows():
        try:
            log_file = open(LOG, 'a+')
            err_file_list = open(ERROR_FILE_LIST, 'a+')
            print(row['path'])
            start = time.time()
            file_name = row['path'].split('/')[-1]
            meta = rdd_util.generate_meta(sc, row['path'])
            result_fd = open(OUTPUT_DIR + file_name + '.json', 'w')
            json.dump(meta, result_fd, indent=2)
            end = time.time()
            s1 = file_name + " Time elapsed:" + str(end - start) + " seconds"
            log_file.writelines(
                datetime.now().strftime('%m-%d,%H:%M:%S') + ' ' + s1 + '\n')
        # Catch all exceptions
        except:
            logging.error("Exception occurred on " + file_name, exc_info=True)
            err_file_list.writelines(file_name + '\n')
            continue
        finally:
            log_file.close()
            err_file_list.close()


main()
