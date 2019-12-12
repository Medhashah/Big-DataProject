import json
import subprocess

import os

from dateutil import parser
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, FloatType
import time
import sys


def main(start_index, end_index):
    spark = SparkSession \
        .builder \
        .appName("big_data_prof") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    conf = spark.sparkContext._conf.setAll(
        [('spark.executor.memory', '12g'), ('spark.app.name', 'big_data_proj'), ('spark.executor.cores', '4'),
         ('spark.cores.max', '4'), ('spark.driver.memory', '12g')])
    spark.sparkContext.stop()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    new_conf = spark.sparkContext._conf.getAll()
    print(new_conf)
    cmd = "hadoop fs -ls /user/hm74/NYCOpenData"
    files = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
    pfiles = [(x.split()[7], int(x.split()[4])) for x in files[1:]]
    pfiles_sorted = sorted(pfiles, key=lambda x: x[1])
    os.mkdir('job_{}_{}'.format(start_index, end_index))
    for i, nyc_open_datafile in enumerate(pfiles_sorted[start_index:end_index]):
        print("processing number {} of {}".format(i+start_index, end_index))
        # pretty hacky preprocessing but it will work for now
        # could maybe use pathlib library or get it with hdfs
        processed_path = nyc_open_datafile[0]
        df_nod = spark.read.option("header", "true").option("delimiter", "\t").csv(processed_path)
        file_name = processed_path.split('/')[-1].replace('.tsv.gz', '')
        try:
            print(file_name)
            start_process = time.time()
            table_dict = make(df_nod, file_name)
            json_type = json.dumps(table_dict)
            with open("job_{}_{}/{}.json".format(start_index, end_index, file_name), 'w+', encoding="utf-8") as f:
                f.write(json_type)
            end_process = time.time()
            print("total process time {}".format(end_process - start_process))
        except Exception as e:
            print("unable to process because {}".format(e))

def make(df_to_process, file_name):
    columns = df_to_process.columns
    column_count = len(columns)
    get_int_udf = F.udf(get_int, IntegerType())
    get_real_udf = F.udf(get_real, FloatType())
    get_datetime_udf = F.udf(get_datetime)
    get_text_udf = F.udf(get_text)
    agg_list = []
    general_fre = []
    longest = []
    shortest = []
    for i, column in enumerate(columns):
        # df_to_process = df_to_process.withColumn("int_{}".format(column), get_real_udf(column))
        int_name = '{}_int'.format(column)
        real_name = '{}_real'.format(column)
        date_name = '{}_date'.format(column)
        text_name = '{}_text'.format(column)
        len_name = '{}_len'.format(column)
        df_to_process = df_to_process.withColumn(int_name, get_int_udf(column))
        df_to_process = df_to_process.withColumn(real_name, get_real_udf(column))
        df_to_process = df_to_process.withColumn(date_name, get_datetime_udf(column))
        df_to_process = df_to_process.withColumn(text_name, get_text_udf(column))
        df_to_process = df_to_process.withColumn(len_name, F.length(F.col(text_name)))
        agg_list.extend([
                    F.count(F.when(F.col(column).isNull(), True)).alias("count_null_{}".format(column)),
                    F.count(F.when(F.col(column).isNotNull(), True)).alias("count_{}".format(column)),
                    F.countDistinct(F.col(column)).alias("dist_count_{}".format(column)),
                    F.count(F.col(int_name)).alias("int_count_{}".format(column)),
                    F.count(F.col(real_name)).alias("real_count_{}".format(column)),
                    F.count(F.col(date_name)).alias("date_count_{}".format(column)),
                    F.count(F.col(text_name)).alias("text_count_{}".format(column)),
                    F.max(F.col(int_name)).alias("int_max_{}".format(column)),
                    F.max(F.col(real_name)).alias("real_max_{}".format(column)),
                    F.max(F.col(date_name)).alias("date_max_{}".format(column)),
                    F.min(F.col(int_name)).alias("int_min_{}".format(column)),
                    F.min(F.col(real_name)).alias("real_min_{}".format(column)),
                    F.min(F.col(date_name)).alias("date_min_{}".format(column)),
                    F.mean(F.col(int_name)).alias("int_mean_{}".format(column)),
                    F.mean(F.col(real_name)).alias("real_mean_{}".format(column)),
                    F.mean(F.col(len_name)).alias("text_len_mean_{}".format(column)),
                    F.stddev(F.col(int_name)).alias("int_std_{}".format(column)),
                    F.stddev(F.col(real_name)).alias("real_std_{}".format(column)),
                    ])
        if column_count < 8:
            if i == 0:
                general_fre = df_to_process.groupBy(column).agg(F.count(F.col(column)).alias("_c_count")).orderBy(F.col("_c_count"), ascending=False).limit(5).agg(F.collect_list(column).alias('fre'))
                shortest = df_to_process.filter(F.col(text_name).isNotNull()).orderBy(F.col(len_name), ascending=True).limit(5).agg(F.collect_list(column).alias('shortest_values')).select('shortest_values')
                longest = df_to_process.orderBy(F.col(len_name), ascending=False).limit(5).agg(F.collect_list(column).alias('longest_values')).select('longest_values')
            else:
                general_fre = general_fre.union(df_to_process.groupBy(column).agg(F.count(F.col(column)).alias("_c_count")).orderBy(F.col("_c_count"), ascending=False).limit(5).agg(F.collect_list(column).alias('fre')))
                shortest = shortest.union(df_to_process.filter(F.col(text_name).isNotNull()).orderBy(F.col(len_name), ascending=True).limit(5).agg(F.collect_list(column).alias('shortest_values')).select('shortest_values'))
                longest = longest.union(df_to_process.filter(F.col(text_name).isNotNull()).orderBy(F.col(len_name), ascending=False).limit(5).agg(F.collect_list(column).alias('longest_values')).select('longest_values'))
        else:
            general_fre.append(df_to_process.groupBy(column).agg(F.count(F.col(column)).alias("_c_count")).orderBy(F.col("_c_count"), ascending=False).limit(5).agg(F.collect_list(column).alias('fre')).collect())
            shortest.append(df_to_process.filter(F.col(text_name).isNotNull()).orderBy(F.col(len_name), ascending=True).limit(5).agg(F.collect_list(column).alias('shortest_values')).select('shortest_values').collect())
            longest.append(df_to_process.orderBy(F.col(len_name), ascending=False).limit(5).agg(F.collect_list(column).alias('longest_values')).select('longest_values').collect())
            print("column {} of {}".format(i, column_count))
    df_all_agg = df_to_process.agg(*agg_list)
    all_agg = df_all_agg.collect()
    if not isinstance(general_fre, list):
        general_fre = general_fre.collect()
        shortest = shortest.collect()
        longest = longest.collect()
    else:
        print(general_fre)
        print(shortest)
        print(longest)
        general_fre = [[x[0]['fre']] for x in general_fre]
        shortest = [[x[0]['shortest_values']] for x in shortest]
        longest = [[x[0]['longest_values']] for x in longest]
    table_dict = convert_df_to_dict(columns, all_agg, longest, shortest, general_fre, file_name)
    return table_dict


def convert_df_to_dict(columns, all_agg, longest, shortest, fre, file_name):
    all_agg = all_agg[0]
    stats_shortest = shortest
    stats_longest = longest
    general_fre = fre
    table_dict = dict()
    table_dict['dataset_name'] = file_name
    table_dict['columns'] = []
    for i, column in enumerate(columns):
        column_dict = {}
        gen_count = "count_{}".format(column)
        gen_count_null = "count_null_{}".format(column)
        gen_dist_count = "dist_count_{}".format(column)
        int_count = "int_count_{}".format(column)
        real_count = "real_count_{}".format(column)
        date_count = "date_count_{}".format(column)
        text_count = "text_count_{}".format(column)
        int_max = "int_max_{}".format(column)
        real_max = "real_max_{}".format(column)
        date_max = "date_max_{}".format(column)
        int_min = "int_min_{}".format(column)
        real_min = "real_min_{}".format(column)
        date_min = "date_min_{}".format(column)
        int_mean = "int_mean_{}".format(column)
        real_mean = "real_mean_{}".format(column)
        int_std = "int_std_{}".format(column)
        real_std = "real_std_{}".format(column)
        text_len_mean = "text_len_mean_{}".format(column)
        column_dict['column_name'] = column
        column_dict['number_empty_cells'] = all_agg[gen_count_null]
        column_dict['number_non_empty_cells'] = all_agg[gen_count]
        column_dict['number_distinct_values'] = all_agg[gen_dist_count]
        column_dict['frequent_values'] = general_fre[i][0]
        column_dict['data_type'] = []
        if all_agg[int_count] != 0:
            type_dict = {}
            type_dict['type'] = "INTERGER(LONG)"
            type_dict['count'] = int(all_agg[int_count])
            type_dict['max_value'] = int(all_agg[int_max])
            type_dict['min_value'] = int(all_agg[int_min])
            type_dict['mean'] = float(all_agg[int_mean])
            type_dict['stddev'] = float(all_agg[int_std])
            column_dict['data_type'].append(type_dict)
        if all_agg[real_count] != 0:
            type_dict = {}
            type_dict['type'] = 'REAL'
            type_dict['count'] = int(all_agg[real_count])
            type_dict['max_value'] = float(all_agg[real_max])
            type_dict['min_value'] = float(all_agg[real_min])
            type_dict['mean'] = float(all_agg[real_mean])
            type_dict['stddev'] = float(all_agg[real_std])
            column_dict['data_type'].append(type_dict)
        if all_agg[date_count] != 0:
            type_dict = {}
            type_dict['type'] = "DATE/TIME"
            type_dict['count'] = int(all_agg[date_count])
            type_dict['max_value'] = all_agg[date_max]
            type_dict['min_value'] = all_agg[date_min]
            column_dict['data_type'].append(type_dict)
        if all_agg[text_count] != 0:
            type_dict = {}
            type_dict['type'] = "TEXT"
            type_dict['count'] = all_agg[text_count]
            type_dict['shortest_values'] = stats_shortest[i][0]
            type_dict['longest_values'] = stats_longest[i][0]
            type_dict['average_length'] = all_agg[text_len_mean]
            column_dict['data_type'].append(type_dict)
        table_dict['columns'].append(column_dict)
    return table_dict


def is_int(val):
    try:
        int(val)
        return True
    except:
        return False


def is_real(val):
    try:
        float(val)
        return True
    except:
        return False


def is_datetime(val):
    try:
        parser.parse(val)
        return True
    # raw exception here, I tried to catch none raw dateutil error exception, but it's giving some errors
    # not sure I will need to fix up.
    except:
        return False


def get_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def get_real(val):
    if is_int(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def get_datetime(val):
    if is_int(val):
        return None
    elif is_real(val):
        return None
    try:
        return parser.parse(val).isoformat()
    # raw exception here, I tried to catch none raw dateutil error exception, but it's giving some errors
    # not sure I will need to fix up.
    except:
        return None


def get_text(val):
    if is_int(val):
        return None
    elif is_real(val):
        return None
    elif is_datetime(val):
        return None
    elif val is None:
        return None
    else:
        return val


if __name__ == "__main__":
    start_index = int(sys.argv[1])
    end_index = int(sys.argv[2])
    main(start_index, end_index)