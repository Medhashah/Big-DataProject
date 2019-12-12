"""
Current status of this code: Everything working correctly, creating separate json for each dataset.
"""
import pyspark
from pyspark.sql import SparkSession

conf = pyspark.SparkConf().setAll([('spark.driver.memory','32g'), ('spark.executor.memory', '8g'), ('spark.executor.cores', '4'), ('spark.executor.instances', '6')])

sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


# -------------------------------------------------------------------------------
from pyspark.sql.functions import lit
from dateutil.parser import parse
import json
import time

def mapper_identical_vals(x):
    ans = []
    for coli, col in enumerate(x[:-1]):
        dataset_name = str(x[-1])
        try:
            if(col != None):
                col_eval = json.loads(col)
                t = type(col_eval)
                if(t == int):
                    col_eval = int(col_eval)
                    # maintain datatype, sum, count, max, min, sum_of_squares
                    ans.append(((dataset_name, coli, 'I', col_eval), ('I', col_eval, 1, col_eval, col_eval, col_eval**2)))
                elif(t == float):
                    col_eval = float(col_eval)
                    # maintain datatype, sum, count, max, min, sum_of_squares
                    try:
                        ans.append(((dataset_name, coli, 'R', col_eval), ('R', col_eval, 1, col_eval, col_eval, col_eval**2)))
                    except OverflowError as err:
                        #print("Overflow exception occurred", coli, col, x[-1])
                        ans.append(((dataset_name, coli, 'T', col), ('T', len(col), 1)))
            else:
                ans.append(((dataset_name, coli, 'None', 'None'), ('None', 1)))
        except ValueError:
            # check whether it is of type DATE
            try:
                parsed_date = parse(col, ignoretz = True)
                # treat as a DATE type: datatype, parsed_date, count
                # parsed_date can be used as a key, but we will maintain original value for future use
                ans.append(((dataset_name, coli, 'D', col), ('D', parsed_date, 1)))
            except:
                # treat as a normal text, maintain datatype, sum, count
                ans.append(((dataset_name, coli, 'T', col), ('T', len(col), 1)))
        except SyntaxError:
            print("Error in evaluating data type for", col)
    return ans



def mapper_identical_datatypes(x):
    if(x[1][0] == 'I' or x[1][0] == 'R'):
        top5cnt_list = [(x[1][2], x[0][3]), (0, 0), (0, 0), (0, 0), (0, 0)]
        # key = (col, datatype); value = (data_type, sum, total_count, max,     \
        # min, sum_of_squares, top5cnt_list, distinct_cnt)
        new_list = [x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_val = tuple(new_list)
    elif(x[1][0] == 'T'):
        top5cnt_list = [(x[1][2], x[0][3]), (0, 0), (0, 0), (0, 0), (0, 0)]
        top5long_list = []
        top5short_list = []
        for i in range(min(5, x[1][2])):
            top5long_list.append((x[1][1], x[0][3]))
            top5short_list.append((x[1][1], x[0][3]))
        for i in range(len(top5long_list), 5):
            top5long_list.append((0, ""))
            top5short_list.append((float('inf'), ""))
        new_list = [x[1][0], x[1][1], x[1][2]]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_list.append(top5long_list)
        new_list.append(top5short_list)
        new_val = tuple(new_list)
    elif(x[1][0] == 'D'):
        top5cnt_list = [(x[1][2], x[0][3]), (0, 0), (0, 0), (0, 0), (0, 0)]
        # datatype, count, max, min, top5list, dist_cnt
        new_list = [x[1][0], x[1][2], (x[0][3], x[1][1]), (x[0][3], x[1][1])]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_val = tuple(new_list)
    elif(x[1][0] == 'None'):
        return x
    return ((x[0][0], x[0][1], x[0][2]), new_val)



def mapper_identical_columns(x):
    if(x[1][0] == 'I' or x[1][0] == 'R'):
        tot_cnt = x[1][2]
        mean = x[1][1]/x[1][2]
        mx = x[1][3]
        mn = x[1][4]
        stdev = (abs(x[1][5]/x[1][2] - mean**2))**0.5
        tcnt_list = x[1][6]
        dist_cnt = x[1][7]
        return ((x[0][0], x[0][1]), {x[1][0]: (tot_cnt, mean, mx, mn, stdev, tcnt_list, dist_cnt)})
    elif(x[1][0] == 'T'):
        mean = x[1][1]/x[1][2]
        # datatype, tot_cnt, mean, t5cnt, dist_cnt, long5, short5
        return ((x[0][0], x[0][1]), {'T': (x[1][2], mean, x[1][3], x[1][4], x[1][5], x[1][6])})
    elif(x[1][0] == 'D'):
        # datatype, count, max, min, top5cnt, dist_cnt
        return ((x[0][0], x[0][1]), {'D': x[1][1:]})
    elif(x[1][0] == 'None'):
        # datatype, count
        return ((x[0][0], x[0][1]), {x[1][0]: x[1][1]})



def mapper_identical_datasets(x):
    column_obj = {"column_name": x[0][1], "number_non_empty_cells": 0, "number_empty_cells": 0, "number_distinct_values": 0, "frequent_values": [], "data_types": []}
    for dtype, dinfo in x[1].items():
        if(dtype == 'I' or dtype == 'R'):
            column_obj['number_non_empty_cells'] += dinfo[0]
            column_obj['number_distinct_values'] += dinfo[6]
            column_obj['frequent_values'].extend(dinfo[5])
            if(dtype == 'I'):
                tp = "INTEGER (LONG)"
            else:
                tp = "REAL"
            data_type_obj = {"type": tp, "count": dinfo[0], "max_value": dinfo[2], "min_value": dinfo[3], "mean": dinfo[1], "stddev": dinfo[4]}
            column_obj['data_types'].append(data_type_obj)
        elif(dtype == 'T'):
            column_obj['number_non_empty_cells'] += dinfo[0]
            column_obj['number_distinct_values'] += dinfo[3]
            column_obj['frequent_values'].extend(dinfo[2])
            sl = [dinfo[5][0][1], dinfo[5][1][1], dinfo[5][2][1], dinfo[5][3][1], dinfo[5][4][1]]
            ll = [dinfo[4][0][1], dinfo[4][1][1], dinfo[4][2][1], dinfo[4][3][1], dinfo[4][4][1]]
            data_type_obj = {"type": 'TEXT', "count": dinfo[0], "shortest_values": sl, "longest_values": ll, "average_length": dinfo[1]}
            column_obj['data_types'].append(data_type_obj)
        elif(dtype == 'D'):
            column_obj['number_non_empty_cells'] += dinfo[0]
            column_obj['number_distinct_values'] += dinfo[4]
            column_obj['frequent_values'].extend(dinfo[3])
            data_type_obj = {"type": 'DATE/TIME', "count": dinfo[0], "max_value": dinfo[1][0], "min_value": dinfo[2][0]}
            column_obj['data_types'].append(data_type_obj)
        elif(dtype == 'None'):
            column_obj['number_empty_cells'] = dinfo
    column_obj['frequent_values'] = sorted(column_obj['frequent_values'], key = lambda x: x[0], reverse = True)[:5]
    column_obj['frequent_values'] = [x[1] for x in column_obj['frequent_values']]
    return ((x[0][0]), [column_obj])



def reduce_identical_datatypes(x, y):
    if(x[0] == 'I' or x[0] == 'R'):
        x[6].extend(y[6])
        red_top5cnt = sorted(x[6], key = lambda x: x[0], reverse = True)[:5]
        return (x[0], x[1] + y[1], x[2] + y[2], max(x[3], y[3]), min(x[4], y[4]), x[5] + y[5], red_top5cnt, x[7] + y[7])
    elif(x[0] == 'T'):
        x[3].extend(y[3])
        x[5].extend(y[5])
        x[6].extend(y[6])
        # top 5 frequent counts
        red_top5cnt = sorted(x[3], key = lambda x: x[0], reverse = True)[:5]
        # longest 5
        red_long5 = sorted(x[5], key = lambda x: x[0], reverse = True)[:5]
        # shortest 5
        red_short5 = sorted(x[6], key = lambda x: x[0], reverse = False)[:5]
        return ('T', x[1] + y[1], x[2] + y[2], red_top5cnt, x[4] + y[4], red_long5, red_short5)
    elif(x[0] == 'D'):
        x[4].extend(y[4])
        red_top5cnt = sorted(x[4], key = lambda x: x[0], reverse = True)[:5]
        if(x[2][1] > y[2][1]):
            mx = x[2]
        else:
            mx = y[2]
        if(x[3][1] > y[3][1]):
            mn = y[3]
        else:
            mn = x[3]
        return ('D', x[1] + y[1], mx, mn, red_top5cnt, x[5] + y[5])
    elif(x[0] == 'None'):
        return ('None', x[1] + y[1])



def reduce_identical_vals(x, y):
    if(x[0] == 'I' or x[0] == 'R'):
        # type, sum, count, min, max, sum of squares
        return (x[0], x[1] + y[1], x[2] + y[2], x[3], x[4], x[5] + y[5])
    elif(x[0] == 'T'):
        # type, sum, count
        return ('T', x[1] + y[1], x[2] + y[2])
    elif(x[0] == 'D'):
        # type, parsed_date, count
        return ('D', x[1], x[2] + y[2])
    elif(x[0] == 'None'):
        return ('None', x[1] + y[1])



def reduce_identical_columns(x, y):
    x.update(y)
    return x



def reduce_identical_datasets(x, y):
    x.extend(y)
    return x



def process_dataset_rdd(dataset_rdd):
    # maps int/real, text, date, None datatypes with appropriate values to calculate
    # respective statistics in future
    dataset_map1 = dataset_rdd.flatMap(mapper_identical_vals)
    print("map 1 complete")
    # reduce to unique values
    dataset_red1 = dataset_map1.reduceByKey(reduce_identical_vals)
    print("reduce 1 complete")
    # map to group elements by their data types
    dataset_map2 = dataset_red1.map(mapper_identical_datatypes)
    print("map 2 complete")
    # reduce to calculate sum, count, min, max, sum of squres
    dataset_red2 = dataset_map2.reduceByKey(reduce_identical_datatypes)
    print("reduce 2 complete")
    # calculate mean, max, min, stdev, top_5_cnt_list, dist_cnt
    dataset_map3 = dataset_red2.map(mapper_identical_columns)
    print("map 3 complete")
    # group by columns
    dataset_red3 = dataset_map3.reduceByKey(reduce_identical_columns)
    print("reduce 3 complete")
    # generate required output structure of info of each column
    dataset_map4 = dataset_red3.map(mapper_identical_datasets)
    # groupby datasets
    dataset_red4 = dataset_map4.reduceByKey(reduce_identical_datasets)
    return dataset_red4.collect()



def process_datasets():
    start_time = time.time()
    df_datasets = spark.read.csv("/user/hm74/NYCOpenData/datasets.tsv",header=False,sep="\t")
    processed_dataset_cnt = 0
    #for dataset_row in sorted(df_datasets.collect())[1000:1300]:
    datasets_list = ['xjtr-h2x2', 'y7az-s7wc', 'ydkf-mpxb', 'yini-w76t', 'yjxr-fw8i']
    for dataset_fname in datasets_list:
        # this line required if running in normal mode!
        #dataset_fname = dataset_row[0]
        print(dataset_fname)
        print(time.time())
        dataset = spark.read.csv("/user/hm74/NYCOpenData/" + dataset_fname + ".tsv.gz", header = True, sep = "\t")
        col_list = dataset.columns
        dataset = dataset.withColumn("dataset_name", lit(dataset_fname))
        dataset_rdd = dataset.rdd
        #dataset_rdd.repartition(8)
        dsinfo = process_dataset_rdd(dataset_rdd)[0][1]
        dsinfo.sort(key = lambda x: x['column_name'], reverse = False)
        ds_obj = {"dataset_name": dataset_fname, "columns": [], "key_column_candidates": []}
        for coli in range(len(dsinfo)):
            cur_col = dsinfo[coli]
            cur_col["column_name"] = col_list[coli]
            ds_obj["columns"].append(cur_col)
            if(cur_col["number_non_empty_cells"] == cur_col["number_distinct_values"] and cur_col['number_empty_cells'] == 0):
                ds_obj["key_column_candidates"].append(col_list[coli])
        with open(dataset_fname + '.json', 'w+') as json_out_file:
            json.dump(ds_obj, json_out_file)
        processed_dataset_cnt += 1
        #break
        #if(processed_dataset_cnt == 10):
        #    break
    print(time.time() - start_time)



def process_datasets_parallel():
    start_time = time.time()
    df_datasets = spark.read.csv("/user/hm74/NYCOpenData/datasets.tsv",header=False,sep="\t")
    processed_dataset_cnt = 0
    datasets_rdd_list = []
    datasets_dict = {}
    for dataset_i, dataset_row in enumerate(sorted(df_datasets.collect())):
        dataset_fname = dataset_row[0]
        print(dataset_fname)
        dataset = spark.read.csv("/user/hm74/NYCOpenData/" + dataset_fname + ".tsv.gz", header = True, sep = "\t")
        datasets_dict[dataset_fname] = dataset.columns
        dataset = dataset.withColumn("dataset_name", lit(dataset_fname))
        datasets_rdd_list.append(dataset.rdd)
        processed_dataset_cnt += 1
        #if(processed_dataset_cnt == 10):
        #    break
    datasets_rdd = sc.union(datasets_rdd_list)
    print(datasets_rdd.getNumPartitions())
    print(sc._jsc.sc().getExecutorMemoryStatus().keySet().size())
    datasets_rdd.repartition(64)
    print(time.time() - start_time)
    #print(datasets_rdd.count())
    dssinfo = process_dataset_rdd(datasets_rdd)
    for dsi in range(len(dssinfo)):
        dsinfo = dssinfo[dsi][1]
        dataset_fname = dssinfo[dsi][0]
        col_list = datasets_dict[dataset_fname]
        dsinfo.sort(key = lambda x: x['column_name'], reverse = False)
        ds_obj = {"dataset_name": dataset_fname, "columns": [], "key_column_candidates": []}
        for coli in range(len(dsinfo)):
            cur_col = dsinfo[coli]
            cur_col["column_name"] = col_list[coli]
            ds_obj["columns"].append(cur_col)
            if(cur_col["number_non_empty_cells"] == cur_col["number_distinct_values"] and cur_col['number_empty_cells'] == 0):
                ds_obj["key_column_candidates"].append(col_list[coli])
        with open(dataset_fname + '.json', 'w+') as json_out_file:
            json.dump(ds_obj, json_out_file)
    print(time.time() - start_time)


#process_datasets()
process_datasets_parallel()
