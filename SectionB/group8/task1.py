import os
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from csv import reader
import time
import json
import subprocess

spark = SparkSession.builder.appName("generic_profiling").config('spark.driver.memory', '10g').config('spark.executor.cores', 5).config('spark.executor.memory', '10g').getOrCreate()



def generic_profiling(path, start_time, n):  #function that performs generic profiling.


    def stat1(df):                                  
        
        def number_non_empty_cells(col):           
            return F.count(col)

        def number_empty_cells(col):                   
            return F.count(F.when(F.isnull(col), col))

        def number_distinct_values(col):                
            return F.approxCountDistinct(col)

        def frequent_values(col):                

            @F.udf
            def f1(v):
                if len(v) > 0:
                    from collections import Counter
                    x = [w[0] for w in Counter(v).most_common(5)]
                    return x
                else:
                    return None

            return f1(F.collect_list(col))

        def integer_count(col):                                    
            x = col.cast('integer')
            return F.when(F.count(x) == 0, None).otherwise(F.count(x))

        def integer_max_value(col):                                
            x = col.cast('integer')
            return F.max(x)

        def integer_min_value(col):                            
            x = col.cast('integer')
            return F.min(x)

        def integer_mean(col):                                     
            x = col.cast('integer')
            return F.mean(x)

        def integer_stddev(col):                                
            x = col.cast('integer') 
            return F.stddev(x)

        def real_count(col):                               
            x = col.cast('double')
            return F.when(F.count(x) == 0, None).otherwise(F.count(x))

        def real_max_value(col):                           
            x = col.cast('double')
            return F.max(x)

        def real_min_value(col):                           
            x = col.cast('double')
            return F.min(x)

        def real_mean(col):                            
            x = col.cast('double')
            return F.mean(x)

        def real_stddev(col):                           
            x = col.cast('double')
            return F.stddev(x)
        
        def date_count(col):                           
            x = col.cast('string')
            return F.when(F.count(F.to_date(x, 'MM/dd/yyyy')) == 0, None).otherwise(F.count(F.to_date(x, 'MM/dd/yyyy')))
        
        def date_max_value(col):                      
            x = col.cast('string')
            return F.max(F.to_date(x, 'MM/dd/yyyy'))
        
        def date_min_value(col):                        
            x = col.cast('string')
            return F.min(F.to_date(x, 'MM/dd/yyyy'))
        
        def string_count(col):                         
            x = col.cast('string')
            return F.count(x)
        
        def string_shortest_values(col):               
    
            @F.udf
            def f2(l, m):
                if len(l) > 0:
                    if isinstance(l[0], str):
                        x = list(set(l))
                        x.sort(key = lambda s: len(s))
                        return x[:5]
                    else:
                        return None
                else:
                    return None

            return f2(F.collect_list(col), F.min(F.length(col)))
        
        def string_longest_values(col):             
    
            @F.udf
            def f2(l, m):
                if len(l) > 0:
                    if isinstance(l[0], str):
                        x = list(set(l))
                        x.sort(key = lambda s: len(s))
                        return x[-5:]
                    else:
                        return None
                else:
                    return None

            return f2(F.collect_list(col), F.max(F.length(col)))
        
        def string_average_length(col):             
            return F.avg(F.length(col))
        
        def get_s(df):
            
            if df.count() <= 100000:              
                funs = [number_non_empty_cells, 
                           number_empty_cells,
                           number_distinct_values,     
                           frequent_values,
                           integer_count,
                           integer_max_value,
                           integer_min_value,
                           integer_mean,
                           integer_stddev,
                           real_count,
                           real_max_value,
                           real_min_value,
                           real_mean,
                           real_stddev,
                           date_count,
                           date_max_value,
                           date_min_value,
                           string_count,
                           string_shortest_values,
                           string_longest_values,
                           string_average_length]
            else:
                funs = [   number_non_empty_cells,    
                           number_empty_cells,         
                           number_distinct_values,
                           integer_count,
                           integer_max_value,
                           integer_min_value,
                           integer_mean,
                           integer_stddev,
                           real_count,
                           real_max_value,
                           real_min_value,
                           real_mean,
                           real_stddev,
                           date_count,
                           date_max_value,
                           date_min_value,
                           string_count,
                           string_average_length]
            
            columns = df.columns
            
            schema = {}
            for temp in df.dtypes:
                schema[temp[0]] = temp[1]    
            
            def exp():                        
                t = []
                for c in columns:
                    if ('.' in c):
                        continue
                    else:
                        for f in funs:        
                            if f.__name__ in ['number_non_empty_cells','number_empty_cells','number_distinct_values','frequent_values','integer_count','integer_max_value','integer_min_value','integer_mean','integer_stddev', 'real_count',
                            'real_max_value','real_min_value','real_mean','real_stddev', 'date_count','date_max_value',
                            'date_min_value']:
                                t.append(f(F.col(c)).alias('{0}_{1}'.format(f.__name__, c)))
                            elif schema[c] == 'string' and f.__name__.split('_')[0] == 'string': 
                                t.append(f(F.col(c)).alias('{0}_{1}'.format(f.__name__, c)))
                return t
        
            ex = iter(exp())
            
           
            
            f_v = {}
            l_v = {}
            s_v = {}
            if df.count() > 100000:                            
                e = spark.createDataFrame([(['I#'],)], ['a'])   
                e1 = spark.createDataFrame([(['a'],)], ['a'])
                e2 = spark.createDataFrame([(['a'],)], ['a'])
                for c in df.columns:
                    if ('.' in c):
                        continue
                    else:
                        e = e.union(df.groupBy(F.col(c)).count().sort(F.desc("count")).limit(5).select(F.collect_list(F.col(c)))) #calculating the frequent values
                        if schema[c] == 'string':  
                            e2 = e2.union(df.withColumn("len", F.length(F.col(c))).sort(F.desc("len")).select(F.col(c), 'len').distinct().limit(5).select(F.collect_list(F.col(c)))) #longest values
                            e1 = e1.union(df.withColumn("len", F.length(F.col(c))).sort(F.asc("len")).select(F.col(c), 'len').distinct().limit(5).select(F.collect_list(F.col(c))))  #shortest values
                
                z = e.collect()      
                z1 = e1.collect()
                z2 = e2.collect()
        
                i = 1
                j = 1     
                for c in df.columns:
                    f_v['{0}_{1}'.format('frequent_values',c)] = z[i][0]
                    if schema[c] == 'string':
                        l_v['{0}_{1}'.format('longest_values',c)] = z2[j][0]
                        s_v['{0}_{1}'.format('shortest_values',c)] = z1[j][0]
                        j = j + 1
                    i = i + 1

            
            return df.agg(*ex).toJSON().first(), f_v, l_v, s_v 
        
        def get_l(s, f_v, l_v, s_v):
            
            
            
            r = json.loads(s)      
            
            funs = [number_non_empty_cells,   
                   number_empty_cells,
                   number_distinct_values,
                   frequent_values,
                   integer_count,
                   integer_max_value,
                   integer_min_value,
                   integer_mean,
                   integer_stddev,
                   real_count,
                   real_max_value,
                   real_min_value,
                   real_mean,
                   real_stddev,
                   date_count,
                   date_max_value,
                   date_min_value,
                   string_count,
                   string_shortest_values,
                   string_longest_values,
                   string_average_length]
            
            main_labels = ['number_non_empty_cells',      
                             'number_empty_cells', 
                             'number_distinct_values',
                             'frequent_values']
            integer_labels = ['integer_count',
                            'integer_max_value',
                            'integer_min_value',
                            'integer_mean',
                            'integer_stddev']
            real_labels = ['real_count',
                            'real_max_value',
                            'real_min_value',
                            'real_mean',
                            'real_stddev']
            date_labels = ['date_count',
                            'date_max_value',
                            'date_min_value']
            string_labels = ['string_count',
                               'string_shortest_values',
                               'string_longest_values',
                               'string_average_length']
            
            schema = {}
            for temp in df.dtypes:
                schema[temp[0]] = temp[1]      
            
            l = []
            for c in df.columns:
                main = {}
                integer = {}                   
                real = {}
                date = {}
                string = {}
                main['column_name'] = c
                integer['type'] = 'INTEGER (LONG)'
                real['type'] = 'REAL'
                date['type'] = 'DATE/TIME'
                string['type'] = 'TEXT'
                for f in funs:                #store the results into a dictionary
                    if '{0}_{1}'.format(f.__name__, c) in r.keys() and f.__name__ in main_labels:
                        main[f.__name__] = r['{0}_{1}'.format(f.__name__, c)]
                    elif '{0}_{1}'.format(f.__name__, c) in r.keys() and f.__name__ in integer_labels:
                        integer[f.__name__[f.__name__.index('_')+1:]] = r['{0}_{1}'.format(f.__name__, c)]
                    elif '{0}_{1}'.format(f.__name__, c) in r.keys() and f.__name__ in real_labels:
                        real[f.__name__[f.__name__.index('_')+1:]] = r['{0}_{1}'.format(f.__name__, c)]
                    elif '{0}_{1}'.format(f.__name__, c) in r.keys() and f.__name__ in date_labels:
                        date[f.__name__[f.__name__.index('_')+1:]] = r['{0}_{1}'.format(f.__name__, c)]
                    elif '{0}_{1}'.format(f.__name__, c) in r.keys() and f.__name__ in string_labels:
                        string[f.__name__[f.__name__.index('_')+1:]] = r['{0}_{1}'.format(f.__name__, c)]
                if bool(f_v):  #add values of frequent values and longest and shortest values for rows > 100000
                    main['frequent_values'] = f_v['{0}_{1}'.format('frequent_values',c)]
                if bool(l_v) and schema[c] == 'string' and '{0}_{1}'.format('longest_values',c) in l_v.keys():
                    string['longest_values'] = l_v['{0}_{1}'.format('longest_values',c)]
                if bool(s_v) and schema[c] == 'string' and '{0}_{1}'.format('shortest_values',c) in s_v.keys():
                    string['shortest_values'] = s_v['{0}_{1}'.format('shortest_values',c)]

                temp = []
                if 'integer_count_{0}'.format(c) in r.keys():
                    temp.append(integer)
                if 'real_count_{0}'.format(c) in r.keys():
                    temp.append(real)
                if 'date_count_{0}'.format(c) in r.keys():
                    temp.append(date)
                if schema[c] == 'string':
                    temp.append(string)
                main['data_types'] = temp
                l.append(main)
                
            return l                               
        
        s, f2, f3, f4 = get_s(df)
        l = get_l(s, f2, f3, f4)
        
        return l                              

        
    
    
    # check if the files exists in the path given and infer the name of the files 
    cmd = 'hdfs dfs -ls {}'.format(path)
    files = subprocess.check_output(cmd, shell=True).decode("utf-8").strip().split('\n')
    files = list(files)
    files = files[1:]       
    data_order = {}
    for f in files:
        tk = f.split(' ')
        data_order[tk[-1]] = int(tk[-4])
    import operator
    data_order_x = sorted(data_order.items(), key=operator.itemgetter(1))  #sort the filenames in order of increasing size
    print('files success')

 
    dataset = spark.read.format('csv').option("delimiter", "\t").option("header", "false").option("inferschema", "true").csv(str(path + '/' + 'datasets.tsv'))
    dataset.createOrReplaceTempView("dataset")
    if dataset:
        print("dataset success")
        

    #s = []
    name = ''
    i = 1
    for ftk in data_order_x:   
        if i <= n:
            filename = ftk[0]               
            if filename.endswith(".gz"):
                
                if i in (1597, 1688, 1697, 1716, 1766, 1771, 1787, 1788, 1812, 1816, 1817, 1824): #skip some problematic datasets
                    print('Skipped  Dataset at index ',str(i))
                    i = i + 1
                    continue
                
                
                f = filename[filename.rfind('/'):]   
                f = f[1:]
                t = {}
                df = spark.read.format('csv').option("delimiter", "\t").option("header", "true").option("inferschema", "true").csv(str(filename))   #read the dataframe
                
                name = spark.sql('select _c1 as name from dataset where _c0 = "{0}"'.format(str(f[:f.index(".")]))).toPandas()["name"][0]   #get the name of the dataset
                print("Dataset", str(f),"at index ",str(i),", name: ",str(json.dumps(name)))
                print("No of rows: ", str(df.count()))
                print("No of columns: ",str(len(df.columns)))
                
                
                t['dataset_name'] = name        
#                
                t['columns'] = stat1(df)           
                with open('task1.json', 'a') as fp:  
                    json.dump(t, fp)
                
#               #print the time taken to process the dataset
                
                print("Completed dataset " + f + ' ' + str(time.time() - start_time))
                print("Completed {0} of {1}".format(i, n))
                start_time = time.time()
                i = i + 1
        else:
            break

    return 0

path = str(sys.argv[1])  
n = int(sys.argv[2])      


start_time = time.time()
generic_profiling(path, start_time, n)   
print("--- %s seconds ---" % (time.time() - start_time))  


spark.stop() 


path_to_folder = str(sys.argv[1])
num_datasets_to_compute = int(sys.argv[2])

process_start_time = time.time()
general_profiling_task1(path_to_folder, process_start_time, num_datasets_to_compute)
print("--- %s seconds ---" % (time.time() - process_start_time))
spark.stop()
