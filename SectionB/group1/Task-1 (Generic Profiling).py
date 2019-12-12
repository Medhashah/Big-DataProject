import os
import sys
import pyspark
import string
import csv
import json
import statistics
from itertools import combinations
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as D
from pyspark.sql.window import Window
from dateutil.parser import parse
import datetime

spark = SparkSession.builder.appName("project-part1").config("spark.some.config.option", "some-value").getOrCreate()

if not os.path.exists('Results_JSON'):
    os.makedirs('Results_JSON')

filelist=os.listdir('NYCOpenData/')
filelist1=[]
file_name_dict={}
with open('NYCOpenData/datasets.tsv') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for i,row in enumerate(reader):
        file_name_dict[row[0].lower()]=row[1]
        filelist1.append(row[0]+'.tsv.gz')

filelist.remove('datasets.tsv')

def count_not_null(c, nan_as_null=False):
    pred = F.col(c).isNotNull() & (~isnan(c) if nan_as_null else F.lit(True))
    return F.sum(pred.cast("integer")).alias(c)

get_integer=F.udf(lambda x: x if type(x)==int else None, D.IntegerType())
get_string=F.udf(lambda x: x if type(x)==str else None, D.StringType())
get_float=F.udf(lambda x: x if type(x)==float else None, D.FloatType())

def check_date(d):
    try:
        z=parse(d)
        return str(z)
    except:
        return None

def str_to_int(d):
    if type(d)==str:
        try:
            z=int(d)
            return z
        except:
            return None
    else:
        return None
    
def str_to_float(d):
    if type(d)==str:
        try:
            z=float(d)
            return z
        except:
            return None
    else:
        return None
get_date=F.udf(lambda x: check_date(x), D.StringType())
get_str_int=F.udf(lambda x: str_to_int(x), D.IntegerType())
get_str_float=F.udf(lambda x: str_to_float(x), D.FloatType())

print('files left = ', len(filelist1))
filelist1.remove('xzy8-qqgf')
for file in filelist1:
    filepath='/user/hm74/NYCOpenData/'+file.lower()+'.tsv.gz'
    DF = spark.read.format('csv').options(header='true',inferschema='true').option("delimiter", "\t").load(filepath)
    DF_dict={"dataset_name": file_name_dict[file.split('.')[0]], 'columns': DF.columns, 'key_column_candidates':[]}
    col_name_list=DF.columns 
    col_dicts=[]
    total_rows=DF.count()
    
    for i, x in enumerate(DF.columns):
        DF=DF.withColumnRenamed(x, str(i))
    count_not_null_all_col = DF.agg(*[count_not_null(c) for c in DF.columns]).collect()[0]
    count_null_all_col=[(total_rows-count_notNull) for count_notNull in count_not_null_all_col]
    
     
    for i, cols in enumerate(DF.columns):
        if total_rows==0:
            continue
        col_dict={}
        col_dict["column_name"]=col_name_list[i]
        col_dict['number_non_empty_cells']=count_not_null_all_col[i]
        col_dict['number_empty_cells']=count_null_all_col[i]
        freq_DF_desc=DF.groupBy(cols).count().sort(F.desc('count'))
        freq_DF_desc=freq_DF_desc.where(F.col(cols).isNotNull())
        
        top5_freq=[]
        if freq_DF_desc.count()<5:
            top5_freq=[row[0] for row in freq_DF_desc.collect()]
        else:
            top5_freq=[row[0] for row in freq_DF_desc.take(5)]
        col_dict['frequent_values']=top5_freq
        col_dict['data_types']=[]
        
        int_col=cols+' '+'int_type'
        str_col=cols+' '+'str_type'
        float_col=cols+ ' '+ 'float_type'
        date_col=cols+' '+'date_type'
        str_int_col=cols + ' '+'str_int'
        str_float_col=cols +' '+'str_float'
        df=DF.select([get_integer(cols).alias(int_col), get_string(cols).alias(str_col), get_float(cols).alias(float_col), get_date(cols).alias(date_col),
                     get_str_int(cols).alias(str_int_col),get_str_float(cols).alias(str_float_col)])
        
        int_df=df.select(int_col).where(F.col(int_col).isNotNull())
        str_df=df.select(str_col).where(F.col(str_col).isNotNull())
        float_df=df.select(float_col).where(F.col(float_col).isNotNull())
        date_df=df.select(date_col).where(F.col(date_col).isNotNull())
        str_int_df=df.select(str_int_col).where(F.col(str_int_col).isNotNull())
        str_float_df=df.select(str_float_col).where(F.col(str_float_col).isNotNull())
        
        if float_df.count()>1:
            type_dict={}
            type_dict['type']='REAL'
            type_dict['count']=float_df.count()
            type_dict['max_value']=float_df.agg({float_col: "max"}).collect()[0][0]
            type_dict['min_value']=float_df.agg({float_col: "min"}).collect()[0][0]
            type_dict['mean']=float_df.agg({float_col: "avg"}).collect()[0][0]
            type_dict['stddev']=float_df.agg({float_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
        
        if int_df.count()>1:
            type_dict={}
            type_dict['type']='INTEGER (LONG)'
            type_dict['count']=int_df.count()
            type_dict['max_value']=int_df.agg({int_col: 'max'}).collect()[0][0]
            type_dict['min_value']=int_df.agg({int_col: 'min'}).collect()[0][0]
            type_dict['mean']=int_df.agg({int_col: 'avg'}).collect()[0][0]
            type_dict['stddev']=int_df.agg({int_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
            
        if str_df.count()>1:
            type_dict={'type':'TEXT', 'count': str_df.count()}
            str_rows=str_df.distinct().collect()
            str_arr=[row[0] for row in str_rows]
            if len(str_arr)<=5:
                type_dict['shortest_values']=str_arr
                type_dict['longest_values']=str_arr
                
            else:
                str_arr.sort(key=len, reverse=True)
                type_dict['shortest_values']=str_arr[-5:]
                type_dict['longest_values']=str_arr[:5]
            
            type_dict['average_length']=sum(map(len, str_arr))/len(str_arr)
            col_dict['data_types'].append(type_dict)
        
        if date_df.count()>1:
            type_dict={"type":"DATE/TIME", "count":date_df.count()}
            min_date, max_date = date_df.select(F.min(date_col), F.max(date_col)).first()
            type_dict['max_value']=max_date
            type_dict['min_value']=min_date
            col_dict['data_types'].append(type_dict)
        
        if str_float_df.count()>1:
            type_dict={}
            type_dict['type']='REAL'
            type_dict['count']=str_float_df.count()
            type_dict['max_value']=str_float_df.agg({str_float_col: "max"}).collect()[0][0]
            type_dict['min_value']=str_float_df.agg({str_float_col: "min"}).collect()[0][0]
            type_dict['mean']=str_float_df.agg({str_float_col: "avg"}).collect()[0][0]
            type_dict['stddev']=str_float_df.agg({str_float_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
        
        if str_int_df.count()>1:
            type_dict={}
            type_dict['type']='INTEGER (LONG)'
            type_dict['count']=str_int_df.count()
            type_dict['max_value']=str_int_df.agg({str_int_col: 'max'}).collect()[0][0]
            type_dict['min_value']=str_int_df.agg({str_int_col: 'min'}).collect()[0][0]
            type_dict['mean']=str_int_df.agg({str_int_col: 'avg'}).collect()[0][0]
            type_dict['stddev']=str_int_df.agg({str_int_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
        col_dicts.append(col_dict)
    
    
    json_file_path=file.split('.')[0]
    print('Processed '+ str(c)+' file')
    c=c+1
    json_file_path='Results_JSON/'+ json_file_path+ '.json'
    with open(json_file_path, 'w', newline='\n') as json_file:
        json.dump(DF_dict, json_file)
        for Dict in col_dicts:
            json.dump(Dict, json_file,default=str)
    
    
            
      


