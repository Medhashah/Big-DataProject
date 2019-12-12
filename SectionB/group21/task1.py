from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import types
import json
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("task1").config("spark.some.config.option","some-value").getOrCreate()

# Read all file name
from csv import reader,writer
file_df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").option("inferSchema","true").load("/user/hm74/NYCOpenData/datasets.tsv")
file_df = file_df.toDF("file_name","dataset_name")
file_name_list = file_df.select("file_name").collect()
f_name_list = [f[0] for f in file_name_list]
dataset_name_list = file_df.select("dataset_name").collect()

no_data = ["no data"]
for fname in f_name_list:
  dataset_name = dataset_name_list[f_name_list.index(fname)]
  # Get the file path according to the file name
  path = "/user/hm74/NYCOpenData/" + str(fname) + ".tsv.gz"
  df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","true").option("inferSchema","true").load(path)
  # Get names of all columns
  header = df.columns
  try:
      # Judge whether the type of input is float
      def isFloat(x):
          try:
              float(x)
              return True
          except:
              return False

      # Judge whether the type of input is int
      def isInt(x):
          try:
              if float(x) - long(x) > 0:
                  return False
              else:
                  return True
          except:
              return False

      # Judge whether the type of input is date
      import datetime
      def isDate(x):
          Mon = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']
          Month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                   'November', 'December']
          try:
              # Split the input using different format
              if '-' in x:
                  x = x.split('-')
              if ',' in x:
                  x = x.split(',')
              if ' ' in x:
                  x = x.split(' ')
              if '/' in x:
                  x = x.split('/')
              # Deal with different formats of date
              temp = []
              for xs in x:
                  if 'th' in xs:
                      temp.append(xs.split('th'))
                  if xs in Mon:
                      temp.append(Mon.index(xs))
                  else:
                      if xs in Month:
                          temp.append(Month.index(xs))
                      else:
                          temp.append(xs)
              # Date type can only include month and day or year, month and day
              if (len(temp) == 2):
                  d1 = datetime.date(2019, int(temp[0]), int(temp[1]))
                  return True
              if (len(temp) == 3):
                  d1 = datetime.date(int(temp[1]), int(temp[2]), int(temp[0]))
                  return True
          except:
              return False

      # Judge whether the type of input is time
      def isTime(x):
          try:
              if 'AM' in x:
                  x = x.split('AM')
              if 'PM' in x:
                  x = x.split('PM')
              x = x.split(':')
              if (len(x) == 3):
                  datetime.time(x[0], x[1], x[2])
                  return True
              else:
                  return False
          except:
              return False

      # Judge the data type
      def data_types(k):
          k = k[0]
          if isDate(k) or isTime(k):
              return ("date/time", k)
          if isFloat(k):
              if isInt(k):
                  return ("int", int(k))
              else:
                  return ("real", float(k))
          else:
              return ("text", k)

      # Judge whether the column contains any Nonetype value
      for i in range(0, len(header)):
          r = df.groupBy(header[i]).count().orderBy('count', ascending=False)
          top2 = r.take(2)
          if len(top2) < 2:
              break
          top1_type = data_types(top2[0])
          top2_type = data_types(top2[1])
          if top1_type[0] != top2_type[0] and ((int(top2[0][1]) - int(top2[1][1])) / len(df.rdd.collect())) > 0.3:
              no_data.append(top1_type[1])

      column_infos = []
      for i in range(0, len(header)):
          # Count the number of values which means nothing but belongs to the data type "String"
          null_number = 0
          count = 0
          for j in no_data:
              number = df.filter(df[header[i]] == j).count()
              if number > 0: count += 1
              null_number += number

          # Number of non-empty cells
          r1 = df.filter(df[header[i]].isNotNull()).count() - null_number

          # Number of empty cells
          r2 = df.filter(df[header[i]].isNull()).count() + null_number

          # Number of distinct values
          r3 = df.select(header[i]).distinct().count() - count

          # Top5 most frequent values
          r4 = df.groupBy(header[i]).count().orderBy('count', ascending=False)

          for j in range(0, len(no_data)):
              if j == 0:
                  r4 = r4.rdd.filter(lambda x: x[0] != no_data[j])
              else:
                  r4 = r4.filter(lambda x: x[0] != no_data[j])
          result4 = []
          if (len(r4.collect()) > 5):
              maxs = 5
          else:
              maxs = len(r4.collect())
          for r in range(0, maxs):
              result4.append(r4.collect()[r])
          r4 = result4

          # Judge the type of data
          def data_types(k):
              k = k[0]
              if isDate(k) or isTime(k):
                  return ("date/time", k)
              if isFloat(k):
                  if isInt(k):
                      return ("int", int(k))
                  else:
                      return ("real", float(k))
              else:
                  return ("text", k)

          # Data types
          schema1 = types.StructType([types.StructField('data_type', types.StringType(), True),
                                      types.StructField(header[i], types.StringType(), True)])
          column_data_types = []
          data_type = df.select(header[i]).rdd.map(data_types)
          data_types = sqlContext.createDataFrame(data_type, schema1).distinct()

          # Integer
          intset = data_types.filter("data_type='int'")
          if len(intset.rdd.collect()) > 0:
              column_data_types.append('Integer(Long)')
              int_max = intset.select(max(header[i])).rdd.collect()[0][0]
              int_min = intset.select(min(header[i])).rdd.collect()[0][0]
              int_mean = intset.select(mean(header[i])).rdd.collect()[0][0]
              int_std = intset.select(stddev(header[i])).rdd.collect()[0][0]
              int_temp = {"type": "INTEGER(LONG)", "count": len(intset.rdd.collect()), "max_value": int_max,
                          "min_value": int_min, "mean": int_mean, "stddev": int_std}
              column_data_types.append(int_temp)

          # Real
          realset = data_types.filter("data_type='real'")
          if len(realset.rdd.collect()) > 0:
              column_data_types.append('REAL')
              real_max = intset.select(max(header[i])).rdd.collect()[0][0]
              real_min = intset.select(min(header[i])).rdd.collect()[0][0]
              real_mean = intset.select(mean(header[i])).rdd.collect()[0][0]
              real_std = intset.select(stddev(header[i])).rdd.collect()[0][0]
              real_temp = {"type": "REAL", "count": len(realset.rdd.collect()), "max_value": real_max,
                           "min_value": real_min, "mean": real_mean, "stddev": real_std}
              column_data_types.append(real_temp)

          # Deal with different formats of date and change them into integer
          def dateNumber(k):
              x = k[1]
              Mon = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
              Month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                       'November', 'December']
              if '-' in x:
                  x = x.split('-')
              if ',' in x:
                  x = x.split(',')
              if ' ' in x:
                  x = x.split(' ')
              if '/' in x:
                  x = x.split('/')
              temp = []
              for xs in x:
                  if 'th' in xs:
                      temp.append(xs.split('th')[0])
                      break
                  if xs in Mon:
                      temp.append(Mon.index(xs) + 1)
                  else:
                      if xs in Month:
                          temp.append(Month.index(xs) + 1)
                      else:
                          temp.append(xs)
                      if len(temp) == 2:
                          return k[1], int(temp[0]) * 100 + int(temp[1])
                      if len(temp) == 3:
                          return k[1], int(temp[0]) * 10000 + int(temp[1]) * 100 + int(temp[2])


          # Date/Time
          dateset = data_types.filter("data_type='date/time'")
          if len(dateset.rdd.collect()) > 0:
              column_data_types.append('DATE/TIME')
              dset = dateset.rdd.map(dateNumber).toDF([header[i], "number"])
              date_max = dset.orderBy('number', ascending=False).select(header[i]).take(1)
              date_min = dset.orderBy('number', ascending=True).select(header[i]).take(1)
              date_temp = {"type": "DATE/TIME", "count": len(dateset.rdd.collect()), "max_value": date_max,
                           "min_value": date_min}
              column_data_types.append(date_temp)

          # Calculate the length of text
          def textLength(k):
              try:
                  length = len(k[1])
                  return k[0], k[1], length
              except:
                  return k[0], k[1], 0

          # Text
          schema = types.StructType([types.StructField('data_type', types.StringType(), True),
                                     types.StructField(header[i], types.StringType(), True),
                                     types.StructField('length', types.IntegerType(), True)])
          textset = data_types.filter("data_type='text'")
          if len(textset.rdd.collect()) > 0:
              textset = textset.rdd.map(textLength)
              textset = sqlContext.createDataFrame(textset, schema).distinct()
              text_max = textset.orderBy('length', ascending=False).select(header[i]).take(5)
              text_min = textset.filter("length > 0").orderBy('length', ascending=True).select(header[i]).take(5)
              text_avg = \
              textset.groupBy('data_type').avg('length').toDF("data_type", "mean").select("mean").rdd.collect()[0][0]
              text_temp = {"type": "TEXT", "count": len(textset.rdd.collect()), "shortest_values": text_min,
                           "longest_values": text_max, "average_length": text_avg}
              column_data_types.append(text_temp)

          column_info = {"column_name": header[i], "number_non_empty_cells": r1, "number_empty_cells": r2,
                         "number_distinct_values": r3, "frequent_values": r4, "data_types": column_data_types}
          column_infos.append(column_info)

      # candidate key
      candidate_keys = []
      keys = sc.parallelize(header).map(lambda x: [x]).collect()

      while len(keys) > 0:
          key = keys
          keys = []
          for k in key:
              column = df.select(k)
              item_count = column.count()
              item_distinct_count = column.distinct().count()

              if item_count == item_distinct_count:
                  candidate_keys.append(k)
              else:
                  index = header.index(k[-1])
                  for i in range(index + 1, len(header)):
                      k.append(header[i])
                      keys.append(k)
                      k = k[0:len(k) - 1]

      dataset_info = {"dataset_name": dataset_name[0],
                      "columns": column_infos,"key_column_candidates":candidate_keys}

      f1 = open('task1.json','a')
      d = json.dumps(dataset_info)
      f1.write(d+'\n')
      f1.close()

      information = [datetime.datetime.now(), str(fname)]
      with open('open_dataset.csv', 'a') as fd:
          w = writer(fd)
          w.writerow(information)
  except:
      dataset_info = {"dataset_name": dataset_name[0], "columns": header}
      f1 = open('task1.json','a')
      d = json.dumps(dataset_info)
      f1.write(d+'\n')
      f1.close()

      information = [datetime.datetime.now(), str(fname)]
      with open('open_dataset.csv', 'a') as fd:
          w = writer(fd)
          w.writerow(information)