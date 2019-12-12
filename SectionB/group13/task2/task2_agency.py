import sys
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

import pickle
import pandas as pd
import csv

from functools import reduce
from string import printable
from datetime import datetime

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession \
        .builder \
        .appName("task2_2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

# start = int(sys.argv[1])
# end = int(sys.argv[2])
# cnt = start

pkl_dict=pickle.load(open('color_dict.pkl','rb'))

color1_=pkl_dict['color1']
color2_=pkl_dict['color2']





# nfile=len(filenames)

# split(' '), along,at,

# borough_=['K','M','Q','R','X','Bronx','Manhattan','Queens','Staten Island']
# borough_=['k','m','q','r','x','bronx','manhattan','queens','staten island']


nulltype=['other','n/a','nan','unspecified','unknown','no name','noname','tbd','.','-','_'] # check tbd when save in json

folder='/user/hm74/NYCOpenData/'

# borough, neighborhood, city, country, --> village

# def checkfloat(v):
# 	try:
# 		float(v)
# 		return True
# 	except ValueError:
# 		return False


# def semantic_type(v):
# 	try:
# 		datetime.strptime( v, '%m%d%Y')
# 		return 'data_time'
# 	except ValueError:
# 		return 'city_agency'

def semantic_type_color(v):
	if v in color1_+color2_:
		return 'color'
	else:
		for c in color2_:
			if set(v).issubset(set(c)):
				return 'color'
	return None

#####################################################################################
## 
# '''


# filename='8jfz-tjny'
# colname='Agency'
# id_=261

# filename='a6zp-tcs3'
# colname='Agency'
# id_=262

filename='pvqr-7yc4'
colname='Vehicle Color'
id_=265



tsv_table=pd.read_table('NYCOpenData/'+filename+'.tsv.gz')
tsv_columns=tsv_table.columns
mySchema=StructType([StructField(col_,StringType(),True) for col_ in tsv_columns])
tsv_df = sqlContext.createDataFrame(tsv_table,schema=mySchema)

new_columns=list()
for clmn in tsv_columns:
	new_name = clmn.replace('\n','')
	new_name=''.join(ch for ch in new_name if ch in printable)
	new_columns.append(new_name)

tsv_df=reduce(lambda data, idx: data.withColumnRenamed(tsv_columns[idx], new_columns[idx]), range(len(tsv_columns)), tsv_df)

colname=''.join(ch for ch in colname if ch in printable)

col_df=tsv_df.select(F.col(colname))
clean_df=col_df.where(F.col(colname).isNotNull())

rm_df=clean_df.where((F.lower(F.col(colname)) =='.')|(F.lower(F.col(colname)) =='-')|(F.lower(F.col(colname)) =='_')|(F.lower(F.col(colname)) ==nulltype[0])|(F.lower(F.col(colname)) ==nulltype[1])|(F.lower(F.col(colname)) ==nulltype[2])|(F.lower(F.col(colname)) ==nulltype[3])|(F.lower(F.col(colname)) ==nulltype[4])|(F.lower(F.col(colname)) ==nulltype[5])|(F.lower(F.col(colname)) ==nulltype[6]) |(F.lower(F.col(colname)) ==nulltype[7]))
if rm_df.count()>0:
	clean_df=clean_df.subtract(rm_df)

trans=str.maketrans("","",'/-&)(`*#')

clean_rdd=clean_df.rdd \
	.map(lambda x: x[0].lower().translate(trans)) \
	.map(lambda x: x.replace('   ',' ')) \
	.map(lambda x: x.replace('  ',' ')) \
	.map(lambda x:(x,1)) \
	.reduceByKey(lambda x,y: x+y) \
	.sortBy(lambda x: x[0],True)


clean_list=clean_rdd.collect()
value_list=[row[0] for row in clean_list]
count_list=[row[1] for row in clean_list]


label_list=list()
for idx,v in enumerate(value_list):
	ct=count_list[idx]
	label=semantic_type_color(v)
	if label != None:
		label_list.append([v,label,ct])


with open('labels_rest/'+filename+'_'+"{0:0=3d}".format(id_)+'.csv','w',newline='', encoding='utf-8') as f:
	writer=csv.writer(f)
	writer.writerow(['value','label','count'])
	writer.writerows(label_list)


# '''
################################
# '''

# for filename in filenames[start:end]:
# 	colname=colnames[cnt]
# 	id_=ids[cnt]
# 	print("="*40)
# 	print("Processing file: %s %s id=%d)" % (filename,colname.encode("utf-8"),id_))


# 	tsv_table=pd.read_table('NYCOpenData/'+filename+'.tsv.gz')
# 	tsv_columns=tsv_table.columns
# 	mySchema=StructType([StructField(col_,StringType(),True) for col_ in tsv_columns])
# 	tsv_df = sqlContext.createDataFrame(tsv_table,schema=mySchema)

# 	new_columns=list()
# 	for clmn in tsv_columns:
# 		new_name = clmn.replace('\n','')
# 		new_name=''.join(ch for ch in new_name if ch in printable)
# 		new_columns.append(new_name)

# 	tsv_df=reduce(lambda data, idx: data.withColumnRenamed(tsv_columns[idx], new_columns[idx]), range(len(tsv_columns)), tsv_df)

# 	colname=''.join(ch for ch in colname if ch in printable)

# 	col_df=tsv_df.select(F.col(colname))
# 	clean_df=col_df.where(F.col(colname).isNotNull())

# 	rm_df=clean_df.where((F.lower(F.col(colname)) =='.')|(F.lower(F.col(colname)) =='-')|(F.lower(F.col(colname)) =='_')|(F.lower(F.col(colname)) ==nulltype[0])|(F.lower(F.col(colname)) ==nulltype[1])|(F.lower(F.col(colname)) ==nulltype[2])|(F.lower(F.col(colname)) ==nulltype[3])|(F.lower(F.col(colname)) ==nulltype[4])|(F.lower(F.col(colname)) ==nulltype[5])|(F.lower(F.col(colname)) ==nulltype[6]) |(F.lower(F.col(colname)) ==nulltype[7]))
# 	if rm_df.count()>0:
# 		clean_df=clean_df.subtract(rm_df)
# 	# clean_df=clean_df.select(F.col(colname).alias("value"))

# 	# trans1=str.maketrans("","",')(`*#')
# 	trans=str.maketrans("","",'-&/)(`*#')

# 	clean_rdd=clean_df.rdd \
# 		.map(lambda x: x[0].lower().translate(trans)) \
# 		.map(lambda x: x.replace('   ',' ')) \
# 		.map(lambda x: x.replace('  ',' ')) \
# 		.map(lambda x:(x,1)) \
# 		.reduceByKey(lambda x,y: x+y) \
# 		.sortBy(lambda x: x[0],True)


# 	clean_list=clean_rdd.collect()
# 	value_list=[row[0] for row in clean_list]
# 	count_list=[row[1] for row in clean_list]


# 	label_list=list()
# 	for idx,v in enumerate(value_list):.p
# 		ct=count_list[idx]
# 		label=semantic_type(v)
# 		if label != None:
# 			label_list.append([v,label,ct])

# 	# gt_rdd=sc.parallelize(label_list) \
# 	#     .sortBy(lambda x: x[1],True) # -> True

# 	# gt_list=[[row[0],row[1],row[2]]  for row in gt_rdd.collect()]

# 	with open('labels_city/'+filename+'_'+"{0:0=3d}".format(id_)+'.csv','w',newline='', encoding='utf-8') as f:
# 		writer=csv.writer(f)
# 		writer.writerow(['value','label','count'])
# 		writer.writerows(label_list)


# 	cnt+=1
