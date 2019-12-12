# get all the tables we need to go over
# for each table, get the column we need to group
# group the column by values
# output results
# update tables using the array

from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
from csv import reader
from replace_col_name import replace 
if __name__ == "__main__":
	# get a list of tables and columns
	f = open("./cluster1.txt","r")
	contents = f.read()
	array = contents.split(",")
	tb_col = []
	txt = []
	for item in array:
		# print (item)
		# tc = item[2:len(item)-1]
		# print(tc)
		txt.append(item)
		tc = item.split("'")[1]
		if len(item.split("'")) > 3:
			tc = item[2:len(item)-1]	
		table = tc.split(".")[0]
		col = tc.split(".")[1]
		# Name_arr = col.split("_")
		# col = ""
		# for k in Name_arr:
		#	col = col + k + " "
		# change col name into space delimited
		pair = (table,col.strip())
		tb_col.append(pair)
	
	sc = SparkContext()
	spark = SparkSession \
	.builder \
	.appName("final-project") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()
	errors = []
	for i in range(0,len(tb_col)):
		print("this is the "+str(i+1) +" table out of "+ str(len(tb_col))+" tables")
		item = tb_col[i]
		f_in_txt = txt[i]
		try:
			# load table into database
			table = spark.read.format('csv')\
			.options(delimiter="\t",header='true',inferschema='true')\
			.load("/user/hm74/NYCOpenData/"+item[0]+".tsv.gz")
			newtable = table
			for col in table.columns:
				newtable = newtable.withColumnRenamed(col,col.replace(" ","_"))
			table = newtable
			table.createOrReplaceTempView("table") # create table
			table_name = item[0]
			col_name = item[1]
			print ("grouping file:" + table_name) # print output message
			query = "select `"+item[1]+"`, count(`"+item[1]+"`) as count_col from table group by `"+ item[1]+"`"
			tcp_interactions = spark.sql(query)
			result = tcp_interactions.collect()
			file = []
			file.append("total number of rows in this file: "+str(tcp_interactions.count()))
			for entry in result:
				item = str(entry[0])+","+str(entry[1])
				file.append(item)
			file = sc.parallelize(file)
			file.saveAsTextFile("labelling-sql/"+table_name+"_"+col_name+".out")
			f_in_txt = f_in_txt+","	
			replace(f_in_txt,"")
		except Exception as inst:
			table_item = (item,inst)
			print(str(table_item).encode("utf-8"))
			print(str(inst).encode("utf-8"))
			print("--------------------------------------------------------------------------")
			print(" ")
			errors.append(table_item)		
	sc.stop()
	labelling = [("xxx","xxx")] 
