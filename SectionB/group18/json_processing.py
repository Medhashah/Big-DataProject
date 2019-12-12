import json
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

int_mean_values = []
int_max_values = []
int_min_values = []
int_stddev_values = []
real_mean_values = []
real_max_values = []
real_min_values = []
real_stddev_values = []
text_avg_lengths = []

int_cols = 0
real_cols = 0
text_cols = 0
datetime_cols = 0

int_real_cols = 0
int_text_cols = 0
int_datetime_cols = 0
real_text_cols = 0
real_datetime_cols = 0
text_datetime_cols = 0

int_real_text_cols = 0
int_real_datetime_cols = 0
int_text_datetime_cols = 0
real_text_datetime_cols = 0

all_4_cols = 0

inc_int = False
inc_real = False
inc_text = False
inc_datetime = False

json_data = open("complete.json") # Adjust filename as needed
data = json.load(json_data)
for dataset in data["datasets"]:
	for column in dataset["columns"]:
		types_inc = [0, 0, 0, 0]
		for data_type in column["data_types"]:
			if data_type["type"] == "INTEGER (LONG)":
				types_inc[0] = 1
				int_mean_values.append(data_type["mean"])
				int_max_values.append(data_type["max_value"])
				int_min_values.append(data_type["min_value"])
				int_stddev_values.append(data_type["stddev"])

			if data_type["type"] == "REAL":
				types_inc[1] = 1
				real_mean_values.append(data_type["mean"])
				real_max_values.append(data_type["max_value"])
				real_min_values.append(data_type["min_value"])
				real_stddev_values.append(data_type["stddev"])
				real_cols += 1
				inc_real = True
			if data_type["type"] == "TEXT":
				types_inc[2] = 1
				text_avg_lengths.append(data_type["average_length"])
				text_cols += 1
				inc_text = True
			if data_type["type"] == "DATE/TIME":
				types_inc[3] = 1
				datetime_cols += 1
				inc_datetime = True
		if types_inc == [0, 0, 0, 1]:
			datetime_cols += 1
		elif types_inc == [0, 0, 1, 0]:
			text_cols += 1
		elif types_inc == [0, 0, 1, 1]:
			text_datetime_cols += 1
		elif types_inc == [0, 1, 0, 0]:
			real_cols += 1
		elif types_inc == [0, 1, 0, 1]:
			real_datetime_cols += 1
		elif types_inc == [0, 1, 1, 0]:
			real_text_cols += 1
		elif types_inc == [0, 1, 1, 1]:
			real_text_datetime_cols += 1
		elif types_inc == [1, 0, 0, 0]:
			int_cols += 1
		elif types_inc == [1, 0, 0, 1]:
			int_datetime_cols += 1
		elif types_inc == [1, 0, 1, 0]:
			int_text_cols += 1
		elif types_inc == [1, 0, 1, 1]:
			int_text_datetime_cols += 1
		elif types_inc == [1, 1, 0, 0]:
			int_real_cols += 1
		elif types_inc == [1, 1, 0, 1]:
			int_real_datetime_cols += 1
		elif types_inc == [1, 1, 1, 0]:
			int_real_text_cols += 1
		elif types_inc == [1, 1, 1, 1]:
			all_4_cols += 1

# Plots were created one at a time, in order to save and include in report
# Uncomment whichever one is needed

### Good plot: Co-occurance of different data types:
'''
names = ['just int', 'just real', 'just text', 'just datetime', 'int+real', 'int+text', 'int+datetime', 'real+text', 'real+datetime', 'text+datetime', 'all but datetime', 'all but text', 'all but real', 'all but int', 'all 4']
values = [int_cols, real_cols, text_cols, datetime_cols, int_real_cols, int_text_cols, int_datetime_cols, real_text_cols, real_datetime_cols, text_datetime_cols, int_real_text_cols, int_real_datetime_cols, int_text_datetime_cols, real_text_datetime_cols, all_4_cols]
plt.barh(names[::-1], values[::-1])
plt.title("Data type co-occurance")
plt.xlabel("Number of columns")
plt.show()
'''

### Good plot: distribution of avg text lengths:

plt.hist(text_avg_lengths, bins=list(range(0,200,2)))
plt.title("Average text lengths")
plt.xlabel("Average length of text entries in a column, in characters")
plt.ylabel("Frequency")
plt.show()
