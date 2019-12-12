from pyspark import SparkContext
from dateutil.parser import parse
import sys, os, re, time, json, time, math, datetime


special_datetime_word = ['year', 'time', 'date', 'day', 'month', 'yy']
special_number_word = ['per', 'num', '%', '#', 'amount']
special_text_word = ['dbn', 'name', 'id']


def tranlated_value_general(x):
	try:
		a = int(x)
		return ('INTEGER', a)
	except:
		pass
	try:
		a = float(x)
		return ('REAL', a)
	except:
		pass
	return ('TEXT', x)

def tranlated_value_datetime(x):
	try:
		a = parse(x)
		return ('DATE TIME', a, x)
	except:
		pass
	try:
		a = int(x)
		return ('INTEGER', a)
	except:
		pass
	try:
		a = float(x)
		return ('REAL', a)
	except:
		pass
	return ('TEXT', x)

def tranlated_value_text(x):
	try:
		a = int(x)
		return ('INTEGER', a)
	except:
		pass
	return ('TEXT', x)


def save_metadate_to_json(ds_name):
	start_time = time.time()
	add = lambda a, b: a + b


	output = dict()
	output["dataset_name"] = ds_name
	output["columns"] = []

	header = sc.textFile("/user/hm74/NYCOpenData/" + ds_name + ".tsv.gz").first().split("\t")
	hdr_len = len(header)

	dataset_rdd = sc.textFile("/user/hm74/NYCOpenData/" + ds_name + ".tsv.gz").map(lambda x: x.split("\t")).filter(lambda x: len(x) >= hdr_len)
	#header = dataset_rdd.first()

	dataset_rdd = dataset_rdd.filter(lambda line: line != header)
	#print(dataset_rdd.first())

	total_count = dataset_rdd.count()

	for i in range(len(header)):
		print(header[i])

		def safe_project(x):
			try:
				p = x[i].strip()
				return p
			except:
				return ""

		col_rdd = dataset_rdd.map(safe_project).cache()
		non_empty_count = col_rdd.filter(lambda x:x is not None and x != "").count()
		empty_count = total_count - non_empty_count
		distinct_count = col_rdd.distinct().count()

		freq_values_pair = sorted(col_rdd.countByValue().items(), reverse=True, key=lambda x: x[1])[0:5]
		freq_values = [p[0] for p in freq_values_pair]


		if any(word in header[i].lower() for word in special_datetime_word) and all(word2 not in header[i].lower() for word2 in special_number_word):
			tranlated_value = col_rdd.map(tranlated_value_datetime)
		elif any(word in header[i].lower() for word in special_text_word):
			tranlated_value = col_rdd.map(tranlated_value_text)
		else: tranlated_value = col_rdd.map(tranlated_value_general)

		col = dict()
		col["column_name"] = header[i]
		col["number_non_empty_cells"] = non_empty_count
		col["number_empty_cells"] = empty_count
		col["number_distinct_values"] = distinct_count
		col["frequent_values"] = freq_values
		col["data_types"] = []


		#data_types = tvp.map(lambda x: x[0]).distinct().cache()
		count_int = 0
		count_numeric = 0
		count_txt = 0
		count_dt = 0

		#if (ds_name == 'nbgq-j9jt'): print(tranlated_value.collect()[:10])

		try:
			numeric_col_data = tranlated_value.filter(lambda x: x[0] == 'REAL').map(lambda x: x[1]).cache()
			#if not numeric_col_data.isEmpty():
			#mean_numeric = numeric_col_data.mean()
			min_numeric = numeric_col_data.min()
			max_numeric = numeric_col_data.max()
			count_numeric = numeric_col_data.count()
			s = numeric_col_data.sum()

			mean_numeric = s/count_numeric
			s2 = numeric_col_data.map(lambda x: pow(x - mean_numeric, 2)).sum()
			sd_numeric = math.sqrt(float(s2)/count_numeric)

			type_real = dict()
			type_real["type"] = 'REAL'
			type_real['count'] = count_numeric
			type_real['max_value'] = max_numeric
			type_real['min_value'] = min_numeric
			type_real['mean'] = mean_numeric
			type_real['stddev'] = sd_numeric

			col["data_types"].append(type_real)

			#sd_numeric = numeric_col_data.stdev()

		except:
			pass


		if (total_count != (count_numeric + count_int + count_txt + count_dt)):
			try:
				int_col_data = tranlated_value.filter(lambda x: x[0] == 'INTEGER').map(lambda x: x[1]).cache()

				min_int = int_col_data.min()
				max_int = int_col_data.max()
				count_int = int_col_data.count()
				s = int_col_data.sum()

				mean_int = s/count_int
				s2 = int_col_data.map(lambda x: pow(x - mean_int, 2)).sum()
				sd_int = math.sqrt(float(s2)/count_int)

				type_int = dict()
				type_int["type"] = 'INTEGER'
				type_int['count'] = count_int
				type_int['max_value'] = max_int
				type_int['min_value'] = min_int
				type_int['mean'] = mean_int
				type_int['stddev'] = sd_int

				col["data_types"].append(type_int)

			except:
				pass

			if (total_count != (count_numeric + count_int  + count_txt + count_dt)):
				try:
					dt_col_data = tranlated_value.filter(lambda x: x[0] == 'DATE TIME').map(lambda x: x[1:]).cache()

					count_dt = dt_col_data.count()
					max_dt = dt_col_data.max(lambda x:x[0])
					min_dt = dt_col_data.min(lambda x:x[0])

					type_dt = dict()
					type_dt["type"] = 'DATE TIME'
					type_dt['count'] = count_dt
					type_dt['min_value'] = min_dt[1]
					type_dt['max_value'] = max_dt[1]

					col["data_types"].append(type_dt)

				except:
					pass

				if (total_count != (count_numeric + count_int + count_txt + count_dt)):
					try:
						txt_col_data = tranlated_value.filter(lambda x: x[0] == 'TEXT').map(lambda x: x[1]).cache()

						count_txt = total_count - count_numeric - count_int - count_dt#txt_col_data.count()
						txt_col_data_len = txt_col_data.map(lambda x: len(x))

						max_txt_len = txt_col_data.distinct().top(5, lambda x:len(x))
						min_txt_len = txt_col_data.distinct().top(5, lambda x:-len(x))

						#print(max_txt_len)

						s = txt_col_data_len.sum()
						mean_txt_len = s/count_txt

						type_txt = dict()
						type_txt["type"] = 'TEXT'
						type_txt['count'] = count_txt
						type_txt['shortest_values'] = min_txt_len
						type_txt['longest_values'] = max_txt_len
						type_txt['average_length'] = mean_txt_len

						col["data_types"].append(type_txt)

					except:
						pass


		output["columns"].append(col)

	with open("./profile/%s.json" % ds_name, 'w') as fp:
		json.dump(output, fp, indent=4, default=str)


	elapsed_time = time.time() - start_time
	print("elapsed_time: ", elapsed_time)


if __name__ == "__main__":
	sc = SparkContext()
	all_datasets = sc.textFile("/user/hm74/NYCOpenData/datasets.tsv").map(lambda x: x.split("\t")[0]).collect()

	folder = os.path.exists("./profile")
	if not folder:
		os.makedirs("./profile")

	for ds_name in all_datasets[int(sys.argv[-1]):]:
		print("/home/zx979/new_project/task1/profile/" + ds_name + ".json")
		if not os.path.exists("/home/zx979/new_project/task1/profile/" + ds_name + ".json"):
			save_metadate_to_json(ds_name)

