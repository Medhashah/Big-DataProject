import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext,DataFrame
from pyspark.sql.functions import *
from csv import reader
import json
import time
import re
import os

class files():
	def __init__(self, fname):
		self.files = []
		with open(fname, 'r') as f:
			data = f.read()
			data_list = data[1:-2].replace("'", "").split(', ')
			for file in data_list:
				tmp = file.split('.')
				self.files.append((tmp[0],tmp[1]))

	def getAll(self):
		return self.files


def type_selector(colname):
	s = colname.lower()
	if 'name' in s:										# apply is_person_name method
		if 'first' in s or 'last' in s or 'family' in s or 'middle' in s:
			return is_person_name
		elif 'street' in s:
			return is_street_name
		elif 'business' in s:
			return is_business_name
		elif 'school' in s:
			return is_school_name
	elif 'phone' in s or 'phone number' in s:			# apply is_us_phone_number method
		return is_us_phone_number
	elif 'website' in s or 'web' in s or 'url' in s:	# apply is_website method
		return is_website
	elif 'latitude' in s:								# apply is_lat_coordinates method
		return is_lat_coordinates
	elif 'longitude' in s:								# apply is_lon_coordinates method
		return is_lon_coordinates
	elif ('zip' in s or 'zipcode' in s or 'zip code' in s or 'zip_code' in s) and 'city' not in s:
		return is_zipcode
	elif 'borough' in s:
		return is_borough
	elif 'address' in s:
		return is_address

def is_person_name(s):
	if re.match('^[a-zA-Z]+,? ?[a-zA-Z]*$', s):	# match: alphabet words, then ' ' or ',', alphabet words
		return True
	else:
		return False

def is_street_name(s):
	if not s: return False
	keywords = ['street','avenue','boulevard','road','roadway','court','highway','lane','pavement','place'
				'route','thoroughfare','track','trail','artery','byway','drag','drive','parkway','passage'
				'row','stroll','terrace','turf','trace','turnpike','way']
	for word in keywords:
		if word in s.lower():
			return True
	return False

def is_business_name(s):
	if not s: 
		return False
	else:
		return True

def is_school_name(s):
	if not s:
		return False
	else:
		return True

# re match address rule: start with digit or not, match everything following, match 2 letter states and zipcode at end
def is_address(s):
	if not s: return False
	if re.match('(\d+-?\d*)?.*([a-zA-Z]{2})?\d{5}(-\d+)?', s):
		return True
	else:
		return False



def is_us_phone_number(s):
	if not s: return False
	if re.match('\(?\d{3}[-.)] ?\d{3}[-.]\d{4}', s):
		return True
	else:
		return False

def is_website(s):
	if not s: return False
	if re.match('^(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$', s):
		return True
	else:
		return False

def is_lat_coordinates(s):
	if not s: return False
	try:
		val = float(s)
	except:
		return False
	else:
		if -90 <= val <= 90:
			return True
		else:
			return False

def is_lon_coordinates(s):
	if not s: return False
	try:
		val = float(s)
	except:
		return False
	else:
		if -180 <= val <= 180:
			return True
		else:
			return False

# five 5 digits and allows the option of having a hyphen and four extended digits
def is_zipcode(s):
	if not s: return False
	# if s.isdigit() and len(s) == 5 and s.startswith('1'):
	# 	return True
	# else:
	# 	return False
	if re.match('^\d{5}(?:-\d{4})?', s.strip()):
		return True
	else:
		return False

def is_borough(s):
	if not s: return False
	borough = ['bronx','brooklyn','manhattan','queens', 'staten island', '1', '2','3','4','5', 'mn','bx','bk','qn','si']
	if s.lower() in borough:
		return True
	else:
		return False


# address, city, neighborhood,color,car_make, city_agency,areas_of_study,subjects_in_school
# school_levels,university_names,building_Classification,vehicle_Type,type_of_location,Parks_Playgrounds
																							  #colname, data          colname,  type
# # labeling if any type checking function return true and label it as that type. For example, (Borough, manhattan) -> (Borough, borough)
def semantic_decision(data, header):
	N = len(header)
	output = []
	for i in range(N):
		if data[i]:
			f = type_selector(header[i])		# get appropriate method
			if not f:							# no appropriate method return, apply default method
				output.append((header[i], header[i]))
			else:
				if f(data[i]):
					label = f.__name__[3:]		# extract naming schema from method name
					output.append((header[i], label))
				else:							# if having a method, but data type isn't the correct semantic type
					output.append((header[i], 'Other'))
		else:
			output.append((header[i], 'NULL'))
	return output

def semantic_generator(sc, path):
	rdd = sc.textFile(path, 1).mapPartitions(lambda x: reader(x, delimiter='\t')).zipWithIndex().cache()
	header = rdd.first()[0]
	rows = rdd.filter(lambda x: x[1] != 0).map(lambda x: x[0])
	items_with_semantic = rows.flatMap(lambda x, h=header: semantic_decision(x,h))
	result = items_with_semantic.map(lambda x:((x[0],x[1]),1)).reduceByKey(lambda x,y:x+y)
	result = result.map(lambda x: (x[0][0],(x[0][1], x[1]))).collect()	# map to format (colname, (semantic, count))

	# write to json
	columns = []
	for colname in header:
		cur_cols = filter(lambda x: x[0]==colname, result)
		all_semantics = [{'semantic_type': col[1][0], 'count':col[1][1]} for col in cur_cols]
		columns.append({'column_name':colname, 'semantic_types':all_semantics})

	file_name = path.split('/')[-1]
	metadata = {
		'dataset_name': file_name,
		'columns':columns
	}
	return metadata


if __name__ == '__main__':
	# 274 files
	F = files('cluster3.txt')
	files = F.getAll()
	sc = SparkContext()

	# print('generating semantic for uwyv-629c.tsv.gz')
	# start = time.time()
	# metadata = semantic_generator(sc, '/user/hm74/NYCOpenData/uwyv-629c.tsv.gz')
	# with open('uwyv-629c.json', 'w') as f:
	# 	json.dump(metadata, f, indent=2)
	# end = time.time()
	# print("Time elapsed: " + str(end - start) + " seconds")

	count,total = 0, len(files)
	files_exist = os.listdir('results')
	for file in files:
		# start generate meta from last left file.
		if (file +'.json') in files_exist:
			print('{}.json has already existed.'.format(file))
			count +=1
			continue
		print('generating meta for {}'.format(file))
		start = time.time()
		metadata = semantic_generator(sc, '/user/hm74/NYCOpenData/{}.tsv.gz'.format(file))
		with open('results/{}.json'.format(file), 'w') as f:
			json.dump(metadata, f, indent=2)
		end = time.time()
		count +=1
		print("Time elapsed: " + str(end - start) + " seconds")
		print('{} / {} files finished...'.format(count, total))





