import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext,DataFrame,functions
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
# according to column name, select appropriate checking function
def type_selector(colname):
	s = colname.lower()
	if 'name' in s and ('first' in s or 'last' in s or 'family' in s or 'middle' in s):
		return is_person_name
	elif 'name' in s and 'business' in s:
		return is_business_name
	elif 'name' in s and 'street' in s:
		return is_street_name
	elif 'name' in s and 'school' in s:
		return is_school_name
	elif 'park' in s:
		return is_park_playground
	elif 'school' in s and ('level' in s or 'type' in s):
		return is_school_level
	elif 'subject' in s:
		return is_subject_in_school
	elif 'interest' in s:
		return is_area_of_study
	elif 'agency' in s:
		return is_city_agency
	elif 'house' in s and 'number' in s:
		return is_house_number
	elif 'prem_typ_desc' in s:	# only work for current task: prem_typ_desc column assigned the type of location as true type
		return is_location_type
	elif 'phone' in s or 'phone number' in s or 'tel' in s:
		return is_phone_number
	elif 'website' in s or 'web' in s or 'url' in s:
		return is_website
	elif 'latitude' in s:
		return is_lat_coordinates
	elif 'longitude' in s:
		return is_lon_coordinates
	elif 'location' in s or 'lat_lon' in s:
		return is_lat_lon_cord
	elif 'zip' in s or 'zipcode' in s or 'zip code' in s or 'zip_code' in s:
		return is_zip_code
	elif 'borough' in s:
		return is_borough
	elif 'city' in s:
		return is_city
	elif 'neighborhood' in s:
		return is_neighborhood
	elif 'address' in s:
		return is_address
	elif 'building' in s and 'classification' in s:
		return is_building_classification
	elif 'color' in s:
		return is_color
	elif 'vehicle' in s and 'make' in s:
		return is_car_make
	elif 'vehicle' in s and 'type' in s:
		return is_vehicle_type


# data validators below
def is_person_name(s):
	if re.match('^[a-zA-Z]+,?\s*?[a-zA-Z]*$', s):	# match: alphabet words, then ' ' or ',', alphabet words
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
# hard to find a regular rule, simple check if length >= 2
def is_business_name(s):
	if not s: return False
	if len(s) >= 2:
		return True
	else:
		return False
# For park and playground types
def is_park_playground(s):
	if not s: return False
	if len(s) >=4:
		return True
	else:
		return False
def is_school_name(s):	#include university_name, college_name
	if not s:return False
	keywords = ['university','school','institute','academy','college']
	for word in keywords:
		if word in s.lower():
			return True
	return False
def is_school_level(s):
	if not s:return False
	keywords = ['elementary','middle','high','yabc','k']
	for word in keywords:
		if word in s.lower():
			return True
	return False
def is_subject_in_school(s):
	if not s:return False
	customized_subjects = ['social studies','mathematics','math','english','science']
	for sub in customized_subjects:
		if sub in s.lower():
			return True
	# check for other possible subjects:
	if len(s) >=3:
		return True
	return False
def is_area_of_study(s):
	if not s:return False
	#simple check length of s, not easy to check all areas of study
	if len(s) >= 4:
		return True
	else:
		return False
def is_house_number(s):
	if not s:return False
	if re.match('\w+-?\w*', s):
		return True
	else:
		return False
def is_phone_number(s):
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
def is_lat_lon_cord(s):
	res = re.findall('-?\d+.\d+', s)
	print(res)
	if len(res) == 2 and is_lat_coordinates(res[0]) and is_lon_coordinates(res[1]):
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
def is_zip_code(s):
	if not s: return False
	if re.match('^\d{5}(?:-\d{4})?', s.strip()):
		return True
	else:
		return False
def is_borough(s):
	if not s: return False
	borough = ['bronx','brooklyn','manhattan','queens', 'staten island', '1', '2','3','4','5', 'mn','bx','bk','qn','si', 'M','X','R','K','Q']
	if s.lower() in borough:
		return True
	else:
		return False
# re match address rule: start with digit or not, match everything following, match 2 letter states and zipcode at end
def is_address(s):
	if not s: return False
	if re.match('(\d+-?\d*)?.*([a-zA-Z]{2})?\d{5}(-\d+)?', s):
		return True
	else:
		return False
# matching rules: alphabetic string including hyphen -, white space
def is_neighborhood(s):
	if not s: return False
	if re.match('[a-zA-Z]+[ -]?[a-zA-Z]*', s):
		return True
	else:
		return False
def is_city(s):
	if not s: return False
	if s.isalpha():
		return True
	else:
		return False
def is_city_agency(s):
	if not s: return False
	agency_list = ['NYCOA','AJC','OATH','DFTA','MOA','BPL','DOB','BIC','CFB','CIDI','OCME',
					'ACS','CLERK','DCP','CUNY','DCAS','CECM','CEC','CSC','CCRB','CGE','CCPC','CAU',
					'CB','COMP','COIB','DCA','DCWP','MOCS','BOC','DOC','DCLA','MODA','DDC','Brooklyn',
					'Manhattan','DOE','BOE','MOEC','DEP','EEPC','DOF','FDNY','GNYC','DOHMH','DHS','NYCHA',
					'HPD','HRO','HRA','CCHR','MOIA','IBO','MOIP','DOITT','MOIGA','DOI','MACJ','OLR','LPC',
					'LAW','BPL','NYPL','QL','LOFT','OMB','MCCM','OM','IA','MOPD','OER','MOSPCE','OMWBE',
					'OSP','ENDGBV','MOME','NYCGO','NYCEDC','NYCERS','SERVICE','TFA','NYPL','OPS','DPR',
					'OPA','NYPD','PPF','DOP','PPB','BCPA','KCPA','NYCountyPA','QPA','RCPA','QPL','DORIS',
					'RGB','STAR','DSNY','SCA','SBS','DSS','OSE','SNP','BSA','TAT','TC','TLC','DOT','DVS','NYWB','NYW','DYCD','311',
					'NYCEM']
	# check agency list
	if s in agency_list:
		return True
	# check agency ID:
	if s.isdigit():
		return True
	# check agency name:
	if len(s) >= 10:
		return True
	return False
# start with simple: check if size of str >=2
def is_building_classification(s):
	if not s: return False
	if len(s) >=2:
		return True
	else:
		return False
# location type, generally speakking, doesn't have number. exclude all strings containing digit
def is_location_type(s):
	if not s: return False
	if re.match('\D+', s):
		return True
	else:
		return False
# for color, we expect alphabet words or abbr, exclude all strings containing digit
def is_color(s):
	if not s: return False
	if re.match('\D+', s):
		return True
	else:
		return False
# car_make containing number doesn't make sense in general way. So rule is to exlcude all strings that contain digit
def is_car_make(s):
	if not s: return False
	if re.match('\D+', s):
		return True
	else:
		return False
# not easy to define regularized format. check length, typically need at least two characters
def is_vehicle_type(s):
	if not s: return False
	if len(s) >=2:
		return True
	else:
		return False

############################################################################################
# Spark part
#compute LCSlongestCommonSubsequence for two texts
def LCS(text1, text2):
	dp = [0] * (len(text2) + 1)
	for i in range(1, len(text1)+1):
		prev = dp[0]
		for j in range(1, len(text2)+1):
			last = dp[j]
			if text1[i-1] == text2[j-1]:
				dp[j] = prev + 1
			if prev < last:
				prev = last
	return max(dp)
# The edit distance of two strings is the number of inserts and deletes of characters needed to turn one into the other
# d(x,y) = |x| + |y| - 2|LCS(x,y)|
# return the most similar col_name (shortest edit distance)
def edit_distance(col_text, headers):
	min_d = float('inf')
	name = ''
	for cur_text in headers:
		d = len(col_text)+len(cur_text) - 2 * LCS(col_text,cur_text)
		if d < min_d:
			min_d = d
			name = cur_text
	return name

def semantic_decision(data):
	colname = data[0]
	# if have data in the entry
	if data[1]:
		F = type_selector(colname)
		# if we could find a type function to check with, return default labeling: use colname as semantic type
		if not F:
			return (colname, colname)
		else:
			if F(data[1]):				# if type checking function F return True, meaning it matches labeling condition.
				label = F.__name__[3:]	# extract naming schema from method name
				return (colname, label)
			else:						# type checking function F return False, labeling as other.
				return (colname, 'Other')
	else:
		return (colname, 'NULL')		# Empty data, labeling NULL

def semantic_generator(spark, path, colname):
	df = spark.read.option("delimiter", "\\t").option("header","true").csv(path)
	# find the target column by using edit distance between provided colname and real colname in df
	column = edit_distance(colname, df.columns)
	select_col = df.select(column).rdd.map(lambda x: (column,x[0]))
	items_with_semantic = select_col.map(lambda x: semantic_decision(x))
	results = items_with_semantic.map(lambda x:((x[0],x[1]),1)).reduceByKey(lambda x,y:x+y).collect()

	# write to json
	all_semantics = [{'semantic_type': item[0][1], 'label':item[0][1],'count':item[1]} for item in results]
	columns = []
	columns.append({'column_name':column, 'semantic_types':all_semantics})
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
	spark = SparkSession.builder.appName("task2_semantic").config("spark.some.config.option", "some-value").getOrCreate()
	
	count,total = 0, len(files)
	files_existed = os.listdir('results')
	for file in files:
		# start generate meta from un-generated files.
		if ('{}_{}.json'.format(file[0], file[1])) in files_existed:
			print('{}_{}.json has already existed.'.format(file[0], file[1]))
			count +=1
			continue
		try:
			print('generating meta for {}'.format(file[0]))
			start = time.time()
			metadata = semantic_generator(spark, '/user/hm74/NYCOpenData/{}.tsv.gz'.format(file[0]), file[1])
		except:
			print('Exception occurred on ' + file[0])
		else:
			with open('results/{}_{}.json'.format(file[0], file[1]), 'w') as f:
				json.dump(metadata, f, indent=2)
			end = time.time()
			count +=1
			print("Time elapsed: " + str(end - start) + " seconds")
			print('{} / {} files finished...'.format(count, total))











