# -*- coding: UTF-8 -*-
import json, os, re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql import Row
from pyspark.sql import functions as F

sc = SparkContext()
spark = SparkSession \
        .builder \
        .appName("task2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


person_name_keyword = ['last name', 'lastname', 'last_name', 'first name', 'firstname', 'first_name']
street_name_keyword = ['street', ' st', 'road', ' rd', 'avenue', ' ave', 'drive', 'boulevard', 'blvd', 'lane', 'parkway', 'place', 'terrace', 'court', 'eb', 'wb', 'bend', 'turnpike', 'highway', 'hwy'] +\
						['square', 'walk', 'bridge', 'circle', 'expressway', 'expwy', 'boundary', 'exit', 'overpass', 'subway', 'n/a', 'alley', 'pkwy', 'tunnel', 'loop', 'crescent', 'entrance', 'end', 'intersection']
borough = ['manhattan', 'brooklyn', 'bklyn', 'queens', 'bronx', 'staten island']
borough_code = ['k', 'm', 'q', 'r', 'x']
school_level = ["elementary","high school","transfer","k-2","k-3","k-8","middle","yabc"]
subjects = ["english","math","science","social", "econom", "algebra", "chemistry", "geom", "history", "physic", "art", "environment", "government"]

area_study = ["animal", "science", "architecture","business","communication","computer", "ology","art","engineer","environment"] +\
			 ["film","professions","hospital", "travel","human","jrotc","law", "government", "design","math","teaching"]

school_name = ["college","university","school","collaborative","institute","academy","learn", "center","preparatory","yabc", "edu"] +\
			  ["IS", "I.S.", "PS", "P.S.", "JHS", "MS", "UNSPECIFIED"]

parks_playgrounds = ["park ","playground","plaza","center","lake","ballfield","ballfields","square","garden", "closed school"]


with open('building_codes_ny.txt') as file:
	content = file.readlines()
building_codes_ny = [code.strip() for code in content]

#https://www.abbreviations.com/acronyms/COLORS
#http://shuttle-house.com/page_top_ENGLISH/Guide/color_chart.html

with open('colors.txt') as file:
	content = file.readlines()
colors = [color.strip() for color in content]

with open('business_keyword.txt') as file:
	content = file.readlines()
business_keyword = [business.strip() for business in content]

with open('agency_abbr_ny.txt') as file:
	content = file.readlines()
agency_abbr_ny = [agency.strip() for agency in content]

with open('agency_ny.txt') as file:
	content = file.readlines()
agency_ny = [agency.strip() for agency in content]


with open('type_locations.txt') as file:
	content = file.readlines()
type_locations = [loc.strip() for loc in content]

with open('car_make.txt') as file:
	content = file.readlines()
car_make = [car_m.strip() for car_m in content]

with open('vehicle_type.txt') as file:
	content = file.readlines()
vehicle_type = [veh_type.strip() for veh_type in content]

############## districts #################
with open('cities_ny.txt') as file:
	content = file.readlines()
cities_ny = [city.strip().lower() for city in content]
with open('cities_us.txt') as file:
	content = file.readlines()
cities_us = [city.strip().lower() for city in content]

with open('towns_ny.txt') as file:
	content = file.readlines()
towns_ny = [town.strip().lower() for town in content]
with open('villages_ny.txt') as file:
	content = file.readlines()
villages_ny = [village.strip().lower() for village in content]
with open('hamlets_ny.txt') as file:
	content = file.readlines()
hamlets_ny = [hamlet.strip().lower() for hamlet in content]

with open('neighbors_ny.txt') as file:
	content = file.readlines()
neighbors_ny = [neighbor.strip().lower() for neighbor in content]


R = Row('city')
cities_df = spark.createDataFrame([R(x) for i, x in enumerate(cities_ny + cities_us)])
cities_df.createOrReplaceTempView("cities_df")
cities_df.show()

R = Row('town')
towns_df = spark.createDataFrame([R(x) for i, x in enumerate(towns_ny)])
towns_df.createOrReplaceTempView("towns_df")
towns_df.show()

R = Row('village')
villages_df = spark.createDataFrame([R(x) for i, x in enumerate(villages_ny)])
villages_df.createOrReplaceTempView("villages_df")
villages_df.show()

R = Row('hamlet')
hamlets_df = spark.createDataFrame([R(x) for i, x in enumerate(hamlets_ny)])
hamlets_df.createOrReplaceTempView("hamlets_df")
hamlets_df.show()

R = Row('borough')
boroughs_df = spark.createDataFrame([R(x) for i, x in enumerate(borough)])
boroughs_df.createOrReplaceTempView("boroughs_df")
boroughs_df.show()

R = Row('neighbor')
neighbors_df = spark.createDataFrame([R(x) for i, x in enumerate(neighbors_ny)])
neighbors_df.createOrReplaceTempView("neighbors_df")
neighbors_df.show()

def LD(s, t):
    if s == "": return len(t)
    if t == "": return len(s)
    if s[-1] == t[-1]: cost = 0
    else: cost = 1
    res = min([LD(s[:-1], t)+1, LD(s, t[:-1])+1, LD(s[:-1], t[:-1]) + cost])
    return res


def address_street_name_count(col_data):
	match_rdd = col_data.filter(lambda x: any(word in x[0].lower().strip() for word in street_name_keyword))
	# not_match_rdd = col_data.filter(lambda x: not any(word in x[0].lower().strip() for word in street_name_keyword))
	# print(not_match_rdd.collect())
	r = re.compile(r'^((\d+((TH)?|(th)?|(ST)?|(st)?|(RD)?)|(rd)?|(ND)?|(nd)?)|(\d+-\d+))\s+([a-zA-Z0-9\s]+)\s+([a-zA-Z]+)$')

	total_count = match_rdd.map(lambda x: int(x[1])).sum()
	address_count = match_rdd.filter(lambda x: bool(re.match(r, x[0]))).map(lambda x: int(x[1])).sum()
	street_name_count = total_count - address_count
	count = {'address': address_count, 'street name': street_name_count}

	return count

def borough_count(col_data):
	all_borough_keyword = borough + borough_code
	count = col_data.filter(lambda x: any(word == x[0].lower().strip() for word in all_borough_keyword)).map(lambda x: int(x[1])).sum()
	return count

def building_class_count(col_data):
	count = col_data.filter(lambda x: any(word + '-' == x[0][0:3] for word in building_codes_ny)).map(lambda x: int(x[1])).sum()
	return count

def color_count(col_data):
	count = col_data.filter(lambda x: any(word.lower() == x[0].lower() or (word.lower() in x[0] and len(word) >= 4) for word in colors)).map(lambda x: int(x[1])).sum()
	return count

def business_name_count(col_data):
	match_rdd = col_data.filter(lambda x: any(word.lower() in x[0].lower() for word in business_keyword))
	count1 = match_rdd.map(lambda x: int(x[1])).sum()
	#print(count1)

	#not_match_rdd = col_data.filter(lambda x: not any(word.lower() in x[0].lower() for word in business_keyword))
	#r = re.compile(r'[a-zA-z]')
	#count2 = not_match_rdd.map(lambda x: int(x[1])).sum()
	#print(count2)

	return count1


def car_make_count(col_data):
	match_rdd = col_data.filter(lambda x: any(((word.lower() in x[0].lower() and len(word) >= 3) or x[0].lower() in word.lower()) for word in car_make))
	#print(match_rdd.collect())
	count = match_rdd.map(lambda x: int(x[1])).sum()

	return count


def vehicle_type_count(col_data):
	match_rdd = col_data.filter(lambda x: any(word.lower() in x[0].lower().strip() and len(word) > 2 for word in vehicle_type))
	count = match_rdd.map(lambda x: int(x[1])).sum()

	return count

def school_level_count(col_data):
	match_rdd = col_data.filter(lambda x: any(word.lower() in x[0].lower().strip() for word in school_level))
	count = match_rdd.map(lambda x: int(x[1])).sum()

	return count

def area_study_count(col_data):
	match_rdd = col_data.filter(lambda x: any(word.lower() in x[0].lower().strip() for word in area_study))
	count = match_rdd.map(lambda x: int(x[1])).sum()
	return count


def agency_abbr_count(col_data):
	count = col_data.filter(lambda x: any(word.lower() == x[0].lower() for word in agency_abbr_ny)).map(lambda x: int(x[1])).sum()
	return count

def agency_count(col_data):
	count = col_data.filter(lambda x: any(word.lower() in x[0].lower() for word in agency_ny)).map(lambda x: int(x[1])).sum()
	return count


def subject_count(col_data):
	count = col_data.filter(lambda x: any(word.lower() in x[0].lower() for word in subjects)).map(lambda x: int(x[1])).sum()
	count2 = col_data.filter(lambda x: any(word.lower() in x[0].lower() for word in subjects)).count()
	return count, count2


def location_type_count(col_data):
	count = col_data.filter(lambda x: any(word.lower() in x[0].lower() for word in type_locations)).map(lambda x: int(x[1])).sum()
	return count

def district_name_count(col_data):

	total = 0

	col_list = col_data.map(lambda x: (x[0], x[1])).collect()
	count = {'city': 0, 'borough': 0, 'neighbor': 0}
	for i in range(len(col_list)):
		if int(col_list[i][1]) > 2 and not "/" in col_list[i][0] and not "-" in col_list[i][0]:
			try:
				print(i, col_list[i][0])
				total += int(col_list[i][1])
				clean_word = re.sub('[0-9]+', '', col_list[i][0].lower()).strip()
				#print(clean_word)
				city_levenshtein_df = spark.sql('''select levenshtein(city, "''' + clean_word + '''") as min_lev_dis, city from cities_df order by min_lev_dis asc limit 1''')
				#town_levenshtein_df = spark.sql('''select levenshtein(town, "''' + clean_word + '''") as min_lev_dis, town from towns_df order by min_lev_dis asc limit 1''')
				#village_levenshtein_df = spark.sql('''select levenshtein(village, "''' + clean_word + '''") as min_lev_dis, village from villages_df order by min_lev_dis asc limit 1''')
				#hamlet_levenshtein_df = spark.sql('''select levenshtein(hamlet, "''' + clean_word + '''") as min_lev_dis, hamlet from hamlets_df order by min_lev_dis asc limit 1''')
				borough_levenshtein_df = spark.sql('''select levenshtein(borough, "''' + clean_word + '''") as min_lev_dis, borough from boroughs_df order by min_lev_dis asc limit 1''')
				neighbor_levenshtein_df = spark.sql('''select levenshtein(neighbor, "''' + clean_word + '''") as min_lev_dis, neighbor from neighbors_df order by min_lev_dis asc limit 1''')
				#evenshtein_df.show()

				city_levenshtein_min = city_levenshtein_df.rdd.first().min_lev_dis
				# town_levenshtein_min = town_levenshtein_df.rdd.first().min_lev_dis
				# village_levenshtein_min = village_levenshtein_df.rdd.first().min_lev_dis
				# hamlet_levenshtein_min = hamlet_levenshtein_df.rdd.first().min_lev_dis
				borough_levenshtein_min = borough_levenshtein_df.rdd.first().min_lev_dis
				neighbor_levenshtein_min = neighbor_levenshtein_df.rdd.first().min_lev_dis
				#print(levenshtein_min)

				if (city_levenshtein_min <= len(clean_word) * 3/8):
					count['city'] += int(col_list[i][1])
				# elif (town_levenshtein_min <= len(clean_word) * 2/8):
				# 	count['town'] += int(col_list[i][1])
				# elif (village_levenshtein_min <= len(clean_word) * 2/8):
				# 	count['village'] += int(col_list[i][1])
				# elif (hamlet_levenshtein_min <= len(clean_word) * 2/8):
				# 	count['hamlet'] += int(col_list[i][1])
				elif (borough_levenshtein_min <= len(clean_word) * 3/8):
					count['borough'] += int(col_list[i][1])
				elif (neighbor_levenshtein_min <= len(clean_word) * 3/8):
					count['neighbor'] += int(col_list[i][1])
			except:
				continue

	#print(count)
	return count, total


# ((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*
def website_count(col_data):
	r = re.compile(r'((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*')
	count = col_data.filter(lambda x: bool(re.match(r, x[0].lower()))).map(lambda x: int(x[1])).sum()
	return count

# (\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})
def phone_number_count(col_data):
	r = re.compile(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
	count = col_data.filter(lambda x: bool(re.match(r, x[0].lower()))).map(lambda x: int(x[1])).sum()
	return count

def coordinate_count(col_data):
	r = re.compile(r'(\([+-]?[0-9]+\.[0-9]+,[\s]?[+-]?[0-9]+\.[0-9]+\))')
	count = col_data.filter(lambda x: bool(re.match(r,x[0].lower()))).map(lambda x: int(x[1])).sum()
	return count

def zipCode_count(col_data):
	r = re.compile(r'(\d{5}([\-\s]\d{4})?|[\_]+\d{5}|[a-zA-Z]+\d{5}|\d{5})')
	count = col_data.filter(lambda x: bool(re.match(r, x[0].lower()))).map(lambda x: int(x[1])).sum()
	return count

def school_name_and_park_count(col_data):
	school_and_park_count = col_data.filter(lambda x: any(word.lower() in x[0].lower() for word in (school_name + parks_playgrounds))).map(lambda x: int(x[1])).sum()
	park_count = col_data.filter(lambda x: any(word.lower() in x[0].lower() for word in parks_playgrounds)).map(lambda x: int(x[1])).sum()
	return school_and_park_count - park_count, park_count


def profile_semantic(ds_col):
	col_data = sc.textFile("/user/hm74/NYCColumns/" + ds_col).map(lambda x: x.split("\t"))
	col_total = col_data.map(lambda x: int(x[1])).sum()
	col_rows = col_data.count()

	print("total rows: {}".format(col_rows))
	print("total records: {}".format(col_total))

	count = business_name_count(col_data)
	business_name_similarity = count/col_total
	print("    business name: {}".format(business_name_similarity))
	if business_name_similarity > 0.54:
		return [('business name', count), ('others', col_total - count)]

	count = school_level_count(col_data)	
	school_level_similarity = count/col_total
	print("    school level type: {}".format(school_level_similarity))
	if school_level_similarity > 0.9:
		return [('school level', count), ('others', col_total - count)]



	count = address_street_name_count(col_data)
	street_name_similarity = count['street name']/col_total
	address_similarity = count['address']/col_total
	print("    address or street name: {}, {}".format(address_similarity, street_name_similarity))
	if street_name_similarity + address_similarity > 0.85:
		return [('address', count['address']), ('street name', count['street name']), ('others', col_total - count['street name'] - count['address'])]


	qualify_item_count, qualify_row_count = subject_count(col_data)
	subject_similarity = max(qualify_item_count/col_total, qualify_row_count/col_rows)
	print("    subject type: {}".format(subject_similarity))
	if subject_similarity >= 0.8:
		return [('subject', col_total)]

	count = area_study_count(col_data)
	area_study_similarity = count/col_total
	print("    area study: {}".format(area_study_similarity))
	if area_study_similarity > 0.8:
		return [('area study', count), ('others', col_total - count)]


	count = website_count(col_data)
	website_url_similarity = count/col_total
	print("    website url: {}".format(website_url_similarity))
	if website_url_similarity > 0.9:
		return [('website url', count), ('others', col_total - count)]


	count = phone_number_count(col_data)
	phone_number_similarity = count/col_total
	print("    phone number: {}".format(phone_number_similarity))
	if phone_number_similarity > 0.9:
		return [('phone number', count), ('others', col_total - count)]


	count = coordinate_count(col_data)
	coordinate_similarity = count/col_total
	print("    coordinate: {}".format(coordinate_similarity))
	if coordinate_similarity > 0.9:
		return [('coordinate', count), ('others', col_total - count)]


	count = zipCode_count(col_data)
	zipcode_similarity = count/col_total
	print("    zipcode: {}".format(zipcode_similarity))
	if zipcode_similarity > 0.9:
		return [('zipcode', count), ('others', col_total - count)]

	school_count, park_count = school_name_and_park_count(col_data)
	school_name_similarity = school_count/col_total
	park_similarity = park_count/col_total
	print("    school name: {}, park: {}".format(school_name_similarity, park_similarity))
	if school_name_similarity + park_similarity > 0.6:
		return [('School name', school_count), ('Park/Playground', park_count), ('others', col_total - park_count - school_count)]

	count = borough_count(col_data)
	borough_similarity = count/col_total
	print("    borough: {}".format(borough_similarity))
	if borough_similarity > 0.9:
		return [('borough', count), ('others', col_total - count)]

	count = building_class_count(col_data)
	building_class_similarity = count/col_total
	print("    building class: {}".format(building_class_similarity))
	if building_class_similarity > 0.9:
		return [('building class', count), ('others', col_total - count)]

	count = color_count(col_data)
	color_similarity = count/col_total
	print("    color: {}".format(color_similarity))
	if color_similarity > 0.8:
		return [('color', count), ('others', col_total - count)]

	count = car_make_count(col_data)
	car_make_similarity = count/col_total
	print("    car make: {}".format(car_make_similarity))
	if car_make_similarity > 0.40:
		return [('car make', count), ('others', col_total - count)]


	count = location_type_count(col_data)
	location_type_similarity = location_type_count(col_data)/col_total
	print("    location type: {}".format(location_type_similarity))
	if location_type_similarity > 0.8:
		return [('location type', count), ('others', col_total - count)]


	count = agency_abbr_count(col_data)
	agency_abbr_similarity = count/col_total
	print("    agency: {}".format(agency_abbr_similarity))
	if agency_abbr_similarity > 0.85:
		return [('agency', count), ('others', col_total - count)]


	count = agency_count(col_data)
	agency_similarity = count/col_total
	print("    agency: {}".format(agency_similarity))
	if agency_similarity > 0.8:
		return [('agency', count), ('others', col_total - count)]


	count = vehicle_type_count(col_data)
	vehicle_type_similarity = count/col_total
	print("    vehicle type: {}".format(vehicle_type_similarity))
	if vehicle_type_similarity > 0.9:
		return [('vehicle type', count), ('other', col_total - count)]


	district_name_stat, dis_total = district_name_count(col_data)
	city_similarity = district_name_stat['city']/dis_total
	borough_similarity = district_name_stat['borough']/dis_total
	neighbor_similarity = district_name_stat['neighbor']/dis_total
	print("    district name: {}, {}, {}".format(city_similarity, borough_similarity, neighbor_similarity))
	if (city_similarity + borough_similarity + neighbor_similarity) > 0.75:
		city_count = int(city_similarity * col_total)
		bor_count = int(borough_similarity * col_total)
		neigh_count = int(neighbor_similarity * col_total)
		return [('city', city_count), ('borough', bor_count), ('neighbor', neigh_count), ('others', col_total - neigh_count - bor_count - city_count)]



if __name__ == "__main__":

	with open('cluster1.txt') as file:
		file_string = file.readline()
		file_string = file_string.replace('\', ', '')
	cluster = file_string.split('\'')
	cluster.pop(0)
	cluster.pop(-1)

	folder = os.path.exists("./profile")
	if not folder:
		os.makedirs("./profile")

	print(cities_ny[:20])
	print(hamlets_ny[:20])

	for ds_col in cluster[:]: # + cluster[3:14] + cluster[15:25] + cluster[26:43] + cluster[44:108]:
		print(ds_col)
		if not os.path.exists("/home/zx979/new_project/task2/profile/" + ds_col + ".json"):
			col = ds_col.split('.')[1]
			ds = ds_col.split('.')[0]

			try:
				output = dict()
				output["dataset_name"] = ds
				output["column_name"] = col
				if any(word in col.lower().strip() for word in person_name_keyword) or col.lower().strip() == 'mi':
					#print("it is a person name becaue it contains person name keyword.")
					#print("\n")
					output = {}
					output["semantic_types"] = []
					sem_type = {}
					sem_type["semantic_type"] = 'person name'
					col_data = sc.textFile("/user/hm74/NYCColumns/" + ds_col).map(lambda x: x.split("\t"))
					col_total = col_data.map(lambda x: int(x[1])).sum()
					sem_type["count"] = col_total
					output["semantic_types"].append(sem_type)
					with open("./profile/%s.json" % ds_col, 'w') as fp:
						json.dump(output, fp, indent=4, default=str)

				elif (col == "HOUSE_NUMBER"):
					output = {}
					output["semantic_types"] = []
					sem_type = {}
					sem_type["semantic_type"] = 'house number'
					col_data = sc.textFile("/user/hm74/NYCColumns/" + ds_col).map(lambda x: x.split("\t"))
					col_total = col_data.map(lambda x: int(x[1])).sum()
					sem_type["count"] = col_total
					output["semantic_types"].append(sem_type)
					with open("./profile/%s.json" % ds_col, 'w') as fp:
						json.dump(output, fp, indent=4, default=str)
				elif (col == "TruckMake"):
					output = {}
					output["semantic_types"] = []
					sem_type = {}
					sem_type["semantic_type"] = 'car make'
					col_data = sc.textFile("/user/hm74/NYCColumns/" + ds_col).map(lambda x: x.split("\t"))
					col_total = col_data.map(lambda x: int(x[1])).sum()
					sem_type["count"] = col_total
					output["semantic_types"].append(sem_type)
					with open("./profile/%s.json" % ds_col, 'w') as fp:
						json.dump(output, fp, indent=4, default=str)

				else:
					result = profile_semantic(ds_col)
					print("\n")
					output = {}
					output["semantic_types"] = []
					for t in result:
						sem_type = {}
						sem_type["semantic_type"] = t[0]
						sem_type["count"] = t[1]
						output["semantic_types"].append(sem_type)

					with open("./profile/%s.json" % ds_col, 'w') as fp:
						json.dump(output, fp, indent=4, default=str)
			except:
				pass




