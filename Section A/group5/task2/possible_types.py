import re


def type_selector(colname):
	s = colname.lower()
	if 'name' in s:										# apply is_person_name method
		if 'first' in s or 'last' in s or 'family' in s or 'middle' in s:
			return is_person_name
	elif 'phone' in s:									# apply is_us_phone_number method
		return is_us_phone_number
	elif 'website' in s or 'web' in s or 'url' in s:	# apply is_website method
		return is_website
	elif 'latitude' in s:								# apply is_lat_coordinates method
		return is_lat_coordinates
	elif 'longitude' in s:								# apply is_lon_coordinates method
		return is_lon_coordinates
	elif 'zip' in s or 'zipcode' in s or 'zip code' in s or 'zip_code' in s and 'city' not in s:
		return is_ny_zipcode
	elif 'borough' in s:
		return is_borough

def is_person_name(s):
	if re.match('^[a-zA-Z]+,? ?[a-zA-Z]*$', s):	# match: alphabet words, then ' ' or ',', alphabet words
		return True
	else:
		return False

def is_us_phone_number(s):
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

def is_ny_zipcode(s):
	if not s: return False
	if s.isdigit() and len(s) == 5 and s.startswith('1'):
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



# types that are hard to use abstract method to identify, potentially contain different data format,or need lots of data sources to verify are follows. 
# and will apply default method for labeling: label colname as semantic type, and check if contains none type data.


# school_name, business_name, address, street_name, city, neighborhood,color,car_make, city_agency,areas_of_study,subjects_in_school
# school_levels,university_names,building_Classification,vehicle_Type,type_of_location,Parks_Playgrounds


# semantic_label = ['person_name',
# 				  'business_name',
# 				  'phone_number', 
# 				  'address', 
# 				  'street_name', 
# 				  'city', 
# 				  'neighborhood',
# 				  'lat_or_lon_coordinates',
# 				  'zipcode',
# 				  'borough',
# 				  'school_name',
# 				  'color',
# 				  'car_make',
# 				  'city_agency',
# 				  'areas_of_study',
# 				  'subjects_in_school',
# 				  'school_levels',
# 				  'college_or_university_names',
# 				  'websites',
# 				  'building_classification',
# 				  'vehicle_type',
# 				  'type_of_location',
# 				  'parks_or_playgrounds']









