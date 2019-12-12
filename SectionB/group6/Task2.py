#!/usr/bin/env python
# coding: utf-8
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SQLContext
import re
import json
import numpy as np

sc = SparkContext()
spark = SparkSession \
	.builder \
	.appName("project-task2") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

#read manually label file
task2_manual_labels_filepath = 'task2-manual-labels.json'

with open(task2_manual_labels_filepath, 'r') as json_file:
	task2_manual_labels = json.load(json_file)

#leverage to one level dict
list_per_file_dict = task2_manual_labels["actual_types"]
manually_label_dict = {}
for per_file_dict in list_per_file_dict:
	filename = per_file_dict["column_name"]
	filelabels_list = per_file_dict["manual_labels"]
	per_file_list = []
	for i in filelabels_list:
		per_file_list.append(i["semantic_type"])
	manually_label_dict[filename] = per_file_list

cluster = sc.textFile('cluster3.txt').flatMap(lambda x : x.split(', ')).map(lambda x : x[1:-1]).collect()
#cluster = ['5694-9szk.Business_Website_or_Other_URL.txt.gz', 'uwyv-629c.StreetName.txt.gz']

def put_in_dict(label, dict, frequency):
	if label in dict.keys():
		dict[label] += frequency
	else:
		dict[label] = {}
		dict[label] = frequency

def get_instance_and_frequency(rows, i):
	instance_name = rows[i][0]
	if(len(rows[i]) <= 1):
		instance_frequency = 1
	else:
		instance_frequency = int(rows[i][1])

	return instance_name, instance_frequency

def similarity_percent(strA, strB):
	a_len = len(strA)
	b_len = len(strB)
	matrix = np.zeros((b_len+1, a_len+1))
	for i in range(1,a_len+1):
		matrix[0][i] = i
	for j in range(1,b_len+1):
		matrix[j][0] = j
	cost = 0
	for i in range(1,a_len+1):
		for j in range(1,b_len+1):
			if strA[i-1] == strB[j-1]:
				cost = 0
			else:
				cost = 1
			edit_exchange_dis = matrix[j-1][i-1] + cost
			edit_add_dis = matrix[j-1][i] + 1
			edit_del_dis = matrix[j][i-1] + 1
			matrix[j][i] = min(edit_exchange_dis,edit_add_dis,edit_del_dis)
	return 1 - matrix[b_len][a_len]/max(a_len, b_len)

def is_empty_or_contains_special_chars(instance_name):
	if not instance_name or instance_name == 'na' or instance_name == 'n/a':
		return True
	regex = re.compile(r'.*[|\^&+%*&#=!>]+.*')
	if regex.match(instance_name):
		return True
	else:
		return False

def to_normal_case(instance_name):
	words = instance_name.split()
	for i in range(len(words)):
		words[i] = words[i].capitalize()
	return ' '.join(word for word in words)

def is_person_name(instance_name):
	return True

def is_business_name(instance_name):
	return True
	

def is_phone_number(instance_name):
	regex = re.compile(r'^\D?(\d{3})\D?\D?(\d{3})\D?(\d{4})$')
	if (regex.match(instance_name)):
		return True
	else:
		return False

def is_address(instance_name):
	regex_addr_start = re.compile(r'\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)', re.IGNORECASE)
	regex_addr_end = re.compile(r'\d{1,4} (?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\s\w+', re.IGNORECASE)
	regex_box = re.compile(r'P\.? ?O\.? Box \d+', re.IGNORECASE)
	if (regex_box.match(instance_name) or regex_addr_start.match(instance_name) or regex_addr_end.match(instance_name)):
		return True
	else:
		return False

def is_street(instance_name):
	regex_start = re.compile(r'[\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd|place)\W?(?=\s|$)', re.IGNORECASE)
	regex_end = re.compile(r'(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd|place)[\s\w]{1, 20}', re.IGNORECASE)
	if (regex_start.match(instance_name) or regex_end.match(instance_name)):
		return True
	else:
		return False

def is_city(instance_name):
	sets = {"New York","Buffalo","Rochester","Yonkers","Syracuse","Albany","New Rochelle","Mount Vernon","Schenectady",\
	"Utica","White Plains","Hempstead","Troy","Niagara Falls","Binghamton","Freeport","Valley Stream","Long Beach","Spring Valley","Rome",\
	"Ithaca","North Tonawanda","Poughkeepsie","Port Chester","Jamestown","Harrison","Newburgh","Saratoga Springs","Middletown","Glen Cove",\
	"Elmira","Lindenhurst","Auburn","Kiryas Joel","Rockville Centre","Ossining","Peekskill","Watertown","Kingston","Garden City","Lockport",\
	"Plattsburgh","Mamaroneck","Lynbrook","Mineola","Cortland","Scarsdale","Cohoes","Lackawanna","Amsterdam","Massapequa Park","Oswego","Rye",\
	"Floral Park","Westbury","Depew","Kenmore","Gloversville","Tonawanda","Glens Falls","Mastic Beach","Batavia","Johnson City","Oneonta","Beacon",\
	"Olean","Endicott","Geneva","Patchogue","Haverstraw","Babylon","Tarrytown","Dunkirk","Mount Kisco","Dobbs Ferry","Lake Grove","Fulton","Oneida",\
	"Woodbury","Suffern","Corning","Fredonia","Ogdensburg","West Haverstraw","Great Neck","Sleepy Hollow","Canandaigua","Lancaster","Massena",\
	"Watervliet","New Hyde Park","East Rockaway","Potsdam","Amityville","Rye Brook","Hamburg","Farmingdale","Rensselaer","New Square","Airmont","Newark",\
	"Monroe","Malverne","Port Jervis","Croton-on-Hudson","Johnstown","Brockport","Geneseo","Chestnut Ridge","Hornell","Briarcliff Manor","Baldwinsville",\
	"Hastings-on-Hudson","Port Jefferson","Ilion","Colonie","Scotia","Herkimer","Williston Park","East Hills","Northport","Great Neck Plaza","Pleasantville",\
	"Pelham","New Paltz","Nyack","Hudson Falls","Warwick","Bayville","Manorhaven","Cedarhurst","Walden","Lawrence","North Hills","North Syracuse","Tuckahoe",\
	"Irvington","Norwich","East Rochester","Canton","Bronxville","Monticello","Horseheads","Solvay","Larchmont","East Aurora","Hudson","Kaser","Wesley Hills","Albion",\
	"New Hempstead","Hilton","Washingtonville","Elmsford","Medina","Webster","Malone","Fairport","Pelham Manor","Bath","Salamanca","Wappingers Falls","Goshen","Kings Point",\
	"Saranac Lake","Williamsville","Ballston Spa","Sea Cliff","Waterloo","Island Park","Mechanicville","Flower Hill","Penn Yan","Old Westbury","Chittenango","Ardsley",\
	"Little Falls","Montebello","Cobleskill","Montgomery","Canastota","Dansville","Manlius","Wellsville","Springville","Chester","Le Roy","Hamilton","Alfred","Fayetteville",\
	"Liberty","Waverly","Owego","Ellenville","Menands","Maybrook","Spencerport","Elmira Heights","Highland Falls","Saugerties"}

	normal_instance_name = to_normal_case(instance_name)

	if normal_instance_name in sets:
		return True
	return False

def is_neighborhood(instance_name):
	set_brooklyn = {'Bath Beach', 'Bay Ridge',	'Bedford Stuyvesant', 'Bensonhurst', 'Bergen Beach', 'Boerum Hill',\
	'Borough Park',	'Brighton Beach', 'Broadway Junction', 'Brooklyn Heights', 'Brownsville', 'Bushwick',\
	'Canarsie', 'Carroll Gardens',	'City Line', 'Clinton Hill', 'Cobble Hill',	'Coney Island',	'Crown Heights',\
	'Cypress Hills', 'Ditmas Park',	'Downtown',	'DUMBO', 'Dyker Heights', 'East Flatbush', 'East New York',	'East Williamsburg',\
	'Farragut',	'Flatbush',	'Flatlands', 'Fort Greene',	'Fort Hamilton', 'Fulton Ferry', 'Georgetown', 'Gerritsen Beach',\
	'Gowanus', 'Gravesend',	'Greenpoint', 'Highland Park', 'Homecrest',	'Kensington', 'Kings Highway', 'Manhattan Beach',\
	'Manhattan Terrace', 'Mapleton', 'Marine Park',	'Midwood', 'Mill Basin', 'Mill Island',	'Navy Yard', 'New Lots', 'North Side',\
	'Ocean Hill', 'Ocean Parkway', 'Paerdegat Basin', 'Park Slope',	'Plum Beach', 'Prospect Heights', 'Prospect Lefferts Gardens',\
	'Prospect Park South', 'Red Hook', 'Remsen Village', 'Rugby', 'Sea Gate', 'Sheepshead Bay', 'South Side',\
	'Spring Creek',	'Starrett City', 'Stuyvesant Heights', 'Sunset Park', 'Tompkins Park North', 'Vinegar Hill',\
	'Weeksville', 'West Brighton', 'Williamsburg', 'Windsor Terrace', 'Wingate'}
	set_bronx = {'Allerton','Bathgate','Baychester','Bedford Park','Belmont','Bronxdale','Bronx Park South','Bronx River',\
	'Castle Hill','City Island','Claremont Village','Clason Point',	'Concourse','Concourse Village','Co-op City','Country Club',\
	'East Tremont',	'Eastchester','Edenwald','Edgewater Park','Fieldston','Fordham','High Bridge','Hunts Point',\
	'Kingsbridge','Kingsbridge Heights','Longwood',	'Marble Hill','Melrose','Morris Heights','Morris Park','Morrisania','Mott Haven',\
	'Mount Eden','Mount Hope','North Riverdale','Norwood','Olinville','Parkchester','Pelham Bay','Pelham Gardens',\
	'Pelham Parkway','Port Morris',	'Riverdale','Schuylerville','Soundview','Spuyten Duyvil','Throgs Neck',	'Unionport',\
	'University Heights','Van Nest','Wakefield','West Farms','Westchester Square','Williamsbridge','Woodlawn'}
	set_manhattan = {'Battery Park City','Beekman Place','Carnegie Hill','Chelsea',	'Chinatown','Civic Center','Clinton',\
	'East Harlem','East Village','Financial District','Flatiron','Gramercy','Greenwich Village','Hamilton Heights',\
	'Harlem (Central)','Herald Square','Hudson Square','Inwood','Lenox Hill','Lincoln Square','Little Italy',\
	'Lower East Side','Manhattan Valley','Manhattanville','Midtown South','Midtown','Morningside Heights',\
	'Murray Hill','NoHo','Roosevelt Island','SoHo','South Village','Stuyvesant Town','Sutton Place','Times Square',\
	'Tribeca','Tudor City','Turtle Bay','Union Square','Upper East Side','Upper West Side','Wall Street','Washington Heights',\
	'West Village','Yorkville'}																																							
	set_queens = {'Arverne','Astoria','Astoria Heights','Auburndale','Bay Terrace','Bayside','Bayswater','Beechhurst','Bellaire','Belle Harbor',\
	'Bellerose','Blissville','Breezy Point','Briarwood','Broad Channel','Brookville','Cambria Heights','Clearview','College Point','Douglaston',\
	'Dutch Kills','East Elmhurst','Edgemere','Elmhurst','Far Rockaway','Floral Park','Flushing','Flushing (Downtown)','Forest Hills',\
	'Forest Hills Gardens','Fresh Meadows','Glen Oaks','Glendale','Hammels','Hillcrest','Hollis','Holliswood','Howard Beach',\
	'Hunters Point','Jackson Heights','Jamaica','Jamaica Center','Jamaica Estates','Jamaica Hills','Kew Gardens',\
	'Kew Gardens Hills','Laurelton','Lefrak City','Lindenwood','Little Neck','Long Island City','Malba',\
	'Maspeth','Middle Village','Murray Hill','Neponsit','New Hyde Park','North Corona','Oakland Gardens','Ozone Park','Pomonok',\
	'Queens Village','Queensboro Hill','Ravenswood','Rego Park','Richmond Hill','Ridgewood','Rochdale','Rockaway Park',\
	'Rosedale','Roxbury','Seaside','Somerville','South Corona','South Jamaica','South Ozone Park','Springfield Gardens',\
	'St. Albans','Steinway','Sunnyside','Sunnyside Gardens','Utopia','Whitestone','Woodhaven','Woodside'}
	set_staten_island = {'Annadale','Arden Heights','Arlington','Arrochar','Bay Terrace','Bloomfield','Bulls Head','Butler Manor',\
	'Castleton Corners','Charleston','Chelsea','Clifton','Concord','Dongan Hills','Egbertville','Elm Park',\
	'Eltingville','Emerson Hill','Fox Hills','Graniteville','Grant City','Grasmere','Great Kills','Greenridge',\
	'Grymes Hill','Heartland Village','Howland Hook','Huguenot','Lighthouse Hill','Livingston','Manor Heights',\
	'Mariner\'s Harbor','Midland Beach','New Brighton','New Dorp','New Dorp Beach','New Springville','Oakwood','Old Place',\
	'Old Town','Park Hill','Pleasant Plains','Port Ivory','Port Richmond','Prince\'s Bay','Randall Manor','Richmond Town',\
	'Richmond Valley','Rosebank','Rossville','Sandy Ground','Shore Acres','Silver Lake','South Beach','St. George',\
	'Stapleton','Sunnyside','Todt Hill','Tompkinsville','Tottenville','Travis','Ward Hill','West Brighton',\
	'Westerleigh','Willowbrook','Woodrow'}

	sets = set()
	sets = sets.union(set_brooklyn)
	sets = sets.union(set_bronx)
	sets = sets.union(set_manhattan)
	sets = sets.union(set_queens)
	sets = sets.union(set_staten_island)

	normal_instance_name = to_normal_case(instance_name)

	if normal_instance_name in sets:
		return True
	return False

def is_lat_lon(instance_name):
	if (instance_name[0] == '(' and instance_name[-1] == ')'):
		lat, lon = instance_name[1:-1].split(', ')
		try:
			float(lat)
			float(lon)
			return True
		except:
			return False
	return False

def is_zipcode(instance_name):
	regex = re.compile(r'\d{5}(?:[-\s]\d{4})?$')
	if (regex.match(instance_name)):
		return True
	else:
		return False

def is_borough(instance_name):
	abbr_set = {"k","m","q","r","x"}
	full_set = {'bronx', 'brooklyn', 'manhattan', 'queens', 'staten island'}
	
	if instance_name in abbr_set:
		return True
	if instance_name in full_set:
		return True
	sets = full_set.union(abbr_set)
	for i in sets:
		if similarity_percent(i, instance_name) >= 0.5:
			return True
	return False

def is_school_name(instance_name):
	sets = {'hs', 'prep', 'school', 'academy', 'college', 'learning'}
	words = instance_name.split()
	for word in words:
		if word in sets:
			return True
		for i in sets:
			if similarity_percent(i, word) >= 0.5:
				return True
	return False

def is_color(instance_name):
	abbr_set = {'BK', 'BL', 'BG', 'BR', 'GL', 'GY', 'MR', 'OR', 'PK', 'PR', 'RD', 'TN', 'WH', 'YW'}
	full_set = {'black', 'blue', 'beige', 'brown', 'gold', 'gray', 'maroon', 'orange', 'pink', 'purple', 'red', 'tan', 'white', 'yellow'}

	if instance_name.upper() in abbr_set:
		return True
	if instance_name in full_set:
		return True

	for i in full_set:
		if (similarity_percent(i.lower(), instance_name) >= 0.5):
			return True
	return False	

def is_car_make(instance_name):
	sets = {"ACUR", "ALFA", "AMGN", "AMER", "ASTO", "AUDI", "AUST", "AVTI", "AUTU", "BENT", "BERO", "BLUI",\
			"BMW", "BRIC", "BROC", "BSA", "BUIC", "CADI", "CHEC", "CHEV", "CHRY", "CITR", "DAEW", "DAIH", "DATS",\
			"DELO", "DESO", "DIAR", "DINA", "DIVC", "DODG", "DUCA", "EGIL", "EXCL", "FERR", "FIAT", "FORD", "FRHT",\
			"FWD", "GZL", "GMC", "GRUM", "HD", "HILL", "HINO", "HOND", "HUDS", "HYUN", "CHRY", "INFI", "INTL",\
			"ISU", "IVEC", "JAGU", "JENS", "AMER", "AMER", "KAWK", "KW", "KIA", "LADA", "LAMO", "LNCI", "LNDR",\
			"LEXS", "LINC", "LOTU", "MACK", "MASE", "MAYB", "MAZD", "MCIN", "MERZ", "MERC", "MERK", "MG", "MITS",\
			"MORG", "MORR", "MOGU", "NAVI", "NEOP", "NISS", "NORT", "OLDS", "OPEL", "ONTR", "OSHK", "PACK", "PANZ",\
			"PTRB", "PEUG", "PLYM","PONT", "PORS", "RELA", "RENA", "ROL", "SAA", "STRN", "SCAN", "SIM", "SIN",\
		"STLG", "STU", "STUZ", "SUBA", "SUNB", "SUZI", "THMS", "TOYT", "TRIU", "TVR", "UD", "VCTY", "VOLK",\
		"VOLV", "WSTR", "WHIT", "WHGM", "AMER", "YAMA", "YUGO"}
	
	if instance_name.upper() in sets:
		return True

	for i in sets:
		#print(instance_name, similarity_percent(i, instance_name))
		#print(similarity_percent(i, instance_name))
		if similarity_percent(i.lower(), instance_name) >= 0.5:
			return True
	return False

def is_city_agency(instance_name):
	abbr_set = {"311","ACS","BIC","BOE","BPL","CCHR","CCRB","CUNY","DCA","DCAS","DCLA","DCP","DDC","DEP","DFTA",\
	"DHS","DOB","DOC","DOE","DOF","DOHMH","DOI","DOITT","DOP","DOR","DOT","DPR","DSNY","DVS","DYCD","EDC",\
	"FDNY","HPD","HRA","LAW","LPC","NYCEM","NYCHA","NYPD","NYPL","OATH","OCME","QPL","SBS","SCA","TLC"}

	if instance_name.upper() in abbr_set:
		return True

	lists = ["office", "dept", "city", "district", "president", "board", "comm", "department", "agency", "library"]
	
	for i in lists:
		if i in instance_name:
			return True
	
	return False

def is_area_of_study(instance_name):
	lists = ["ARCHITECTURE", "SCIENCE", "ART", "TEACHING", "TEACHING", "BUSINESS", "COMMUNICATIONS", "COSMETOLOGY",\
	"ENGINEERING", "HUMANITIES", "TECHNOLOGY", "HEALTH", "ECONOMICS", "ENVIRONMENT", "ALGEBRA", "CHEMISTRY",\
	"ENGLISH", "MATH","SOCIAL STUDIES"]
	for i in lists:
		if i.lower() in instance_name:
			return True
	return False

def is_subjects_in_school(instance_name):
	regex = re.compile(r'[A-Za-z]+ (\d{1,2}|[A-Za-z])?', re.IGNORECASE)
	if (regex.match(instance_name)):
		return True
	else:
		return False

def is_school_levels(instance_name):
	lists = {'K-8','K-2','K-3' 'Elementary', 'Middle', 'Transfer', 'High', 'D75','YABC'}
	for i in lists:
		if i.lower() in instance_name:
			return True
	return False

def is_college_university(instance_name):
	if 'university' in instance_name:
		return True
	return False
	
def is_websites(instance_name):
	regex = re.compile(r'^(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$', re.IGNORECASE)
	if(regex.match(instance_name)):
		return True
	else:
		return False

def is_building_classification(instance_name):
	#print('in func', instance_name)
	regex = re.compile(r'^[A-WYZ][0-9|MUWBHRS]-.*', re.IGNORECASE)
	if(regex.match(instance_name)):
		return True
	else:
		return False

def is_vehicle_type(instance_name):
	sets = {"FIRE", "CONV", "SEDN", "SUBN", "4DSD", "2DSD", "H/WH", "ATV", "MCY", "H/IN", "LOCO", "RPLC",\
			"AMBU", "P/SH", "RBM", "R/RD", "RD/S", "S/SP", "SN/P", "TRAV", "MOBL", "TR/E", "T/CR", "TR/C", "SWT",\
			"W/DR", "W/SR", "FPM", "MCC", "EMVR", "TRAC", "DELV", "DUMP", "FLAT", "PICK", "STAK", "TANK",\
			"REFG", "TOW", "VAN", "UTIL", "POLE", "BOAT", "H/TR", "SEMI", "TRLR", "LTRL", "LSVT", "BUS", "LIM",\
			"HRSE", "TAXI", "DCOM", "CMIX", "MOPD", "MFH", "SNOW", "LSV"}
	
	if instance_name.upper() in sets:
		return True
	for i in sets:
		if similarity_percent(i.lower(), instance_name) >= 0.5:
			return True
	return False

def is_type_of_location(instance_name):
	lists = ["BUILDING", "AIRPORT", "ATM", "BANK", "CLUB", "BRIDGE", "TERMINAL", "STORE", "OFFICE",\
			"STATION", "HOSPITAL", "RESIDENCE", "RESTAURANT", "TUNNEL", "PARK", "SHELTER", "MAILBOX", "SCHOOL", "STREET",\
			"TRANSIT", "STOP", "FACTORY"]
	for i in lists:
		if i.lower() in instance_name:
			return True
	return False

def is_park_playground(instance_name):
	lists = ['park', 'playground', 'garden']
	for i in lists:
		if i in instance_name:
			return True
	return False


pc_pred_dict = {}
for file in cluster:
	filepath = '/user/hm74/NYCColumns/' + file
	DF = spark.read.option("header", "false").option("delimiter", "\t").csv(filepath).cache()
	rows = DF.collect()
	column_name = file.replace('.txt.gz', '')
	revised_col_name = file.split(".")[1].lower().replace('_', '')
	dictPrediction = {}
	dictPrediction["column_name"] = column_name
	dictPrediction["semantic_types"] = {}
	listPredTypes = []
	print('accept file', file)

	for i in range(len(rows)):
		
		instance_name, instance_frequency = get_instance_and_frequency(rows, i)
		
		if (instance_name is None):
			label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)
			continue

		instance_name = instance_name.strip().lower()

		if (is_empty_or_contains_special_chars(instance_name)):
			label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)
			continue

		elif 'first' in revised_col_name or 'last' in revised_col_name or 'mi' == revised_col_name or 'person' in revised_col_name:
		
			
			if (is_person_name(instance_name)):
				label = "Person_name"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'dba' in revised_col_name or 'businessname' in revised_col_name:
		
			
			if (is_business_name(instance_name)):
				label = "Business_name"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'number' in revised_col_name or 'phone' in revised_col_name:
		
			
			if (is_phone_number(instance_name)):
				label = "Phone_Number"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'street' in revised_col_name:
		
			
			if (is_address(instance_name)):
				label = "Address"
			elif (is_street(instance_name)):
				label = "Street_name"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'agency' in revised_col_name:
		
			
			if (is_city_agency(instance_name)):
				label = "City_agency"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'city' in revised_col_name or 'neighborhood' in revised_col_name or 'boro' in revised_col_name:
		
			
			if (is_city(instance_name)):
				label = "City"
			elif (is_neighborhood(instance_name)):
				label = "Neighborhood"
			elif (is_borough(instance_name)):
				label = "Borough"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)
			
		elif 'zip' in revised_col_name:
		
			
			if (is_zipcode(instance_name)):
				label = "Zip_code"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'schoollevel' in revised_col_name:
		
			
			if (is_school_levels(instance_name)):
				label = "School_Levels"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'school' in revised_col_name:
		
			
			if (is_school_name(instance_name)):
				label = "School_name"
			elif (is_school_levels(instance_name)):
				label = "School_Levels"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'color' in revised_col_name:
		
			
			if (is_color(instance_name)):
				label = "Color"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'make' in revised_col_name or 'vehicle' in revised_col_name:
		
			
			if (is_car_make(instance_name)):
				label = "Car_make"
			elif (is_vehicle_type(instance_name)):
				label = "Vehicle_Type"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)
	
		elif 'core' in revised_col_name or 'interest' in revised_col_name:
		
			
			if (is_area_of_study(instance_name)):
				label = "Areas_of_study"
			elif (is_subjects_in_school(instance_name)):
				label = "Subjects_in_school"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'website' in revised_col_name:
		
			
			if (is_websites(instance_name)):
				label = "Websites"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'classification' in revised_col_name:
		
			
			if (is_building_classification(instance_name)):
				label = "Building_Classification"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'typdesc' in revised_col_name:
		
			
			if (is_type_of_location(instance_name)):
				label = "Type_of_location"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'park' in revised_col_name or 'orgname' in revised_col_name:
		
			
			if (is_park_playground(instance_name)):
				label = "Parks_Playgrounds"
			elif (is_school_name(instance_name)):
				label = "School_name"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'location' in revised_col_name or 'lat_lon' in revised_col_name:
		
			
			if (is_lat_lon(instance_name)):
				label = "LAT_LON_coordinates"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		elif 'address' in revised_col_name:
		
			
			if (is_address(instance_name)):
				label = "Address"
			elif (is_street(instance_name)):
				label = "Street_name"
			elif (is_college_university(instance_name)):
				label = "College_University_names"
			elif (is_park_playground(instance_name)):
				label = "Parks_Playgrounds"
			else:
				label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)

		else:

			label = "Other"
			put_in_dict(label, dictPrediction["semantic_types"], instance_frequency)



# Get JSON file as output:
# { "column_name": string
#	"semantic_types": [
#	{ "semantic_type": "Business name" (type: string)
#	  "count": the number of type(type: integer)
#	   },
#	...
#	]
# }
	types_list = []
	for key, value in dictPrediction["semantic_types"].items():
		new_dict = {}
		new_dict['semantic_type'] = key
		new_dict['count'] = value
		listPredTypes.append(key)
		types_list.append(new_dict)
	pc_pred_dict[column_name] = listPredTypes
	dictPrediction["semantic_types"] = types_list
	json_file_path = 'project_task2/' + column_name + '.json'
	with open(json_file_path, 'w') as json_file:
		json.dump(dictPrediction, json_file)
	DF.unpersist()

dictPredCount = {}
for key, value in pc_pred_dict.items():
	for predType in value:
		if predType in dictPredCount.keys():
			dictPredCount[predType] += 1
		else:
			dictPredCount[predType] = 1

dictManualCount = {}
for key, value in manually_label_dict.items():
	for manualType in value:
		if manualType in dictManualCount.keys():
			dictManualCount[manualType] += 1
		else:
			dictManualCount[manualType] = 1

dictMatchCount = {}
for key, value in pc_pred_dict.items():
	for predType in value:
		if predType in manually_label_dict[key]:
			if predType in dictMatchCount.keys():
				dictMatchCount[predType] += 1
			else:
				dictMatchCount[predType] = 1

all_types = ['Person_name', 'Business_name', 'City_agency', 'Neighborhood', 'Building_Classification', 'Areas_of_study',\
'School_Levels', 'Borough', 'Subjects_in_school', 'Parks_Playgrounds', 'Zip_code', 'Address', 'Street_name', 'Phone_Number',\
'City', 'LAT_LON_coordinates', 'School_name', 'Car_make', 'Vehicle_Type', 'Type_of_location', 'Websites', 'Color', \
'College_University_names', 'Other']

list_percent = []
list_recall = []
for each in all_types:
	matchcount = 0.0
	if each not in dictMatchCount.keys():
		#print("%s\t%s = %.5f\t%s = %.5f\n" % (each, 'precison', matchcount, 'recall', matchcount))
		list_percent.append(matchcount)
		list_recall.append(matchcount)
		continue
	else:
		matchcount = float(dictMatchCount[each])
	manualcount = dictManualCount[each]
	predcount = dictPredCount[each]
	#print("%s\t%s = %.5f\t%s = %.5f\n" % (each, 'precison', matchcount/predcount, 'recall', matchcount/manualcount))
	list_percent.append(matchcount/predcount)
	list_recall.append(matchcount/manualcount)

print('list_percent', list_percent)
print('list_recall', list_recall)

