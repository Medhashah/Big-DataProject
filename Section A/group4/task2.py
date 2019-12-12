#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, BooleanType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from datetime import datetime, date
from dateutil.parser import parse
import json
import numpy as np
import re

sc = SparkContext()

#filePath = sm48-t3s6.tsv.gz

spark = SparkSession.builder.appName("project").config("spark.some.config.option"", ""some-value").getOrCreate()
sqlContext = SQLContext(spark)

#files
#columnNameDF = sc.textFile(filePath).map(lambda x : x.split('\t'))
cluster = sc.textFile("cluster2.txt").flatMap(lambda x: x.split(", ")).map(lambda x : x[1:-1]).collect()

labeList = ["person_name", "business_name", "phone_number", "address", "street_name", "city", "neighborhood", "lat_lon_cord", "zip_code", "borough", "school_name", "color", "car_make", "city_agency", "area_of_study", "subject_in_school", "school_level", "college_name", "website", "building_classification", "vehicle_type", "location_type", "park_playground", "other"]


typeStaticsDic = {"person_name" : 0, "business_name" : 0, "phone_number" : 0, "address" : 0, "street_name" : 0, "city" : 0, "neighborhood" : 0, "lat_lon_cord" : 0, "zip_code" : 0, "borough" : 0, "school_name" : 0, "color" : 0, "car_make" : 0, "city_agency" : 0, "area_of_study" : 0, "subject_in_school" : 0, "school_level" : 0, "college_name" : 0, "website" : 0, "building_classification" : 0, "vehicle_type" : 0, "location_type" : 0, "park_playground" : 0, "other" : 0}


labeledCluster = spark.read.csv("labeledCluster.csv", header=True, sep=",")

filesList = labeledCluster.rdd.map(lambda x : (x[0].strip()+'.'+x[1].strip()+".txt.gz", x[2])).collect()
#Convert our previous label to new label in the given label list
labelMap = {"Person name":"person_name", "Business name":"business_name", "Phone Number":"phone_number", "Address":"address", "Street name":"street_name", "City":"city", "Neighborhood":"neighborhood", "LAT/LON coordinates":"lat_lon_cord", "Zip code":"zip_code", "Borough":"borough", "School name":"school_name", "Color":"color", "Car make":"car_make", "City agency":"city_agency","Areas of study":"area_of_study", "Subjects in school":"subject_in_school", "School Levels":"school_level", "College/University names":"college_name", "Websites":"website", "Building Classification":"building_classification", "Vehicle Type":"vehicle_type", "Type of location" : "location_type", "Parks/Playgrounds" : "park_playground", "Other" : "other"}

#Processing the manually labeled columns
fileLabelDic = {}
labeledFileStatics = typeStaticsDic
for files in filesList:
	labeledTypesInFile = [labelMap[string.strip()] for string in files[1].split(", ")]
	fileLabelDic[str(files[0])] = labeledTypesInFile
	#print(*labeledTypesInFile)
	for labeledType in labeledTypesInFile:
		labeledFileStatics[labeledType] += 1

# The following sets or lists is the frequent values for each label. We get them for internet 
vehicleTypeList = {"FIRE", "CONV", "SEDN", "SUBN", "4DSD", "2DSD", "H/WH", "ATV", "MCY", "H/IN", "LOCO", "RPLC", "AMBU", "P/SH", "RBM", "R/RD", "RD/S", "S/SP", "SN/P", "TRAV", "MOBL", "TR/E", "T/CR", "TR/C", "SWT", "W/DR", "W/SR", "FPM", "MCC", "EMVR", "TRAC", "DELV", "DUMP", "FLAT", "PICK", "STAK", "TANK", "REFG", "TOW", "VAN", "UTIL", "POLE", "BOAT", "H/TR", "SEMI", "TRLR", "LTRL", "LSVT", "BUS", "LIM", "HRSE", "TAXI", "DCOM", "CMIX", "MOPD", "MFH", "SNOW", "LSV"}

carMakeList = {"ACUR", "ALFA", "AMGN", "AMER", "ASTO", "AUDI", "AUST", "AVTI", "AUTU", "BENT", "BERO", "BLUI", "BMW", "BRIC", "BROC", "BSA", "BUIC", "CADI", "CHEC", "CHEV", "CHRY", "CITR", "DAEW", "DAIH", "DATS", "DELO", "DESO", "DIAR", "DINA", "DIVC", "DODG", "DUCA", "EGIL", "EXCL", "FERR", "FIAT", "FORD", "FRHT", "FWD", "GZL", "GMC", "GRUM", "HD", "HILL", "HINO", "HOND", "HUDS", "HYUN", "CHRY", "INFI", "INTL", "ISU", "IVEC", "JAGU", "JENS", "AMER", "AMER", "KAWK", "KW", "KIA", "LADA", "LAMO", "LNCI", "LNDR", "LEXS", "LINC", "LOTU", "MACK", "MASE", "MAYB", "MAZD", "MCIN", "MERZ", "MERC", "MERK", "MG", "MITS", "MORG", "MORR", "MOGU", "NAVI", "NEOP", "NISS", "NORT", "OLDS", "OPEL", "ONTR", "OSHK", "PACK", "PANZ", "PTRB", "PEUG", "PLYM","PONT", "PORS", "RELA", "RENA", "ROL", "SAA", "STRN", "SCAN", "SIM", "SIN", "STLG", "STU", "STUZ", "SUBA", "SUNB", "SUZI", "THMS", "TOYT", "TRIU", "TVR", "UD", "VCTY", "VOLK", "VOLV", "WSTR", "WHIT", "WHGM", "AMER", "YAMA", "YUGO"}

locationTypeList = ["BUILDING", "AIRPORT", "ATM", "BANK", "CLUB", "BRIDGE", "TERMINAL", "STORE", "OFFICE", "STATION", "HOSPITAL", "RESIDENCE", "RESTAURANT", "TUNNEL", "PARK", "SHELTER", "MAILBOX", "SCHOOL", "STREET", "TRANSIT", "STOP", "FACTORY"]

areaStudyList = ["ARCHITECTURE", "SCIENCE", "ART", "TEACHING", "TEACHING", "BUSINESS", "COMMUNICATIONS", "COSMETOLOGY", "ENGINEERING", "HUMANITIES", "TECHNOLOGY", "HEALTH", "ECONOMICS", "ENVIRONMENT", "ALGEBRA", "CHEMISTRY", "ENGLISH", "MATH","SOCIAL STUDIES"]

cityNameSet = {"New York","Buffalo","Rochester","Yonkers","Syracuse","Albany","New Rochelle","Mount Vernon","Schenectady","Utica","White Plains","Hempstead","Troy","Niagara Falls","Binghamton","Freeport","Valley Stream","Long Beach","Spring Valley","Rome","Ithaca","North Tonawanda","Poughkeepsie","Port Chester","Jamestown","Harrison","Newburgh","Saratoga Springs","Middletown","Glen Cove","Elmira","Lindenhurst","Auburn","Kiryas Joel","Rockville Centre","Ossining","Peekskill","Watertown","Kingston","Garden City","Lockport","Plattsburgh","Mamaroneck","Lynbrook","Mineola","Cortland","Scarsdale","Cohoes","Lackawanna","Amsterdam","Massapequa Park","Oswego","Rye","Floral Park","Westbury","Depew","Kenmore","Gloversville","Tonawanda","Glens Falls","Mastic Beach","Batavia","Johnson City","Oneonta","Beacon","Olean","Endicott","Geneva","Patchogue","Haverstraw","Babylon","Tarrytown","Dunkirk","Mount Kisco","Dobbs Ferry","Lake Grove","Fulton","Oneida","Woodbury","Suffern","Corning","Fredonia","Ogdensburg","West Haverstraw","Great Neck","Sleepy Hollow","Canandaigua","Lancaster","Massena","Watervliet","New Hyde Park","East Rockaway","Potsdam","Amityville","Rye Brook","Hamburg","Farmingdale","Rensselaer","New Square","Airmont","Newark","Monroe","Malverne","Port Jervis","Croton-on-Hudson","Johnstown","Brockport","Geneseo","Chestnut Ridge","Hornell","Briarcliff Manor","Baldwinsville","Hastings-on-Hudson","Port Jefferson","Ilion","Colonie","Scotia","Herkimer","Williston Park","East Hills","Northport","Great Neck Plaza","Pleasantville","Pelham","New Paltz","Nyack","Hudson Falls","Warwick","Bayville","Manorhaven","Cedarhurst","Walden","Lawrence","North Hills","North Syracuse","Tuckahoe","Irvington","Norwich","East Rochester","Canton","Bronxville","Monticello","Horseheads","Solvay","Larchmont","East Aurora","Hudson","Kaser","Wesley Hills","Albion","New Hempstead","Hilton","Washingtonville","Elmsford","Medina","Webster","Malone","Fairport","Pelham Manor","Bath","Salamanca","Wappingers Falls","Goshen","Kings Point","Saranac Lake","Williamsville","Ballston Spa","Sea Cliff","Waterloo","Island Park","Mechanicville","Flower Hill","Penn Yan","Old Westbury","Chittenango","Ardsley","Little Falls","Montebello","Cobleskill","Montgomery","Canastota","Dansville","Manlius","Wellsville","Springville","Chester","Le Roy","Hamilton","Alfred","Fayetteville","Liberty","Waverly","Owego","Ellenville","Menands","Maybrook","Spencerport","Elmira Heights","Highland Falls","Saugerties"}

brooklynSet = {'Bath Beach', 'Bay Ridge', 'Bedford Stuyvesant', 'Bensonhurst', 'Bergen Beach', 'Boerum Hill',\
 'Borough Park', 'Brighton Beach', 'Broadway Junction', 'Brooklyn Heights', 'Brownsville', 'Bushwick',\
 'Canarsie', 'Carroll Gardens', 'City Line', 'Clinton Hill', 'Cobble Hill', 'Coney Island', 'Crown Heights',\
 'Cypress Hills', 'Ditmas Park', 'Downtown', 'DUMBO', 'Dyker Heights', 'East Flatbush', 'East New York', 'East Williamsburg',\
 'Farragut', 'Flatbush', 'Flatlands', 'Fort Greene', 'Fort Hamilton', 'Fulton Ferry', 'Georgetown', 'Gerritsen Beach',\
 'Gowanus', 'Gravesend', 'Greenpoint', 'Highland Park', 'Homecrest', 'Kensington', 'Kings Highway', 'Manhattan Beach',\
 'Manhattan Terrace', 'Mapleton', 'Marine Park', 'Midwood', 'Mill Basin', 'Mill Island', 'Navy Yard', 'New Lots', 'North Side',\
 'Ocean Hill', 'Ocean Parkway', 'Paerdegat Basin', 'Park Slope', 'Plum Beach', 'Prospect Heights', 'Prospect Lefferts Gardens',\
 'Prospect Park South', 'Red Hook', 'Remsen Village', 'Rugby', 'Sea Gate', 'Sheepshead Bay', 'South Side',\
 'Spring Creek', 'Starrett City', 'Stuyvesant Heights', 'Sunset Park', 'Tompkins Park North', 'Vinegar Hill',\
 'Weeksville', 'West Brighton', 'Williamsburg', 'Windsor Terrace', 'Wingate'}
bronxSet = {'Allerton','Bathgate','Baychester','Bedford Park','Belmont','Bronxdale','Bronx Park South','Bronx River',\
 'Castle Hill','City Island','Claremont Village','Clason Point', 'Concourse','Concourse Village','Co-op City','Country Club',\
 'East Tremont', 'Eastchester','Edenwald','Edgewater Park','Fieldston','Fordham','High Bridge','Hunts Point',\
 'Kingsbridge','Kingsbridge Heights','Longwood', 'Marble Hill','Melrose','Morris Heights','Morris Park','Morrisania','Mott Haven',\
 'Mount Eden','Mount Hope','North Riverdale','Norwood','Olinville','Parkchester','Pelham Bay','Pelham Gardens',\
 'Pelham Parkway','Port Morris', 'Riverdale','Schuylerville','Soundview','Spuyten Duyvil','Throgs Neck', 'Unionport',\
 'University Heights','Van Nest','Wakefield','West Farms','Westchester Square','Williamsbridge','Woodlawn'}
manhattanSet = {'Battery Park City','Beekman Place','Carnegie Hill','Chelsea', 'Chinatown','Civic Center','Clinton',\
 'East Harlem','East Village','Financial District','Flatiron','Gramercy','Greenwich Village','Hamilton Heights',\
 'Harlem (Central)','Herald Square','Hudson Square','Inwood','Lenox Hill','Lincoln Square','Little Italy',\
 'Lower East Side','Manhattan Valley','Manhattanville','Midtown South','Midtown','Morningside Heights',\
 'Murray Hill','NoHo','Roosevelt Island','SoHo','South Village','Stuyvesant Town','Sutton Place','Times Square',\
 'Tribeca','Tudor City','Turtle Bay','Union Square','Upper East Side','Upper West Side','Wall Street','Washington Heights',\
 'West Village','Yorkville'}                                       
queensSet = {'Arverne','Astoria','Astoria Heights','Auburndale','Bay Terrace','Bayside','Bayswater','Beechhurst','Bellaire','Belle Harbor',\
 'Bellerose','Blissville','Breezy Point','Briarwood','Broad Channel','Brookville','Cambria Heights','Clearview','College Point','Douglaston',\
 'Dutch Kills','East Elmhurst','Edgemere','Elmhurst','Far Rockaway','Floral Park','Flushing','Flushing (Downtown)','Forest Hills',\
 'Forest Hills Gardens','Fresh Meadows','Glen Oaks','Glendale','Hammels','Hillcrest','Hollis','Holliswood','Howard Beach',\
 'Hunters Point','Jackson Heights','Jamaica','Jamaica Center','Jamaica Estates','Jamaica Hills','Kew Gardens',\
 'Kew Gardens Hills','Laurelton','Lefrak City','Lindenwood','Little Neck','Long Island City','Malba',\
 'Maspeth','Middle Village','Murray Hill','Neponsit','New Hyde Park','North Corona','Oakland Gardens','Ozone Park','Pomonok',\
 'Queens Village','Queensboro Hill','Ravenswood','Rego Park','Richmond Hill','Ridgewood','Rochdale','Rockaway Park',\
 'Rosedale','Roxbury','Seaside','Somerville','South Corona','South Jamaica','South Ozone Park','Springfield Gardens',\
 'St. Albans','Steinway','Sunnyside','Sunnyside Gardens','Utopia','Whitestone','Woodhaven','Woodside'}
statenIslandSet = {'Annadale','Arden Heights','Arlington','Arrochar','Bay Terrace','Bloomfield','Bulls Head','Butler Manor',\
 'Castleton Corners','Charleston','Chelsea','Clifton','Concord','Dongan Hills','Egbertville','Elm Park',\
 'Eltingville','Emerson Hill','Fox Hills','Graniteville','Grant City','Grasmere','Great Kills','Greenridge',\
 'Grymes Hill','Heartland Village','Howland Hook','Huguenot','Lighthouse Hill','Livingston','Manor Heights',\
 'Mariner\'s Harbor','Midland Beach','New Brighton','New Dorp','New Dorp Beach','New Springville','Oakwood','Old Place',\
 'Old Town','Park Hill','Pleasant Plains','Port Ivory','Port Richmond','Prince\'s Bay','Randall Manor','Richmond Town',\
 'Richmond Valley','Rosebank','Rossville','Sandy Ground','Shore Acres','Silver Lake','South Beach','St. George',\
 'Stapleton','Sunnyside','Todt Hill','Tompkinsville','Tottenville','Travis','Ward Hill','West Brighton',\
 'Westerleigh','Willowbrook','Woodrow'}

neighborhoodSet = set()
neighborhoodSet = neighborhoodSet.union(brooklynSet)
neighborhoodSet = neighborhoodSet.union(bronxSet)
neighborhoodSet = neighborhoodSet.union(manhattanSet)
neighborhoodSet = neighborhoodSet.union(queensSet)
neighborhoodSet = neighborhoodSet.union(statenIslandSet)

abbrAgen = {"311","ACS","BIC","BOE","BPL","CCHR","CCRB","CUNY","DCA","DCAS","DCLA","DCP","DDC","DEP","DFTA","DHS","DOB","DOC","DOE","DOF","DOHMH","DOI","DOITT","DOP","DOR","DOT","DPR","DSNY","DVS","DYCD","EDC","FDNY","HPD","HRA","LAW","LPC","NYCEM","NYCHA","NYPD","NYPL","OATH","OCME","QPL","SBS","SCA","TLC"}
keyAgen = ["department", "dept", "office", "city", "district","admin", "board", "president", "commi"]
 
# compute minimum edit distance of 2 strings.
# This is used to compute the probability of 2 strings. 
# In this way, we can correctly get the similarity of 2 string.
def min_edit_dist(StrA,StrB):
     a_length = len(StrA)
     b_length = len(StrB)
     matrix = np.zeros((a_length+1, b_length+1))
     for i in range(1,a_length+1):
         matrix[i][0] = i
     for j in range(1,b_length+1):
         matrix[0][j] = j
     cost = 0
     for i in range(1,a_length+1):
         for j in range(1,b_length+1):
             if StrA[i-1] == StrB[j-1]:
                 cost = 0
             else:
                 cost = 1
             edit_exchange_dis = matrix[i-1][j-1] + cost
             edit_add_dis = matrix[i-1][j] + 1
             edit_del_dis = matrix[i][j-1] + 1
             matrix[i][j] = min(edit_exchange_dis,edit_add_dis,edit_del_dis)
     
     return 1 - matrix[a_length][b_length]/max(a_length,b_length)


# there are some label types that can be identified using python regular expression.
phoneRegex = re.compile(r'((?:(?<![\d-])(?:\+?\d{1,3}[-.\s*]?)?(?:\(?\d{3}\)?[-.\s*]?)?\d{3}[-.\s*]?\d{4}(?![\d-]))|(?:(?<![\d-])(?:(?:\(\+?\d{2}\))|(?:\+?\d{2}))\s*\d{2}\s*\d{3}\s*\d{4}(?![\d-])))')
addressRegex = re.compile(r'\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)', re.IGNORECASE)
zipRegex= re.compile(r'\b\d{5}(?:[-\s]\d{4})?\b')
po_box = re.compile(r'P\.? ?O\.? Box \d+', re.IGNORECASE)
streetNameRegex1 = re.compile(r'[\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)', re.IGNORECASE)
streetNameRegex2 = re.compile(r'(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\s\w+', re.IGNORECASE)

buildClassRegex = re.compile(r'[A-WYZ][0-9|MUWBHRS]-.*')
subjectRegex = re.compile(r'[A-Za-z]+ (\d{1,2}|[A-Za-z])')
websiteRegex = re.compile(r'^(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$', re.IGNORECASE)
#10305
def isNotValid(string):
	if (not string or string.lower() == 'na' or string.lower() == 'n/a'):
		return True
	regex = re.compile(r'.*[|\^&+%*&#=!>]+.*')
	if (regex.match(string.lower())):
		return True
	else:
		return False

def is_phone(phone):
	matchResult = phoneRegex.match(phone)
	if matchResult:
		return True
	else:
		return False

def is_zip(zipCode):
	matchResult = zipRegex.match(zipCode)
	if matchResult:
		return True
	else:
		return False

def is_url(url):
	if websiteRegex.match(url):
		return True
	return False

def is_dba(businnessName):
	if not businnessName:
		return False
	return True

def is_agency(agency):#min distance
	if agency.isalpha() and agency.isupper():
		if agency in abbrAgen:
			return True
	else:
		for dep in keyAgen:
			if dep in agency.lower():
				return True
	return False

def is_city(city):
	words = city.split()
	for i in range(len(words)):
		words[i] = words[i].capitalize()

	cityCapital = ' '.join(word for word in words)
	if cityCapital in cityNameSet:
		return True
	return False

def is_neighborhood(neighborhood):
	if neighborhood in neighborhoodSet:
		return True
	return False

def is_borough(borough):
	boroughLists = {'bronx', 'brooklyn', 'manhattan', 'queens', 'staten island'}
	abbrLists = {"k","m","q","r","x"}
	if (len(borough) == 1) and (borough.upper() in abbrLists):
		return True
	if (borough.lower() in boroughLists):
		return True
	lists = boroughLists.union(abbrLists)
	for i in lists:
		if (min_edit_dist(i, borough.lower()) >= 0.5):
				return True 
	return False

def is_streetName(street):
	streetNameMatch1 = streetNameRegex1.match(street)
	streetNameMatch2 = streetNameRegex2.match(street)
	if streetNameMatch1 or streetNameMatch2:
		return True
	return False

def is_address(address):
	addressMatch = addressRegex.match(address)
	poboxMatch = po_box.match(address)
	if addressMatch:
		return True
	elif poboxMatch:
		return True
	else:
		return False

def is_latLong(latlong):
	if latlong[0] == "(" and latlong[-1] == ")":
		doubles = latlong[1:-1].split(", ")
		try:
			float(doubles[0])
			float(doubles[1])
			return True
		except ValueError:
			return False
	return False

def is_schoolLevel(level):
	levelList = {"ELEMENTARY", "HIGH", "K-2", "K-3", "K-8", "MIDDLE", "YABC", "TRANSFER", "D75"}
	for l in levelList:
		if l.lower() in level.lower():
			return True
	return False

def is_schoolName(schoolName):
	schoolNameList = {"school", "academy", "college", "acad", "hs", "org"}
	abbre = "S."
	schoolWords = schoolName.split(" ")
	for words in schoolWords:
		if words.lower() in schoolNameList:
			return True
	if abbre in schoolName:
		return True
	return False

def is_college(collegeName):
	if "university" in collegeName.lower():
		return True
	return False


def is_color(color):
	abbrSet = {'BK', 'BL', 'BG', 'BR', 'GL', 'GY', 'MR', 'OR', 'PK', 'PR', 'RD', 'TN', 'WH', 'YW'}
	fullColorSet = {'black', 'blue', 'beige', 'brown', 'gold', 'gray', 'maroon', 'orange', 'pink', 'purple', 'red', 'tan', 'white', 'yellow'}
	if color.upper() in abbrSet:
		return True
	if color.lower() in fullColorSet:
		return True
	for i in fullColorSet:
		if (min_edit_dist(i, color.lower()) >= 0.5):
			return True
	return False
#test!!!
def is_carMake(carMake):
	if carMake.lower() in carMakeList:
		return True
	for make in carMakeList:
		if (min_edit_dist(make.lower(), carMake.lower()) >= 0.7):
			return True
	return False

def is_areaOfStudy(areaStudy):
	for study in areaStudyList:
		if study.lower() in areaStudy.lower():
			return True
	return False

def is_subjectsInSchool(subject):
	if subjectRegex.match(subject.lower()):
		return True
	return False

def is_buildingClassification(building):
	buildClassMatch = buildClassRegex.match(building)
	if buildClassMatch:
		return True
	return False
#test!!!
def is_vehicleType(vehicleType): 
	if vehicleType in vehicleTypeList:
		return True
	for types in vehicleTypeList:
		if (min_edit_dist(types.lower(), vehicleType.lower()) >= 0.7):
			return True
	return False

def is_typeOfLocation(location):
	for types in locationTypeList:
		if types.lower() in location.lower():
			return True
	return False

def is_parkPlayground(parkPlayground):
	lowerletterName = parkPlayground.lower()
	if "park" in lowerletterName or "playground" in lowerletterName:
		return True
	return False

# The following 2 dictionaries are used to compute precision and recall 
predictedTypeStatic = {"person_name" : 0, "business_name" : 0, "phone_number" : 0, "address" : 0, "street_name" : 0, "city" : 0, "neighborhood" : 0, "lat_lon_cord" : 0, "zip_code" : 0, "borough" : 0, "school_name" : 0, "color" : 0, "car_make" : 0, "city_agency" : 0, "area_of_study" : 0, "subject_in_school" : 0, "school_level" : 0, "college_name" : 0, "website" : 0, "building_classification" : 0, "vehicle_type" : 0, "location_type" : 0, "park_playground" : 0, "other" : 0}

correcylyPredictDic = {"person_name" : 0, "business_name" : 0, "phone_number" : 0, "address" : 0, "street_name" : 0, "city" : 0, "neighborhood" : 0, "lat_lon_cord" : 0, "zip_code" : 0, "borough" : 0, "school_name" : 0, "color" : 0, "car_make" : 0, "city_agency" : 0, "area_of_study" : 0, "subject_in_school" : 0, "school_level" : 0, "college_name" : 0, "website" : 0, "building_classification" : 0, "vehicle_type" : 0, "location_type" : 0, "park_playground" : 0, "other" : 0}

for file in cluster:
	print("Processing File : %s" % (file))
	outputJson = {}
	filePath = "/user/hm74/NYCColumns/" + file
	fileDF = sc.textFile(filePath).map(lambda x : x.split('\t')).cache()
	columnNameDF = fileDF.collect()
	columnName = file.split(".")[1].lower()
	replacedColumnName = columnName.replace("_","")
	outputJson["column_name"] = file.split(".")[1]
	outputJson["semantic_types"] = []
	types = {}
	#person name
	#In order to identify each value, we firstly collect each value in the column, and loop through it.
	#In order to minimize the number of types used to label each value, we decided to use column name to do the first filter.
	for values in columnNameDF:
		if len(values) < 2:
			values.append(1)
		if values is None or isNotValid(values[0]):
			try:
				types["other"] += int(values[1])
			except:
				types["other"] = int(values[1])
			continue
		if "first" in columnName or 'last' in columnName or 'mi' == columnName or 'candmi' == columnName:
			if columnName.replace(" ", "").replace(".", "").isalpha():
				try:
					types["person_name"] += int(values[1])
				except:
					types["person_name"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])

		elif "businessname" in replacedColumnName or "dba" in replacedColumnName:
			if(is_dba(values[0])):
				try:
					types["business_name"] += int(values[1])
				except:
					types["business_name"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "phone" in columnName:
			if(is_phone(values[0].strip())):
				try:
					types["phone_number"] += int(values[1])
				except:
					types["phone_number"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "zip" in columnName:
			if(is_zip(values[0].strip())):
				try:
					types["zip_code"] += int(values[1])
				except:
					types["zip_code"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "agency" in replacedColumnName:
			if(is_agency(values[0].strip())):
				try:
					types["city_agency"] += int(values[1])
				except:
					types["city_agency"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "city" in replacedColumnName or "neighborhood" in replacedColumnName or "boro" in replacedColumnName:
			if(is_city(values[0].strip())):
				try:
					types["city"] += int(values[1])
				except:
					types["city"] = int(values[1])
			elif(is_neighborhood(values[0].strip())):
				try:
					types["neighborhood"] += int(values[1])
				except:
					types["neighborhood"] = int(values[1])
			elif(is_borough(values[0].strip())):
				try:
					types["borough"] += int(values[1])
				except:
					types["borough"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "street" in replacedColumnName or "address" in replacedColumnName:
			if(is_address(values[0].strip())):
				try:
					types["address"] += int(values[1])
				except:
					types["address"] = int(values[1])
			elif(is_streetName(values[0].strip())):
				try:
					types["street_name"] += int(values[1])
				except:
					types["street_name"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "latlong" in replacedColumnName or "location" in replacedColumnName:
			if(is_latLong(values[0].strip())):
				try:
					types["lat_lon_cord"] += int(values[1])
				except:
					types["lat_lon_cord"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "schoollevel" in replacedColumnName:
			if(is_schoolLevel(values[0].strip())):
				try:
					types["school_level"] += int(values[1])
				except:
					types["school_level"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "school" in replacedColumnName:
			if(is_schoolName(values[0].strip())):
				try:
					types["school_name"] += int(values[1])
				except:
					types["school_name"] = int(values[1])
			elif(is_college(values[0].strip())):
				try:
					types["college_name"] += int(values[1])
				except:
					types["college_name"] = int(values[1])
			elif(is_schoolLevel(values[0].strip())):
				try:
					types["school_level"] += int(values[1])
				except:
					types["school_level"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "color" in replacedColumnName:
			if(is_color(values[0].strip())):
				try:
					types["color"] += int(values[1])
				except:
					types["color"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])

		elif "vehicle" in replacedColumnName or "make" in replacedColumnName:
			if(is_carMake(values[0].strip())):
				try:
					types["car_make"] += int(values[1])
				except:
					types["car_make"] = int(values[1])
			elif(is_vehicleType(values[0].strip())):
				try:
					types["vehicle_type"] += int(values[1])
				except:
					types["vehicle_type"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "website" in replacedColumnName:
			if(is_url(values[0].strip())):
				try:
					types["website"] += int(values[1])
				except:
					types["website"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "interest" in replacedColumnName or "core" in replacedColumnName:
			if(is_areaOfStudy(values[0].strip())):
				try:
					types["area_of_study"] += int(values[1])
				except:
					types["area_of_study"] = int(values[1])
			elif(is_subjectsInSchool(values[0].strip())):
				try:
					types["subject_in_school"] += int(values[1])
				except:
					types["subject_in_school"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "building" in replacedColumnName :
			if(is_buildingClassification(values[0].strip())):
				try:
					types["building_classification"] += int(values[1])
				except:
					types["building_classification"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "typdesc" in replacedColumnName :
			if(is_typeOfLocation(values[0].strip())):
				try:
					types["location_type"] += int(values[1])
				except:
					types["location_type"] = int(values[1])
			else:
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		elif "park" in columnName or "playground" in columnName:
			if(is_parkPlayground(values[0].strip())):
				try:
					types["park_playground"] += int(values[1])
				except:
					types["park_playground"] = int(values[1])
			elif(is_schoolName(values[0].strip())):
				print(values[0].strip())
				try:
					types["school_name"] += int(values[1])
				except:
					types["school_name"] = int(values[1])
			else:
				print(values[0].strip()+" ---Other")
				try:
					types["other"] += int(values[1])
				except:
					types["other"] = int(values[1])
		else:
			try:
				types["other"] += int(values[1])
			except:
				types["other"] = int(values[1])

	labelsOfColumn = fileLabelDic[file]
	for key in types:
		semantic_type = {}
		semantic_type["semantic_type"] = key
		semantic_type["count"] = types[key]
		outputJson["semantic_types"].append(semantic_type)		
		if key in labelsOfColumn:
			correcylyPredictDic[key] += 1
		predictedTypeStatic[key] += 1

	fileDF.unpersist()
	jsonFileName = file.replace(".txt.gz","")
	with open(jsonFileName + ".json", "w") as f:
		json.dump(outputJson,f)

print("Lable Static:\n")
print(json.dumps(labeledFileStatics, indent = 4))
print("Predict Static:\n")
print(json.dumps(predictedTypeStatic, indent = 4))
print("Correct predict Static:\n")
print(json.dumps(correcylyPredictDic, indent = 4))
print("%s\t%s\t%s\n" % ("type", "precision", "recall"))
for i in correcylyPredictDic:
	#precision = number of columns correctly predicted as type / all columns predicted as type 
	#recall = number of columns correctly predicted as type / number of actual columns of type
	precision = 0.0
	recall = 0.0
	if predictedTypeStatic[i] == 0:
		precision = 0.0
	else:
		precision = float(correcylyPredictDic[i])/predictedTypeStatic[i]
	if labeledFileStatics[i] == 0:
		recall = 0.0
	else:
		recall = float(correcylyPredictDic[i])/labeledFileStatics[i]

	print("%s\t%.5f\t%.5f\n" % (i, precision, recall))

