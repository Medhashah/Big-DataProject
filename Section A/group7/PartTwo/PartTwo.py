import ast
from difflib import SequenceMatcher
import sys
import numpy as np
import json
import re
import csv
import pandas as pd
from pyspark import SparkContext
#from pyspark.sql.functions import levenshtein
from sklearn.metrics.pairwise import cosine_distances
from sklearn.metrics.pairwise import cosine_similarity
from nltk.corpus import stopwords
stopwords = stopwords.words('english')

# change get_semantic_type function to add more semantic types
"""
type we need verified:
[person_name, business_name, phone_number, address, street_name, city, 
neighborhood, lat_lon_cord, zip_code, borough, school_name, 
color, car_make, city_agency, area_of_study, subject_in_school, 
school_level, college_name, website, building_classification, vehicle_type, 
location_type, park_playground, other]


we have done yet:
zip_code, phone_number, address, street_name, neighborhood, lat_lon_cord, borough, 
city_agency, area_of_study, subject_in_school, school_level, website, building_classification,


we should do:
the way we deal with name: just read the column name, if contain the last name, first name or other
person_name, business_name, school_name, college_name,
go through each data, and find the empty label to other


tough: city, color, car_make, park_playground,


actudally we don't have column with location_type, 


"""
subject = ['-', 'english', 'math', 'science', 'social studies', 'algebra',
           'assumed team teaching', 'chemistry', 'earth science', 'economics',
           'english 10', 'english 11', 'english 12', 'english 9', 'geometry',
           'global history 10', 'global history 9', 'living environment',
           'matched sections', 'math a', 'math b', 'other', 'physics', 'us government',
           'us government & economics', 'us history']
color = ['sliver', 'white', 'black', 'red', 'grey', 'blue', 'brown', 'ivory', 'beige',
         'gold', 'pink', 'rose', 'orange', 'yellow', 'ivory',
         'green', 'purple', 'bronze', 'camel',
         'bk', 'wh', 'rd', 'jade', 'blk', 'tan', 'tornado red', 'silk'
         'navy', 'ltgy', 'ltg', 'light grey', 'light brown', 'dark red']
street_name = ['avenue', 'place', 'street', 'st', 'court']
park_playground = ['park', 'playground', 'recreation center']
college_name = ['college', 'institution', 'university', 'institute']
school_name = ['academy', 'school', 'high school', 'closed school',
               'campus', 'language', 'learning']
school_level = ['elementary', 'high school', 'high school transfer', 'k-2',
                'k-3', 'k-8', 'middle', 'yabc', 'elementary school', 'k-8 school',
                'middle school', 'transfer school', 'transfer high school', 'd75']
neighborhood_name = ['arrochar-shore acres', 'dongan hills', 'grant city', 'grymes hill',
                     'new springville', 'rosebank', 'silver lake', 'sunnyside', 'tompkinsville',
                     'bedford park/norwood', 'belmont', 'bronxdale', 'city island', 'east tremont',
                     'highbridge/morris heights', 'kingsbridge/jerome park', 'morris park/van nest',
                     'morrisania/longwood', 'mott haven/port morris', 'parkchester',
                     'pelham parkway south', 'riverdale', 'schuylerville/pelham bay', 'soundview',
                     'throgs neck', 'williamsbridge', 'airport la guardia', 'astoria', 'bayside',
                     'briarwood', 'college point', 'corona', 'elmhurst', 'far rockaway',
                     'flushing meadow park', 'flushing-north', 'flushing-south', 'forest hills',
                     'glendale', 'hammels', 'howard beach', 'jackson heights', 'jamaica estates',
                     'kew gardens', 'little neck', 'long island city', 'maspeth', 'middle village',
                     'oakland gardens', 'ozone park', 'rego park', 'ridgewood', 'rockaway park',
                     'south ozone park', 'whitestone', 'woodhaven', 'woodside', 'bath beach',
                     'bay ridge', 'bedford stuyvesant', 'bensonhurst', 'boerum hill', 'borough park',
                     'brighton beach', 'brooklyn heights', 'bushwick', 'canarsie', 'carroll gardens',
                     'clinton hill', 'cobble hill', 'cobble hill-west', 'crown heights',
                     'downtown-fulton ferry', 'downtown-fulton mall', 'downtown-metrotech',
                     'dyker heights', 'east new york', 'flatbush-central', 'flatbush-east',
                     'flatbush-lefferts garden', 'flatbush-north', 'fort greene', 'gowanus',
                     'gravesend', 'greenpoint', 'kensington', 'madison', 'midwood', 'mill basin',
                     'ocean hill', 'ocean parkway-north', 'park slope', 'park slope south',
                     'prospect heights', 'sheepshead bay', 'sunset park', 'williamsburg-central',
                     'williamsburg-east', 'williamsburg-north', 'williamsburg-south',
                     'windsor terrace', 'wyckoff heights', 'hillcrest', 'jamaica']

interest = ['animal science', 'architecture', 'business', 'communications',
            'computer science & technology', 'cosmetology', 'culinary arts',
            'engineering', 'environmental science', 'film/video', 'health professions',
            'hospitality, travel and tourism', 'humanities & interdisciplinary',
            'jrotc', 'law & government', 'performing arts',
            'performing arts/visual art & design', 'science & math', 'teaching',
            'visual art & design', 'zoned']

borough = ['brooklyn', 'manhattan', 'bronx',
           'staten island', 'queens', 'k', 'r', 'm', 'x', 'q']
agency = ['311', 'acs', 'bic', 'boe', 'bpl', 'cchr', 'ccrb', 'cuny', 'dca',
          'dcas', 'dcla', 'dcp', 'ddc', 'dep', 'dfta', 'dhs', 'dob', 'doc',
          'doe', 'dof', 'dohmh', 'doi', 'doitt', 'dop', 'doris', 'dot', 'dpr',
          'dsny', 'dvs', 'dycd', 'fdny', 'hpd', 'hra', 'law', 'lpc', 'nycem',
          'nycha', 'nychh', 'nypd', 'nypl', 'oath', 'qpl', 'sbs', 'sca', 'tlc',
          'edc', 'nypl-research', 'ocme']
agency_name = ['admin. for children services', 'board of correction', 'board of elections',
               'brooklyn public library', 'business integrity commission', 'campaign finance board',
               'city clerk', 'city council', 'city university', 'citywide pension contributions',
               'citywide savings initiatives', 'civil service commission', 'civilian complaint review bd.',
               'commission on human rights', 'community boards (all)', 'conflicts of interest board', 'd.o.i.t.t.',
               'debt service', 'department for the aging', 'department of buildings', 'department of city planning',
               'department of consumer affairs', 'department of correction', 'department of cultural affairs',
               'department of education', 'department of finance', 'department of investigation',
               'department of probation', 'department of sanitation', 'department of social services',
               'department of transportation', 'dept health & mental hygiene', 'dept of citywide admin srvces',
               'dept of environmental prot.', 'dept of parks and recreation', 'dept of records & info serv.',
               'dept. of design & construction', 'dept. of emergency management', 'dept. of homeless services',
               "dept. of veterans' services", 'dept. small business services', 'district attorney - bronx',
               'district attorney - kings', 'district attorney - n.y.', 'district attorney - queens',
               'district attorney - richmond', 'energy adjustment', 'equal employment practices com',
               'financial info. serv. agency', 'fire department', 'general reserve', 'health and hospitals corp.',
               'housing preservation & dev.', 'independent budget office', 'landmarks preservation comm.',
               'law department', 'lease adjustment', 'mayoralty', 'miscellaneous', 'new york public library',
               'ny public library - research', 'off. of prosec. & spec. narc.', 'office admin trials & hearings',
               'office of admin. tax appeals', 'office of collective barg.', 'office of payroll admin.',
               'office of the actuary', 'office of the comptroller', 'otps inflation adjustment', 'police department',
               'president,borough of brooklyn', 'president,borough of manhattan', 'president,borough of queens',
               'president,borough of s.i.', 'president,borough of the bronx', 'public administrator - bronx',
               'public administrator - n.y.', 'public administrator - queens', 'public administrator -richmond',
               'public administrator- brooklyn', 'public advocate', 'queens borough public library',
               'taxi & limousine commission', 'youth & community development']

vehicel_type = ['2 dr sedan', 'ambulance', 'bicycle', 'bike', 'box truck', 'bu', 'bus', 'carry all', 'convertible',
                'dp', 'ds', 'fb', 'fire truck', 'garbage or refuse', 'gg', 'large com veh(6 or more tires)',
                'livery vehicle', 'll', 'motorcycle', 'ms', 'other', 'passenger vehicle', 'pedicab', 'pick-up truck',
                'scooter', 'sedan', 'small com veh(4 tires)', 'sport utility / station wagon',
                'station wagon/sport utility vehicle', 'taxi', 'tk', 'tow truck / wrecker', 'tract', 'tractor truck diesel',
                'trail', 'trl', 'unknown', 'van', 'wagon']

car_make = ['acura', 'alfa romeo', 'am general', 'aston martin', 'audi', 'avanti motors', 'bentley', 'bmw', 'bugatti',
            'buick', 'cadillac', 'chevrolet', 'chrysler', 'daewoo', 'daihatsu', 'dodge', 'eagle', 'ferrari', 'fiat',
            'fisker', 'ford', 'genesis', 'geo', 'gmc', 'honda', 'hummer', 'hyundai', 'infiniti', 'international', 'isuzu',
            'jaguar', 'jeep', 'karma', 'kia', 'koenigsegg', 'lamborghini', 'land rover', 'lexus', 'lincoin', 'lotus',
            'maserati', 'maybach', 'mazda', 'mclaren', 'mercedes-benz', 'mercury', 'mini', 'mitsubishi', 'morgan', 'nissan',
            'oldsmobile', 'panoz', 'peugeot', 'plymouth', 'pontiac', 'porsche', 'qvale', 'ram', 'rolls-royce', 'saab', 'saleen',
            'saturn', 'scion', 'smart', 'spyker', 'sterling', 'subaru', 'suzuki', 'tesla', 'toyota', 'volkswagen', 'volvo', 'yugo',
            'benz', 'mercedes', 'lambo', 'avanti', 'chevy', 'vw']

location_type = ['abandoned building', 'airport terminal', 'atm', 'bank', 'bar/night club',
                 'beauty & nail salon', 'book/card', 'bridge', 'bus (nyc transit)',
                 'bus (other)', 'bus stop', 'bus terminal', 'candy store', 'cemetery',
                 'chain store', 'check cashing business', 'church', 'clothing/boutique',
                 'commercial building', 'construction site', 'department store', 'doctor/dentist office',
                 'drug store', 'dry cleaner/laundry', 'factory/warehouse', 'fast food', 'ferry/ferry terminal',
                 'food supermarket', 'gas station', 'grocery/bodega', 'gym/fitness facility', 'highway/parkway',
                 'hospital', 'hotel/motel', 'jewelry', 'liquor store', 'loan company', 'mailbox inside',
                 'mailbox outside', 'marina/pier', 'mosque', 'open areas (open lots)', 'other',
                 'other house of worship', 'park/playground', 'parking lot/garage (private)',
                 'parking lot/garage (public)', 'photo/copy', 'private/parochial school', 'public building',
                 'public school', 'residence - apt. house', 'residence - public housing', 'residence-house',
                 'restaurant/diner', 'shoe', 'small merchant', 'social club/policy', 'storage facility',
                 'store unclassified', 'street', 'synagogue', 'taxi (livery licensed)',
                 'taxi (yellow licensed)', 'taxi/livery (unlicensed)', 'telecomm. store', 'tramway',
                 'transit - nyc subway', 'transit facility (other)', 'tunnel', 'variety store', 'video store']



with open("city.txt", "r") as data:
    type_dict = ast.literal_eval(data.read())
for item in subject:
    type_dict[item] = "subject_in_school"
for item in school_level:
    type_dict[item] = "school_level"
for item in neighborhood_name:
    type_dict[item] = "neighborhood"
for item in interest:
    type_dict[item] = "area_of_study"
for item in borough:
    type_dict[item] = "borough"
for item in agency:
    type_dict[item] = "city_agency"
for item in agency_name:
    type_dict[item] = "city_agency"
for item in vehicel_type:
    type_dict[item] = "vehicel_type"
for item in car_make:
    type_dict[item] = "car_make"
for item in location_type:
    type_dict[item] = "location_type"
for item in color:
    type_dict[item] = "color"



def levenshtein(seq1, seq2):
    size_x = len(seq1) + 1
    size_y = len(seq2) + 1
    matrix = np.zeros((size_x, size_y))
    for x in range(size_x):
        matrix[x, 0] = x
    for y in range(size_y):
        matrix[0, y] = y
    for x in range(1, size_x):
        for y in range(1, size_y):
            if seq1[x-1] == seq2[y-1]:
                matrix[x, y] = min(
                    matrix[x-1, y] + 1,
                    matrix[x-1, y-1],
                    matrix[x, y-1] + 1
                )
            else:
                matrix[x, y] = min(
                    matrix[x-1, y] + 1,
                    matrix[x-1, y-1] + 1,
                    matrix[x, y-1] + 1
                )
    return (matrix[size_x - 1, size_y - 1])


def cosine_sim_vectors(vec1, vec2):
    vec1 = vec1.reshape(1, -1)
    vec2 = vec2.reshape(1, -1)
    return cosine_similarity(vec1, vec2)[0][0]


def get_semantic_type(line):
    # checking with regular expression first, which is the most efficiency
    if re.match("http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", line) is not None:
        return "website"
    elif re.match("(\+\d{1,2}\s?)?1?\-?\.?\s?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}", line) is not None:
        return "phone_number"
    elif re.match("\(\d+.?\d*, -?\d+.?\d*\)", line) is not None:
        return "lat_lon_cord"
    elif re.match("[a-z]\d[-(0-9a-z)]+", line) is not None:
        return "building_classification"
    elif re.match("(?!0{5})(\d{5})(?!-?0{4})(|-\d{4})?", line) is not None:
        return "zip_code"
    # checking with dictionary
    elif type_dict.get(line) is not None:
        return type_dict.get(line)
    # checking with the list, from the smallest one

    else:
        for data in park_playground:
            line_split = line.split(" ")
            if data in line_split:
                return "park_playground"

        for data in college_name:
            line_split = line.split(" ")
            if data in line_split:
                return "college_name"

        for data in school_name:
            line_split = line.split(" ")
            if data in line_split:
                return "school_name"

        for data in street_name:
            line_split = line.split(" ")
            if data in line_split:
                return "street_name"
    return "other"


def semanticCheck(sc, file_name, true_type):
    file_name = file_name.strip()[1:-1]
    file_path = "/user/hm74/NYCColumns/" + str(file_name)
    column_name = file_name.split('.')[1]
    data = sc.textFile(file_path, 1).mapPartitions(
        lambda x: csv.reader(x, delimiter='\t', quotechar='"'))

    all_info = []
    
    semantic_type = data.map(lambda x: (get_semantic_type(
        x[0].lower()), int(1)) if len(x)==1 else (get_semantic_type(
        x[0].lower()),int(x[1]))).reduceByKey(lambda x, y: x+y)
    # [type, count]
    if true_type in ["street_name", "business_name", "person_name"]:
        semantic_type = semantic_type.map(lambda x: (
            true_type, x[1]) if x[0] == "other" else x)
    if true_type == "address":
        semantic_type = semantic_type.map(lambda x: (
            true_type, x[1]) if x[0] == "street_name" else x)
    for row in semantic_type.collect():
        # key is semantic type, value is count
        semantic_information = {}
        semantic_information["semantic_type"] = row[0]
        if (row[0] == "other"):
            semantic_information["label"] = true_type
        semantic_information["count"] = row[1]
        all_info.append(semantic_information)

    all_info.sort(key=lambda x: -x["count"])
    prediction_type = all_info[0]["semantic_type"] if all_info[0]["semantic_type"] != "other" or len(
        all_info) == 1 else all_info[1]["semantic_type"]
    with open(file_name+'_semantic_result.json', 'w') as fp:
        json.dump({"column name":str(column_name),"semantic_types": all_info, "true_type": true_type,
                   "prediction_type": prediction_type}, fp)


# match with the csv get the true type
def read_in_true_label(name, df):
    name = name.replace("'", '')
    for row in df:
        if row[0].replace("'", '') == name:
            return row[1]


sc = SparkContext()

file_list = open('cluster3.txt').readline().strip().replace(' ', '').split(",")

# get the true type csv
with open("true_type.csv", 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    column = [[row['file'], row['true_type']] for row in reader]

for item in file_list:
    true_type = read_in_true_label(item, column)
    semanticCheck(sc, item, true_type)
