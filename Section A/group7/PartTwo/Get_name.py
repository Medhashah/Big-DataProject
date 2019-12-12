import json
import re
import csv
from pyspark import SparkContext

# school type
sc = SparkContext()
file_name = "yahh-6yjc.School_Type.txt.gz"
file_path = "/user/hm74/NYCColumns/" + file_name
school_type = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
school_type = school_type.map(lambda x: x[0]).collect()
print("school type:")
print(school_type)

file_name = ["4n2j-ut8i.SCHOOL_LEVEL_.txt.gz","weg5-33pj.SCHOOL_LEVEL_.txt.gz","upwt-zvh3.SCHOOL_LEVEL_.txt.gz","cvh6-nmyi.SCHOOL_LEVEL_.txt.gz", "6je4-4x7e.SCHOOL_LEVEL_.txt.gz"]
result = []
for name in file_name:
    file_path = "/user/hm74/NYCColumns/" + name
    school_level = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    school_level = school_level.map(lambda x: x[0]).collect()
    for level in school_level:
        if level not in result:
            result.append(level)
print("school level:")
print(result)
#school level = ['ELEMENTARY', 'HIGH SCHOOL', 'HIGH SCHOOL TRANSFER',
# 'K-2', 'K-3', 'K-8', 'MIDDLE', 'YABC', 'ELEMENTARY SCHOOL',
# 'K-8 SCHOOL', 'MIDDLE SCHOOL', 'TRANSFER SCHOOL', 'TRANSFER HIGH SCHOOL', 'D75']
school_type_result = ['elementary', 'high school', 'high school transfer', 'k-2', 'k-3', 'k-8', 'middle', 'yabc', 'elementary school', 'k-8 school', 'middle school', 'transfer school', 'transfer high school', 'd75']




# neighborhood
file_name = ["wv4q-e75v.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz",
             "a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz",
             "bawj-6bgn.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz",
             "m59i-mqex.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz",
             "5mw2-hzqx.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz",
             "crbs-vur7.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz"]
result = []
for name in file_name:
    file_path = "/user/hm74/NYCColumns/" + name
    neighbor = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    neighbor = neighbor.map(lambda x: x[0]).collect()
    for neigh_name in neighbor:
        if neigh_name not in result:
            result.append(neigh_name)
print("neighborhood name:")
print(result)
neigh_result = ['arrochar-shore acres', 'dongan hills', 'grant city', 'grymes hill',
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


# area of insterests

file_name = ["i9pf-sj7c.INTEREST.txt.gz", "vw9i-7mzq.interest4.txt.gz",
             "ge8j-uqbf.interest.txt.gz"]
result = []
for name in file_name:
    file_path = "/user/hm74/NYCColumns/" + name
    interest = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    interest = interest.map(lambda x: x[0]).collect()
    for area in interest:
        if area not in result:
            result.append(area)
print(" area of interest and study")
print(result)
interest = ['animal science', 'architecture', 'business', 'communications',
            'computer science & technology', 'cosmetology', 'culinary arts',
            'engineering', 'environmental science', 'film/video', 'health professions',
            'hospitality, travel and tourism', 'humanities & interdisciplinary',
            'jrotc', 'law & government', 'performing arts',
            'performing arts/visual art & design', 'science & math', 'teaching',
            'visual art & design', 'zoned']

# agency code
file_name = ["sa5w-dn2t.Agency.txt.gz", "7jkp-5w5g.Agency.txt.gz",
             "i5ef-jxv3.Agency.txt.gz"]
result = []
for name in file_name:
    file_path = "/user/hm74/NYCColumns/" + name
    agency = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    agency = agency.map(lambda x: x[0]).collect()
    for id in agency:
        if id not in result:
            result.append(id)
print(" agency id")
print(result)

agency = ['311', 'acs', 'bic', 'boe', 'bpl', 'cchr', 'ccrb', 'cuny', 'dca',
          'dcas', 'dcla', 'dcp', 'ddc', 'dep', 'dfta', 'dhs', 'dob', 'doc',
          'doe', 'dof', 'dohmh', 'doi', 'doitt', 'dop', 'doris', 'dot', 'dpr',
          'dsny', 'dvs', 'dycd', 'fdny', 'hpd', 'hra', 'law', 'lpc', 'nycem',
          'nycha', 'nychh', 'nypd', 'nypl', 'oath', 'qpl', 'sbs', 'sca', 'tlc',
          'edc', 'nypl-research', 'ocme']


result = []
file_name = "sqmu-2ixd.Agency_Name.txt.gz"
file_path = "/user/hm74/NYCColumns/" + file_name
agency_name = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
agency_name = agency_name.map(lambda x: x[0]).collect()
for name in agency_name:
    if name not in result:
        result.append(name)
print("agency name are :")
print(result)

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
# vehicle type
result = []
file_name = ["h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz", "h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz",
             "h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz", "h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz",
             "h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz"]

file_name = ["h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz"]
for name in file_name:
    file_path = "/user/hm74/NYCColumns/" + name
    vehicel_type = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    vehicel_type = vehicel_type.map(lambda x: x[0]).collect()
    for name in vehicel_type:
        if name not in result:
            result.append(name)
print("vehicel_type name are :")
print(result)
vehicel_type = ['2 dr sedan', 'ambulance', 'bicycle', 'bike', 'box truck', 'bu', 'bus', 'carry all', 'convertible',
                'dp', 'ds', 'fb', 'fire truck', 'garbage or refuse', 'gg', 'large com veh(6 or more tires)',
                'livery vehicle', 'll', 'motorcycle', 'ms', 'other', 'passenger vehicle', 'pedicab', 'pick-up truck',
                'scooter', 'sedan', 'small com veh(4 tires)', 'sport utility / station wagon',
                'station wagon/sport utility vehicle', 'taxi', 'tk', 'tow truck / wrecker', 'tract', 'tractor truck diesel',
                'trail', 'trl', 'unknown', 'van', 'wagon']



# park facility

file_name = ["erm2-nwe9.Park_Facility_Name.txt.gz"]


# core subject
file_name = ["ub9e-s7ai.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz",
             "sybh-s59s.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz",
             "6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz",
             "d3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz"]
result = []
for name in file_name:
    file_path = "/user/hm74/NYCColumns/" + name
    subject = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    subject = subject.map(lambda x: x[0]).collect()
    for name in subject:
        if name not in result:
            result.append(name)
print("subject name are :")
print(result)

subject = ['-', 'english', 'math', 'science', 'social studies', 'algebra',
           'assumed team teaching', 'chemistry', 'earth science', 'economics',
           'english 10', 'english 11', 'english 12', 'english 9', 'geometry',
           'global history 10', 'global history 9', 'living environment',
           'matched sections', 'math a', 'math b', 'other', 'physics', 'us government',
           'us government & economics', 'us history']
