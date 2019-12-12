import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from operator import add
import os
import re

import datetime

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.7'
import json

import spacy
import en_core_web_sm
from geotext import GeoText

nlp = en_core_web_sm.load()

sc = SparkContext()

spark = SparkSession \
    .builder \
    .appName("projectTask1") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)


def getList(path):
    list = []
    with open(path, "r") as f:
        data = f.readlines()
        for line in data:
            word = line.strip().lower()
            list.append(word)
    return list


neighborhood_set = set(getList("exampleForIdentify/neighbourhood_dict.txt"))
borough_set = set(getList("exampleForIdentify/borough_dict.txt"))
# 后缀判断
businessName_set = {"restaurant", "cafe", "house", "market", "shop", "bar", "company", "food", "deli", "kitchen"}
street_set = {"st", "street", "av" "ave", "avenue", "court", "pl", "place", "rd", "road", "dr", "drive", "lane",
              "blvd", "way"}
school_set = {"school", "university"}
cityAgency_set = {"acs",
                  "administration for children's services",
                  "bic",
                  "business integrity commission",
                  "bnydc",
                  "brooklyn navy yard development corp",
                  "boc",
                  "board of correction",
                  "bpl",
                  "brooklyn public library",
                  "bsa",
                  "board of standards and appeals",
                  "bxda",
                  "bronx district attorney",
                  "ccrb",
                  "civilian complaint review board",
                  "cfb",
                  "campaign finance board",
                  "coib",
                  "conflicts of interest board",
                  "cpc",
                  "city planning commission",
                  "cuny",
                  "city university of new york",
                  "dany",
                  "new york county district attorney",
                  "dca",
                  "department of consumer affairs",
                  "dcas",
                  "department of citywide administrative services",
                  "dcla",
                  "department of cultural affairs",
                  "dcp",
                  "department of city planning",
                  "ddc",
                  "department of design and construction",
                  "dep",
                  "department of environmental protection",
                  "dfta",
                  "department for the aging",
                  "dhs",
                  "department of homeless services",
                  "dob",
                  "department of buildings",
                  "doc",
                  "department of correction",
                  "dof",
                  "department of finance",
                  "dohmh",
                  "department of health and mental hygiene",
                  "doi",
                  "department of investigation",
                  "doitt",
                  "department of information technology and telecommunications",
                  "dop",
                  "department of probation",
                  "doris",
                  "department of records and information services",
                  "dot",
                  "department of transportation",
                  "dpr",
                  "department of parks and recreation",
                  "dsny",
                  "department of sanitation",
                  "dycd",
                  "department of youth and community development",
                  "ecb",
                  "environmental control board",
                  "edc",
                  "economic development corporation",
                  "eepc",
                  "equal employment practices commission",
                  "fcrc",
                  "franchise and concession review committee",
                  "fdny",
                  "fire department of new york",
                  "fisa",
                  "financial information services agency",
                  "hhc",
                  "heal and hospitals corporation",
                  "hpd",
                  "department of housing preservation and development",
                  "hra",
                  "human resources administration",
                  "ibo",
                  "independent budget office",
                  "kcda",
                  "kings county district attorney",
                  "lpc",
                  "landmarks preservation commission",
                  "mocj",
                  "mayor’s office of criminal justice",
                  "mocs",
                  "mayor's office of contract services",
                  "nycdoe",
                  "department of education",
                  "nycem",
                  "emergency management",
                  "nycers",
                  "new york city employees retirement system",
                  "nycha",
                  "new york city housing authority",
                  "nycld",
                  "law department",
                  "nyctat",
                  "new york city tax appeals tribunal",
                  "nypd",
                  "new york city police department",
                  "nypl",
                  "new york public library",
                  "oath",
                  "office of administrative trials and hearings",
                  "ocme",
                  "office of chief medical examiner",
                  "omb",
                  "office of management & budget",
                  "osnp",
                  "office of the special narcotics prosecutor",
                  "ppb",
                  "procurement policy board",
                  "qcda",
                  "queens district attorney",
                  "qpl",
                  "queens borough public library",
                  "rcda",
                  "richmond county district attorney",
                  "rgb",
                  "rent guidelines board",
                  "sbs",
                  "small business services",
                  "sca",
                  "school construction authority",
                  "tbta",
                  "triborough bridge and tunnel authority",
                  "tlc",
                  "taxi and limousine commission",
                  "trs",
                  "teachers' retirement system"}
schoolLevel_set = {"elementary", "middle", "high", "middle school", "high school", "elementary school"}
park_set = {"park", "playgroud"}

color_set = {'aliceblue', 'antiquewhite', 'aqua', 'aquamarine', 'azure', 'beige', 'bisque', 'black',
             'blanchedalmond', 'blue',
             'blueviolet', 'brown', 'burlywood', 'cadetblue', 'chartreuse', 'chocolate', 'coral', 'cornflowerblue',
             'cornsilk',
             'crimson', 'cyan', 'darkblue', 'darkcyan', 'darkgoldenrod', 'darkgray', 'darkgreen', 'darkkhaki',
             'darkmagenta',
             'darkolivegreen', 'darkorange', 'darkorchid', 'darkred', 'darksalmon', 'darkseagreen',
             'darkslateblue', 'darkslategray', 'darkturquoise', 'darkviolet', 'deeppink', 'deepskyblue', 'dimgray',
             'dodgerblue', 'firebrick', 'floralwhite', 'forestgreen', 'fuchsia', 'gainsboro', 'ghostwhite', 'gold',
             'goldenrod', 'gray', 'green', 'greenyellow', 'honeydew', 'hotpink', 'indianred', 'indigo', 'ivory',
             'khaki', 'lavender',
             'lavenderblush', 'lawngreen', 'lemonchiffon', 'lightblue',
             'lightcoral', 'lightcyan', 'lightgoldenrodyellow', 'lightgreen',
             'lightgray', 'lightpink', 'lightsalmon', 'lightseagreen',
             'lightskyblue', 'lightslategray', 'lightsteelblue', 'lightyellow',
             'lime', 'limegreen', 'linen', 'magenta', 'maroon', 'mediumaquamarine',
             'mediumblue', 'mediumorchid', 'mediumpurple', 'mediumseagreen', 'mediumslateblue',
             'mediumspringgreen',
             'mediumturquoise', 'mediumvioletred', 'midnightblue', 'mintcream', 'mistyrose', 'moccasin',
             'navajowhite', 'navy', 'oldlace', 'olive', 'olivedrab', 'orange', 'orangered',
             'orchid', 'palegoldenrod', 'palegreen', 'paleturquoise', 'palevioletred', 'papayawhip',
             'peachpuff', 'peru', 'pink', 'plum', 'powderblue', 'purple', 'red', 'rosybrown', 'royalblue',
             'saddlebrown', 'salmon',
             'sandybrown', 'seagreen', 'seashell', 'sienna', 'silver', 'skyblue', 'slateblue', 'slategray', 'snow',
             'springgreen', 'steelblue', 'tan', 'teal', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat',
             'white', 'whitesmoke', 'yellow', 'yellowgreen',
             'bg', 'bge', 'bk', 'bkgr', 'bkgy', 'bl', 'bla', 'blg', 'blgy', 'blk', 'blu', 'blw', 'bn',
             'br', 'brgy', 'brn', 'bro', 'brw', 'brwn', 'burg', 'bw', 'bwn', 'dbl', 'dkb', 'dkbl',
             'dkg', 'dkgr', 'dkgy', 'dkrd', 'gd', 'gl', 'gld', 'gn', 'gr', 'grn', 'gry', 'gy', 'gybl',
             'gygr', 'gygy', 'laven', 'lt', 'ltbl', 'ltg', 'ltgy', 'ltgr', 'maroo', 'mr', 'noc',
             'nocl', 'or', 'orang', 'other', 'pr', 'purpl', 'rd', 'rdgr', 'rdgy', 'rdw', 'sil', 'silve',
             'sl', 'tn', 'tngy', 'tngr', 'wh', 'whbl', 'whgy', 'whi', 'wt', 'yello', 'yw'}
study_set = {
    'humanities', 'history', 'linguistics', 'languages', 'philosophy', 'economics', 'geography', 'teaching',
    'political science', 'psychology', 'biology', 'chemistry', 'arts', 'agriculture', 'architecture',
    'law & government',
    'science & math', 'tourism', 'film', 'physics', 'zoned', 'astronomy', 'computer science', 'business',
    'engineering', 'medicine',
    'public administration', 'transportation', 'environmental science', 'animal science', 'communications',
    'health professions'}
city_set = {'bronx', 'brooklyn', 'manhattan', 'queens', 'staten island', 'nyc'}
vehicle_set = {'conv', 'h/tr', 'lim', 'bus', 'taxi', 't/cr', 'ltrl', 'mfh', 'dump', 'delv', 'semi', 'util', 'sedn',
               '2dsd', 'trac', '4dsd', 'snow', 'pick', 'rbm', 'tank', 'dcom', 'swt', 's/sp', 'trlr',
               'h/in', 'r/rd', 'stak', 'rd/s', 'flat', 'sn/p', 'mopd', 'fire', 'van', 'trav', 'w/sr', 'pole',
               'h/wh', 'fpm', 'mcc', 'atv', 'tr/e', 'subn', 'tr/c', 'p/sh', 'w/dr', 'boat', 'lsvt', 'rplc', 'loco',
               'cmix', 'cust', 'tow', 'hrse', 'refg', 'mcy', 'emvr', 'ambu', 'lsv'}

subject_set = {"english", "math", "science", "social studies", "art",
               "citizenship", "geography", "history", "language", "literacy", "music",
               "arithmetic", "design", "mathematics", "pe", "pysical education",
               "biology", "computer science", "sociology", "psychology", "economics"}
location_set = {"abandoned building", "building", "airport terminal", "bank", "church", "clothing shop",
                "boutique", "restuaurant", "hotel", "park", "museum", "gallery", "gam"}
car_set = {
    "abarth",
    "alfa romeo",
    "aston martin",
    "audi",
    "bentley",
    "bmw",
    "bugatti",
    "cadillac",
    "chevrolet",
    "chrysler",
    "citroën",
    "dacia",
    "daewoo",
    "daihatsu",
    "dodge",
    "donkervoort",
    "ds",
    "ferrari",
    "fiat",
    "fisker",
    "ford",
    "honda",
    "hummer",
    "hyundai",
    "infiniti",
    "iveco",
    "jaguar",
    "jeep",
    "kia",
    "ktm",
    "lada",
    "lamborghini",
    "lancia",
    "land rover",
    "landwind",
    "lexus",
    "lotus",
    "maserati",
    "maybach",
    "mazda",
    "mclaren",
    "mercedes-benz",
    "mg",
    "mini",
    "mitsubishi",
    "morgan",
    "nissan",
    "opel",
    "peugeot",
    "porsche",
    "renault",
    "rolls-royce",
    "rover",
    "saab",
    "seat",
    "skoda",
    "smart",
    "ssangyong",
    "subaru",
    "suzuki",
    "tesla",
    "toyota",
    "volkswagen",
    "volvo"}

semantic_dict = {"Business name": businessName_set,
                 "Street name": street_set,
                 "School name": school_set,
                 "City agency": cityAgency_set,
                 "Parks": park_set,
                 "Color": color_set,
                 "Areas of study": study_set,
                 "City": city_set,
                 "Vehicle Type": vehicle_set,
                 "Subjects in school": subject_set,
                 "Type of location": location_set,
                 "Car make": car_set,
                 "Neighborhood": neighborhood_set,
                 "Borough": borough_set}
# re 判断
building_classification = re.compile(r"^[A-Z]\d-[A-Z]+-?[A-Z]+$")
coordinates = re.compile(
    r"\([\-\+]?(0?\d{1,2}\.\d{1,15}|1[0-7]?\d{1}\.\d{1,15}|180\.0{1,15}),\s?[\-\+]?([0-8]?\d{1}\.\d{1,15}|90\.0{1,15})\)$")
schoolLevel = re.compile(r"K-(\d|1[0-2])")
zip = re.compile(r'\b(\d{5}(?:[-\s]\d{4})?|\d{9})\b')
phone = re.compile(r'\b(\d{10}|\d{3}[-\.\s]\d{3}[-\.\s]\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]\d{4})\b')
university = re.compile(r'([A-Za-z]\s)*(College|University|Institute of Technology|Law School)')

website = re.compile(r"(HTTP\:\/\/|HTTP\:\/\/)+([A-Za-z0-9][A-Za-z0-9\-]*\.)+[A-Za-z0-9][A-Za-z0-9\-]*")
address = re.compile(
        r'^[\w\s]{1,20}(?:STREET|AVENUE|ROAD|BOULEVARD|PLACE)\W?(?=\s|$)',
        re.IGNORECASE)

def isCity(string):
    string = string.lower()
    city = {'bronx', 'brooklyn', 'manhattan', 'queens', 'staten island', 'nyc'}
    if string in city:
        return True
    string = string.title()
    places = GeoText(string)
    if len(places.cities):
        return True
    else:
        return False

def toSemanticTuple(x):
    value = x[:-(len(x.split()[-1]) + 1)]
    count = int(x.split()[-1])
    semantic_type = "other"
    if building_classification.match(value):
        semantic_type = "Building Classification"
    elif coordinates.match(value):
        semantic_type = "LAT/LON coordinates"
    elif schoolLevel.match(value):
        semantic_type = "School Levels"
    elif zip.match(value):
        semantic_type = "Zip code"
    elif phone.match(value):
        semantic_type = "Phone number"
    elif university.match(value):
        semantic_type = "College/University names"
    elif website.match(value):
        semantic_type = "Websites"
    elif address.match(value):
        semantic_type = "Address"
    elif isCity(value):
        semantic_type = "City"
    else:
        value = value.lower()
        for item in schoolLevel_set:
            if value == item:
                semantic_type = "School Levels"
        for key in semantic_dict.keys():
            set = semantic_dict.get(key)
            for item in set:
                if value.find(item) != -1:
                    semantic_type = key
                    # print(value)
                    break
        if semantic_type == "other":
            doc = nlp(value)
            # print(len(doc.ents))
            for ent in doc.ents:
                if ent.label_ == "PERSON":
                    semantic_type = "PERSON"
                    # print(value)
                    break
                if ent.label_ == "ORG":
                    semantic_type = "Business name"
                    # print(value)
    # if semantic_type == "School Levels":
    #     print(value)
    return (semantic_type, count)


file_list = []
for root, dirs, files in os.walk('NYCColumns'):
    for f in files:
        file_list.append(f)
# print(len(file_list))
# i = 0
# with open("pre_index_for_task2"+'.txt', 'w') as f:
#     for item in file_list:
#         f.write(str(i)+"*"+item+"\n")
#         i= i+1

# start_position = int(sys.argv[1])
# end_position = int(sys.argv[2])
id_list = []
for i in sys.argv[1:]:
    id_list.append(int(i))
# for id in range(start_position, end_position):
for id in id_list:
    try:
        print("processing the" + str(id) + "th dataset:" + str(file_list[id]))
        lines = sc.textFile('NYCColumns/' + file_list[id])
        origin = lines.map(toSemanticTuple).reduceByKey(add).collect()
        # print(type(origin))
        semantic_types = []
        for item in origin:
            semantic_types.append({"semantic_type": item[0],
                                   "count": item[1]})
        result_dict = {"column_name": file_list[id].split(".")[1],
                       "semantic_types": semantic_types}
        json_str = json.dumps(result_dict)
        with open("output_for_task2/index*" + str(id) + "*" + file_list[id] + '.json', 'w') as json_file:
            json_file.write(json_str)
        with open("index_for_task2"+'.txt', 'a') as f:
            f.write(str(id) + "*" + file_list[id]+"\n")
    except Exception:
        print("exception")
        with open("output_for_task2/exception.txt", 'a') as f:
            f.write(str(id) + "-" + file_list[id]+"\n")
        pass
    continue
