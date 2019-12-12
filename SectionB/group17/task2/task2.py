# -*- coding: UTF-8 -*-
import json
import os
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add
from fuzzywuzzy import process, fuzz

threshold = 80

def initLists():
    global cities, color, car_make, borough, school_level, vehicle_type, subjects, color, areas, neighbor, agencies, location, nameset

    cities = sc.textFile("cities.txt").collect()
    neighbor = sc.textFile("neighborhood.txt").collect()
    car_make = ['abarth', 'alfa romeo', 'aston martin', 'audi', 'bentley', 'bmw', 'bugatti', 'cadillac', 'chevrolet', 'chrysler', 'citroen', 'dacia', 'daewoo', 'daihatsu', 'dodge', 'donkervoort', 'ds', 'ferrari', 'fiat', 'fisker', 'ford', 'honda', 'hummer', 'hyundai', 'infiniti', 'iveco', 'jaguar', 'jeep', 'kia', 'ktm', 'lada', 'lamborghini', 'lancia', 'land rover', 'landwind', 'lexus', 'lotus', 'maserati', 'maybach', 'mazda', 'mclaren', 'mercedes-benz', 'mg', 'mini', 'mitsubishi', 'morgan', 'nissan', 'opel', 'peugeot', 'porsche', 'renault', 'rolls-royce', 'rover', 'saab', 'seat', 'skoda', 'smart', 'ssangyong', 'subaru', 'suzuki', 'tesla', 'toyota', 'volkswagen', 'volvo']
    borough = ['bronx', 'brooklyn', 'manhattan', 'queens', 'staten island']
    school_level = ['middle', 'elementary', 'high', 'k-2', 'k-3', 'k-4', 'k-5', 'k-6', 'k-7', 'k-8', 'k-9', 'k-10', 'k-11', 'k-12'] # transfer school ?
    vehicle_type = ['sedan', 'ambulance', 'truck', 'bicycle', 'bus', 'convertible', 'motorcycle', 'vehicle', 'moped', 'scooter', 'taxi', 'pedicab', 'boat', 'van', 'bike', 'tank']
    subjects = ['algebra', 'chemistry', 'earth science', 'economics', 'english', 'geometry', 'global history', 'living environment', 'physics', 'math', 'math a', 'math b', 'us history', 'science', 'us government', 'us government & economics', 'us history', 'social studies']
    color = ['amber', 'apricot', 'aqua', 'auburn', 'azure', 'beige', 'black', 'blue', 'bronze', 'brown', 'burgundy', 'charcoal', 'cherry blossom pink', 'chocolate', 'cobalt', 'copper', 'cream', 'crimson', 'cyan', 'dandelion', 'dark', 'denim', 'ecru', 'emerald green', 'forest green', 'fuchsia', 'gold', 'green', 'grey', 'indigo', 'ivory', 'jade', 'khaki', 'lavender', 'lemon', 'lilac', 'lime green', 'magenta', 'maroon', 'mauve', 'mint green', 'moss green', 'mustard', 'navy blue', 'olive', 'orange', 'peach', 'pink', 'powder blue', 'puce', 'prussian blue', 'purple', 'quartz grey', 'red', 'rose', 'royal blue', 'ruby', 'salmon pink', 'sandy brown', 'sapphire', 'scarlet', 'shocking pink', 'silver', 'sky blue', 'tan', 'tangerine', 'turquoise', 'violet', 'white', 'yellow']
    areas = ['animal science', 'architecture', 'business', 'communications', 'computer science & technology', 'cosmetology', 'culinary arts', 'engineering', 'environmental science', 'film/video', 'health professions', 'hospitality, travel, & tourism', 'humanities & interdisciplinary', 'jrotc', 'law & government', 'performing arts', 'performing arts/visual art & design', 'science & math', 'teaching', 'visual art & design', 'zoned']
    agencies = ["NYCOA", "AJC", "OATH", "DFTA", "MOA", "BPL", "DOB", 'BIC', 'CFB', 'CIDI', 'OCME', 'ACS', 'CLERK', 'DCP', 'CUNY', 'DCAS', 'CECM', 'CEC', 'CSC', 'CCRB', 'CGE', 'CCPC', 'CAU', 'CB', 'COMP', 'COIB', 'DCA', 'DCWP', 'MOCS', 'BOC', 'DOC', 'DCLA', 'MODA', 'DDC', 'DOE', 'BOE', 'MOEC', 'DEP', 'EEPC', 'DOF', 'FDNY', 'GNYC', 'DOHMH', 'DHS', 'NYCHA', 'HPD', 'HRO', 'HRA', 'CCHR', 'MOIA', 'IBO', 'MOIP', 'DOITT', 'MOIGA', 'DOI', 'MACJ', 'OLR', 'LPC', 'LAW', 'BPL', 'NYPL', 'QL', 'LOFT', 'OMB', 'MCCM', 'OM', 'IA', 'MOPD', 'OER', 'MOSPCE', 'OMWBE', 'OSP', 'ENDGBV', 'MOME', 'NYCGO', 'NYCEDC', 'NYCERS', 'SERVICE', 'TFA', 'NYPL', 'OPS', 'DPR', 'OPA', 'NYPD', 'PPF', 'DOP', 'PPB', 'BCPA', 'KCPA', 'QPA', 'RCPA', 'PUB ADV', 'QPL', 'DORIS', 'RGB', 'STAR', 'DSNY', 'SCA', 'SBS', 'DSS', 'OSE', 'SNP', 'BSA', 'TAT', 'TC', 'TLC', 'DOT', 'DVS', 'NYWB', 'NYW', 'DYCD', '311']
    location = ['building', 'terminal', 'atm', 'bank', 'bar', 'club', 'salon', 'bridge', 'stop', 'cemetery', 'church', 'clothing', 'boutique', 'site', 'facility', 'office', 'cleaner', 'laundry', 'factory', 'warehouse', 'fast food', 'supermarket', 'station', 'grocery', 'bodega', 'gym', 'fitness', 'highway', 'parkway', 'shelter', 'hospital', 'hotel', 'motel', 'jewelry', 'company', 'inside', 'outside', 'marina', 'pier', 'mosque', 'park', 'playground', 'parking lot', 'garage', 'school', 'restaurant', 'diner', 'shoe', 'merchant', 'synagogue', 'tramway', 'subway', 'tunnel', 'store']

    global phone_pattern, address_pattern, street_pattern, coordinate_pattern, zip_pattern, school_pattern, park_pattern, website_pattern, building_pattern

    address_pattern = re.compile(r'[0-9 ]+([0-9]*[(th)(st)(nd)(rd)] *)?[a-z0-9\. ]*')
    street_pattern = re.compile(r'([0-9]*[(th)(st)(nd)(rd)] *)?[a-z0-9\. ]*')
    school_pattern = re.compile(r'^([a-z ])*school[a-z0-9\-\.\, ]+|[a-z0-9\-\.\, ]*school|[a-z0-9\-\.\, ]*academy|[a-z0-9\-\.\, ]*institute')
    park_pattern = re.compile(r'[a-z0-9\-\'\.\(\) ]*park|[a-z0-9\-\'\.\(\) ]*playground|[a-z0-9\-\'\.\(\) ]*garden|[a-z0-9\-\'\.\(\) ]*center|[a-z0-9\-\'\.\(\) ]*field|[a-z0-9\-\'\.\(\) ]*square|[a-z0-9\-\'\.\(\) ]*beach|[a-z0-9\-\'\.\(\) ]*ground|[a-z0-9\-\'\.\(\) ]*ground|[a-z0-9\-\'\.\(\) ]*pk$')
    website_pattern = re.compile(r'http[s]?:\/\/|www\.|[a-z0-9\.\-_]*\.org|[a-z0-9\.\-_]*\.com|[a-z0-9\.\-_]*\.edu|[a-z0-9\.\-_]*\.gov|[a-z0-9\.\-_]*\.net|[a-z0-9\.\-_]*\.info|[a-z0-9\.\-_]*\.us|[a-z0-9\.\-_]*\.nyc')
    building_pattern = re.compile(r'[a-z][0-9](\-[a-z\-]*)?')

    global street_suffix, company_suffix
    street_suffix = ('street', 'road', 'avenue', 'drive', 'lane', 'court', 'place', 'boulevard', 'way', 'parkway', 'st', 'rd', 'av', 'ave', 'dr', 'pl', 'blvd', 'st w', 'st e')
    company_suffix = ('architecture', 'corp', 'inc', 'group', 'design', 'consulting', 'service', 'mall', 'taste', 'fusion', 'llc', 'pllc', 'deli', 'pizza', 'restaurant', 'chinese', 'shushi', 'bar', 'snack', 'cafe', 'coffee', 'kitchen', 'grocery', 'food', 'farm', 'market', 'wok', 'gourmet', 'p.c.', 'burger', 'engineering', 'laundromat', 'wine', 'liquors', 'garden', 'diner', 'cuisine', 'place', 'cleaners', 'pizzeria', 'shop', 'inc.', 'architect', 'engineer', 'china')

    global functionToTypes, typeToFunction, type_list
    type_list = ["Person Name", "Business name", "Phone Number", "Address", "Street name", "City", "Neighborhood", "LAT/LON coordinates", "Zip code", "Borough", "School name", "Color","Car make", "City agency", "Areas of study", "Subjects in school", "School Levels", "College/University names", "Websites", "Building Classification", "Vehicle Type", "Type of location", "Parks/Playgrounds", "other"]
    functionToTypes = {isPersonName: "Person Name", isBussinessName: "Business name", isPhoneNumber: "Phone Number", isAddress: "Address", isStreetName: "Street name", isCity: "City", 
                      isNeighborhood: "Neighborhood", isCoordinates: "LAT/LON coordinates", isZipcode: "Zip code", isBorough: "Borough", isSchool: "School name", isColor: "Color",
                      isCarMake: "Car make", isAgency: "City agency", isStudyArea: "Areas of study", isSubject: "Subjects in school", isSchoolLevel: "School Levels", isCollege: "College/University names",
                      isWebsite: "Websites", isBuildingClass: "Building Classification", isVehicleType: "Vehicle Type", isLocationType: "Type of location", isPark: "Parks/Playgrounds", isOther: "other"}
    typeToFunction = {}
    for func in functionToTypes:
        typeToFunction[functionToTypes[func]] = func

class Column:
    column_name = ""
    semantic_types = []

    def __init__(self, name):
        self.column_name = name

class SemanticType:
    semantic_type = ""
    label = ""
    count = 0

    def __init__(self, type, label, count):
        self.semantic_type = type
        self.label = label
        self.count = count

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def checkItemInList(keyword, keyword_list):
    matched = process.extractOne(keyword, keyword_list)
    if matched[1] > threshold:
        return True
    else:   
        return False

def isPersonName(keyword):
    if re.compile(r'^[a-z]*, *[a-z]*|[a-z]*').match(keyword):
        return True
    return False

def isBussinessName(keyword):
    for suffix in company_suffix:
        if suffix in keyword:
            return True
    return False

def isPhoneNumber(keyword):
    return re.match(re.compile(r'1?[ -\.]*[0-9]{3}[ -\.]*[0-9]{3}[ -\.]*[0-9]{4}|.*\(.*[0-9]{3}.*\).*[0-9]{3}.*-.*[0-9]{4}'), keyword)

def isAddress(keyword):
    if re.compile(r'^[0-9]+').match(keyword):
        address_num = re.compile(r'^[0-9\-]+').findall(keyword)[0]
        if not keyword[len(address_num):].startswith(('th', 'st', 'nd', 'rd')) and isStreetName(keyword[len(address_num):]):
            return True
        else:
            return False
    else:
        return False

def isStreetName(keyword):
    if keyword.strip() in street_suffix:
        return False
    if re.match(street_pattern, keyword) and keyword.endswith(street_suffix):
        return True
    else:
        return False

def isCity(keyword):
    if checkItemInList(keyword, cities) or keyword.endswith('city'):
        return True
    return False

def isNeighborhood(keyword):
    return checkItemInList(keyword, neighbor)

def isCoordinates(keyword):
    if re.match(re.compile(r'\(?-?[0-9]{1,3}\.[0-9]*, *-?[0-9]{1,3}\.[0-9]*\)?'), keyword):
        group = re.findall(r'-?[0-9]{1,3}\.?[0-9]*', keyword)
        if (float(group[0]) < 180.0 or float(group[0]) > -180.0) or (float(group[1]) < 180.0 or float(group[1]) > -180.0):
            return True
    return False

def isZipcode(keyword):
    return re.match(re.compile(r'[0-9]{5}|[0-9]{5}-[0-9]{4}'), keyword)

def isBorough(keyword):
    return checkItemInList(keyword, borough)

def isSchool(keyword):
    return re.match(school_pattern, keyword)

def isColor(keyword):
    return checkItemInList(keyword, color)

def isCarMake(keyword):
    return checkItemInList(keyword, car_make)

def isAgency(keyword):
    return checkItemInList(keyword, agencies)

def isStudyArea(keyword):
    return checkItemInList(keyword, areas)

def isSubject(keyword):
    return checkItemInList(keyword, subjects)

def isSchoolLevel(keyword):
    return checkItemInList(keyword, school_level)

def isCollege(keyword):
    return re.compile(r'[a-z0-9\.\- ]*').match(keyword) and keyword.endswith(('college', 'university'))

def isWebsite(keyword):
    return re.match(website_pattern, keyword)

def isBuildingClass(keyword):
    return re.match(building_pattern, keyword)

def isVehicleType(keyword):
    return checkItemInList(keyword, vehicle_type)

def isLocationType(keyword):
    return checkItemInList(keyword, location)

def isPark(keyword):
    return re.match(park_pattern, keyword)

def isOther(keyword):
    if len(keyword) == 1 and re.match(re.compile(r'[`\-\.a-z0-9]'), keyword):
        return True
    return False

def getSemanticType(keyword, strategy):
    if keyword is None or len(keyword) == 0:
        return -1
    keyword_type = 'other'
    for checkFunction in strategy:
        if checkFunction(keyword):
            return functionToTypes[checkFunction]
    return 'other'

def checkSemanticType(input, strategy):
    if input is None:
        return (('other', 'None'), (1, 1))
    key = input[0].strip()
    result = ['', '', 1, input[1]]
    result[0] = getSemanticType(key.lower(), strategy)
    return ((result[0], result[1]), (result[2], result[3]))

def getStrategy(column_name):
    strategy = [match for match in process.extract(column_name, type_list, limit=len(typeToFunction))]
    strategy = [item[0] for item in strategy if item[1] > 40]
    strategy = [typeToFunction[currtype] for currtype in strategy]
    if 'vehicle' in column_name:
        if isCarMake in strategy:
            strategy.remove(isCarMake)
        strategy.insert(1, isCarMake)
    elif 'city' in column_name:
        if isNeighborhood in strategy:
            strategy.remove(isNeighborhood)
        strategy.insert(1, isNeighborhood)
        if isBorough in strategy:
            strategy.remove(isBorough)
        strategy.insert(1, isBorough)
    elif 'mi' in column_name or 'initial' in column_name:
        if isOther in strategy:
            strategy.remove(isOther)
        strategy.insert(1, isOther)
    elif 'interest' in column_name:
        if isStudyArea not in strategy:
            strategy.insert(1, isStudyArea)
    elif 'course' in column_name:
        if isSubject not in strategy:
            strategy.insert(1, isSubject)
    elif 'location' in column_name:
        if isCoordinates not in strategy:
            strategy.insert(0, isCoordinates)
    return strategy

def getPredictedLabel(labels):
    getMaxPerc = [labels[0]]
    max_percent = labels[0][1][0]
    for label in labels[1:]:
        if label[1][0] / max_percent > 0.5:
            getMaxPerc.append(label)
        else:
            break
    labels = sorted(labels, key=lambda x:x[1][2], reverse=True)
    if labels[0] not in getMaxPerc and labels[0][0][0] != 'other':
        getMaxPerc.append(labels[0])
    labels = sorted(labels, key=lambda x:x[1][1], reverse=True)
    if labels[0] not in getMaxPerc and labels[0][0][0] != 'other':
        getMaxPerc.append(labels[0])
    getMaxPerc = sorted(getMaxPerc, key=lambda x:x[1][1], reverse=True)
    print(getMaxPerc)
    max_count = getMaxPerc[0][1][1]
    index = 1
    for label in getMaxPerc[1:]:
        if label[1][1] / float(max_count) > 0.33:
            index += 1
        else:
            break
    getMaxPerc = getMaxPerc[:index]
    getMaxPerc = sorted(getMaxPerc, key=lambda x:x[1][2], reverse=True)
    predicted = [getMaxPerc[0][0][0]]
    max_count = getMaxPerc[0][1][2]
    for label in getMaxPerc[1:]:
        if label[1][2] / float(max_count) > 0.33:
            predicted.append(label[0][0])
        else:
            break
    if len(predicted) > 1 and 'other' in predicted and 'other' != predicted[0]:
        predicted.remove('other')
    return predicted

if __name__ == "__main__":
    sc = SparkContext()
    initLists()

    path = "./NYCColumns/"

    cluster = open("cluster1.txt", 'r')
    task2_files = [file.strip().strip('\'') for file in cluster.read().strip('[]').split(',')]
    task2_files = list(set(task2_files)) 
    cluster.close()

    column_list = []
    column_types = {}
    column_predicted_count = [0 for i in range(len(type_list))]
    column_true_count = [0 for i in range(len(type_list))]
    column_type_matrix = [[0 for i in range(len(type_list))] for j in range(len(type_list))]

    count = 0
    for file in task2_files:
        if os.stat(path + file).st_size > 1024 * 500:
            continue
        count += 1
        print("Processing File %d %s" % (count, file))
        column_name = file.split('.')[0] + '.' + file.split('.')[1]
        currColumn = Column(column_name)
        # check strategy
        checkStrategy = getStrategy(file.split('.')[1].lower())
        column = sc.textFile(path + file)
        column = column.map(lambda x: (x.split("\t")[0], int(x.split("\t")[1]))) \
                       .map(lambda x: checkSemanticType(x, checkStrategy)) \
                       .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                       .map(lambda x: ((x[0][0], x[0][1]), (float(x[1][1]) / x[1][0], x[1][0], x[1][1]))) \
                       .sortBy(lambda x: -x[1][0])

        items = column.collect()
        predictedLabel = getPredictedLabel(items)
        column_types[column_name] = predictedLabel
        currColumn.semantic_types = [SemanticType(item[0][0], item[0][1], item[1][2]) for item in items]
        
        column_list.append(currColumn)
        print("Column %s predicted label is %s" % (column_name, predictedLabel))
        print("File %d %s finish" % (count, file))

    true_types_file = open("Manually_Label.txt", 'r')
    for line in true_types_file.readlines():
        line = line.strip()
        column_name = line.split(' ')[0][:-7]
        if column_name not in column_types:
            continue
        column_true_type = line.split(' ')[-1]
        true_type_index = type_list.index(process.extractOne(column_true_type, type_list)[0])
        if len(column_types[column_name]) != 1 or fuzz.token_sort_ratio(column_types[column_name][0], column_true_type.replace('_', ' ')) < threshold:
            print(column_name)
            print("true type index %s" % (column_true_type))
            print("predicted types %s\n" % str(column_types[column_name]))
        predicted_index_list = [type_list.index(predicted_type) for predicted_type in column_types[column_name]]
        column_true_count[true_type_index] += 1
        for predicted_index in predicted_index_list:
            column_predicted_count[predicted_index] += 1
            column_type_matrix[true_type_index][predicted_index] += 1
    true_types_file.close()

    precision_recall = [[0, 0] for i in range(len(type_list))]
    print("true type count")
    print('\t'.join(str(item) for item in column_true_count))
    print("predicted type count")
    print('\t'.join(str(item) for item in column_predicted_count))
    print("matrix")
    for line in column_type_matrix:
        print(line)

    for i in range(len(type_list)):
        if column_predicted_count[i] != 0:
            precision_recall[i][0] = float(column_type_matrix[i][i]) / column_predicted_count[i]
        if column_true_count[i] != 0:
            precision_recall[i][1] = float(column_type_matrix[i][i]) / column_true_count[i]

    print('precision\trecall')
    for i in range(len(type_list)):
        print('%f\t%f' % (precision_recall[i][0], precision_recall[i][1]))

    write_file = open('task2.json', 'w+')
    json.dump(column_list, write_file, default=lambda x: x.__dict__, sort_keys=True)
    write_file.close()
    sc.stop()