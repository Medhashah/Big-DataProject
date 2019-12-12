import sys
import csv
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from names_dataset import NameDataset
import json
#from pyspark import SparkContext

# read single column dataset and return as a set
def getLabelList():
    labelList = {'person_name'.upper(), 'business_name'.upper(), 'phone_number'.upper(), 
            'address'.upper(), 'street_name'.upper(), 'city'.upper(),
            'neighborhood'.upper(), 'lat_lon_cord'.upper(), 'zip_code'.upper(), 
            'borough'.upper(), 'school_name'.upper(), 'color'.upper(), 'car_make'.upper(), 
            'city_agency'.upper(), 'area_of_study'.upper(), 'subject_in_school'.upper(), 
            'school_level'.upper(), 'college_name'.upper(), 'website'.upper(), 
            'building_classification'.upper(), 'vehicle_type'.upper(), 'location_type'.upper(), 
            'park_playground'.upper(), 'other'.upper()}
    return labelList

def readSingleColumnDataset(filePath):
    output = set()
    with open(filePath, 'r', encoding='utf8') as csvFile:
        read = csv.reader(csvFile)
        flag = False   # flag marks whether it is the first reading
        for row in read:
            if(not flag):
                flag = True
                continue
            output.add(row[0].upper())
    return output

def readDoubleColumnDataset(filePath):
    output = set()
    with open(filePath, 'r', encoding='utf8') as csvFile:
        read = csv.reader(csvFile)
        flag = False    # flag marks whether it is the first reading
        for row in read:
            if(not flag):
                flag = True
                continue
            output.add(row[0].upper())
            output.add(row[1].upper())
    return output

# return a dictionary
# for example: output['M4']:'CONVENT'
def readBuildingClassificationDataset(filePath):
    output = dict()
    with open(filePath) as csvfile:
        read = csv.reader(csvfile)
        for row in read:
            output[row[0]] = re.findall(r'[\w]+', row[1])
    return output

def readNeighborhoodDataset(filePath):
    output = set()
    with open(filePath, 'r') as csvFile:
        read = csv.reader(csvFile)
        flag = False
        for row in read:
            if not flag:
                flag = True
                continue
            word = row[0].strip().replace('.', '').replace(' ', '').replace('/', '') \
                                 .replace('-', '').replace('_', '').replace(',', '').upper()
            output.add(word)
    return output

def readData(filePath):
    output = []
    freq = []
    with open(filePath, 'r', encoding='utf8') as file:
        lines = file.readlines()
        for line in lines:
            data = line.split('\t')
            output.append(data[0])
            freq.append(int(data[1]))
    return output, freq

def checkFileName(columnName):
    # check each word to initilally narrow searching range.
    # @argument columnName is a array containing each word of the column name.
    if ('FIRST' in columnName or 'LAST' in columnName) and 'NAME' in columnName:
        return ['PERSON_NAME', 'ADDRESS', 'BUSINESS_NAME']
    elif 'DBA' in columnName or ('BUSINESS' in columnName and 'NAME' in columnName):
        return ['BUSINESS_NAME', 'ADDRESS']
    elif 'PHONE' in columnName or 'FAX' in columnName:
        return ['PHONE_NUMBER']
    elif 'ADDRESS' in columnName and (not 'ZIP' in columnName) and (not 'CITY' in columnName) \
        and (not ('STREET' in columnName and 'NAME' in columnName)):
        return ['ADDRESS', 'BUSINESS_NAME', 'STREET_NAME']
    elif ('STREET' in columnName or 'STR' in columnName) and (not 'ADDRESS' in columnName):
        return ['STREET_NAME', 'ADDRESS']
    elif 'CITY' in columnName:
        return ['CITY', 'BOROUGH', 'NEIGHBORHOOD', 'ZIP_CODE', 'STREET_NAME', 'ADDRESS', 'PHONE_NUMBER']
    elif 'NEIGHBORHOOD' in columnName:
        return ['CITY', 'BOROUGH', 'NEIGHBORHOOD']
    elif ('LAT' in columnName and 'LON' in columnName) or 'LOCATION' in columnName:
        return ['LAT_LON_CORD']
    elif 'ZIP' in columnName:
        return ['ZIP_CODE']
    elif 'BORO' in columnName:
        return ['BOROUGH', 'CITY', 'NEIGHBORHOOD']
    elif (('SCHOOL' in columnName or 'ORG' in columnName) and 'NAME' in columnName) or columnName == 'SCHOOL':
        return ['SCHOOL_NAME', 'PARK_PLAYGROUND', 'ADDRESS', 'COLLEGE_NAME']
    elif 'COLOR' in columnName:
        return ['COLOR']
    elif 'MAKE' in columnName or 'MODEL' in columnName:
        return ['CAR_MAKE', 'COLOR']
    elif 'AGENCY' in columnName:
        return ['CITY_AGENCY']
    elif 'INTEREST' in columnName:
        return ['AREA_OF_STUDY']
    elif 'SUBJECT' in columnName or 'COURSE' in columnName:
        return ['SUBJECT_IN_SCHOOL']
    elif 'LEVEL' in columnName or ('SCHOOL' in columnName and 'TYPE' in columnName):
        return ['SCHOOL_LEVEL']
    elif 'UNIVERSITY' in columnName or 'COLLEGE' in columnName:
        return ['COLLEGE_NAME']
    elif 'WEBSITE' in columnName:
        return ['WEBSITE']
    elif 'CLASSIFICATION' in columnName:
        return ['BUILDING_CLASSIFICATION']
    elif 'VEHICLE' in columnName and  'TYPE' in columnName:
        return ['VEHICLE_TYPE', 'CAR_MAKE']
    elif 'PREM' in columnName and 'TYP' in columnName and 'DESC' in columnName:
        return ['LOCATION_TYPE']
    elif 'PARK' in columnName:
        return ['PARK_PLAYGROUND', 'SCHOOL_NAME']
    elif 'CANDMI' in columnName or 'MI' in columnName:
        return ['PERSON_NAME']
    elif 'LANDMARK' in columnName:
        return ['COLLEGE_NAME', 'STREET_NAME', 'LOCATION_TYPE', 'PARK_PLAYGROUND']
    else:
        return ['NONE MATCH']

def checkColor(cell):
    '''
        # split the column value by ',', ' ', '.', '/' if it is long
        # if not split, use fuzzywuzzy to compare to each value in dataset
        # and set a threshold to handle the mispelled(length of 4 characters or more)
        # if split, both part should be in the dataset.
        # Can correctly judge data like colorAbbreviation1/colorAbbreviation2, CA1.CA2, CA1-CA2, etc.
    '''
    cell = cell.strip()
    if(cell in relativeDataSets['COLOR']):
        return True
    else:
        values = re.findall(r'[\w]+', cell)
        if len(values) == 0:
            return False
        elif len(values) == 1:
            if len(values[0]) == 4:
                v1, v2 = values[0][:2], values[0][2:]
                if(v1 in relativeDataSets['COLOR'] and v2 in relativeDataSets['COLOR']):
                    return True
            else:
                return False
        else:
            for v in values:
                if not v in relativeDataSets['COLOR']:
                    return False
            return True

def checkCity(cell):
    # TODO
    cell = cell.strip('[_-`/., \']')
    if(cell in relativeDataSets['CITY']):
        return True
    else:
        return False


def checkSchoolLevel(cell):
    cell = cell.strip()
    if cell in relativeDataSets['SCHOOL_LEVEL']:
        return True
    for word in cell.split(' ', 1):
        if word in relativeDataSets['SCHOOL_LEVEL']:
            return True
    return False

def checkCarMake(cell):
    # Since there are some data which is abbreviations of full names, we handle this condition.
    cell = cell.strip('[_-`/., \']')
    if cell in relativeDataSets['CAR_MAKE']:
        return True

    # Recognize some special data format like AUDI Q7; LEXUS XXX
    words = cell.split(' ', 1)
    if len(words) > 1:
        if words[0] in relativeDataSets['CAR_MAKE']:
            return True
        else:
            return False

    # choose candidates whose length difference is no more than 2.
    cellLength = len(cell)
    if cellLength <= 4:
        return False
    candidates = []
    for cm in relativeDataSets['CAR_MAKE']:
        if(abs(len(cm)-len(cell)) <= 2):
            candidates.append(cm)
    if not candidates:
        return False
    fw_output = process.extractOne(cell, candidates)

    for carMake in relativeDataSets['CAR_MAKE']:
        if cell in carMake and len(cell) >= 4:
            return True
        if fw_output:
            if fw_output[1] >= 88:
                return True
    return False


# argument @dataSet is a object of NameDataset()
def checkPersonName(cell):
    # matching abbreviation pattern like B.W.(Bruce Wayne)
    if re.match('^[A-Z]\.\s*[A-Z]\.', cell):
        return True
    # matching abbreviation pattern like MR. PARKER, MS. PEPPER OR T. STARK
    if re.match('^MR\.*\s*[A-Z]+', cell) or re.match('^MS\.*\s*[A-Z]+', cell) \
        or re.match('^[A-Z]\.*\s*[A-Z]+', cell):
        lastName = re.findall(r'[\w]+', cell)[-1]
        if not relativeDataSets['PERSON_NAME'].search_last_name(lastName):
            return False
        return True

    # matching regular name
    cell = cell.strip('[+,/-._;]')
    words = re.findall(r'[\w]+', cell)
    if len(words) == 0:
        return False
    elif len(words) == 1:
        return relativeDataSets['PERSON_NAME'].search_first_name(words[0]) or relativeDataSets['PERSON_NAME'].search_last_name(words[0])
    else:
        for i in range(len(words)-1):
            if not relativeDataSets['PERSON_NAME'].search_first_name(words[i]):
                return False
        if not relativeDataSets['PERSON_NAME'].search_last_name(words[-1]):
            return False
        return True

# argument @dataSet is a dictionary
def checkBuildingClassification(cell):
    words = re.findall(r'[\w]+', cell)
    number, words = words[0], words[1:]
    for word in words:
        if not word in relativeDataSets['BUILDING_CLASSIFICATION'][number]:
            return False
    return True

# argument @abbrDataSet is a set containing all agencies' abbreviation names.
# argument @relativeWords is a set containing all agency-name's relative words.
def checkAgency(cell):
    relativeWords = {'OFFICE', 'ADMINISTRATION', 'BOARD', 'COMMISSION', 'COUNCIL',
                    'DEPARTMENT', 'AGENCY', 'CENTER', 'MANAGEMENT', 'AUTHORITY', 'ADMIN',
                    'LIBRARY', 'BD', 'SERVICE', 'COUNCIL', 'DEPT', 'CORP.', 'DEV.',
                    'COMM.', 'ADJUSTMENT', 'OFF.', 'ADMINISTRATOR'}
    if cell in relativeDataSets['CITY_AGENCY']:
        return True
    for word in relativeWords:
        if word in cell:
            return True
    return False

def checkNeighborhood(cell):
    cell = cell.strip().replace('.', '').replace(' ', '').replace('/', '') \
                       .replace('-', '').replace('_', '').replace(',', '')
    if cell in relativeDataSets['NEIGHBORHOOD']:
        return True
    else:
        cellLength = len(cell)
        if cellLength <= 4:
            return False
        candidates = []
        for neighbor in relativeDataSets['NEIGHBORHOOD']:
            if(abs(len(neighbor)-len(cell)) <= 2):
                candidates.append(neighbor)
        if not candidates:
            return False
        fw_output = process.extractOne(cell, candidates)

        if fw_output:
            if fw_output[1] >= 88:
                return True
    return False

def checkVehicleType(cell):
    cell = cell.strip('[_-`/., \']')
    relativeWords = {'SEDAN', 'AMBULANCE', 'TRUCK', 'CAB', 'CONVERTIBLE', 'VEH', 'VEHICLE', 
                     'MOTORCYCLE', 'MOTORSCOOTER', 'WAGON', 'VAN', 'TAXI', 'BUS'}
    if cell in relativeDataSets['VEHICLE_TYPE']:
        return True
    words = re.findall(r'[\w]+', cell)
    for word in words:
        if word in relativeWords:
            return True
    return False

def checkBorough(cell):
    cell = cell.strip()
    abbr = {'K','M','Q','R','X'}
    if cell in relativeDataSets['BOROUGH']:
        return True
    elif cell in abbr:
        return True
    else:
        cellLength = len(cell)
        if cellLength <= 4:
            return False
        candidates = []
        for boro in relativeDataSets['BOROUGH']:
            if(abs(len(boro)-len(cell)) <= 2):
                candidates.append(boro)
        if not candidates:
            return False
        fw_output = process.extractOne(cell, candidates)

        if fw_output:
            if fw_output[1] >= 88:
                return True
    return False

def checkSubjects(cell):
    cell = cell.strip()
    if cell in relativeDataSets['SUBJECT_IN_SCHOOL']:
        return True
    return False

def checkAreasOfStudy(cell):
    cell = cell.strip()
    if cell in relativeDataSets['AREA_OF_STUDY']:
        return True
    return False

def checkTypeLocation(cell):
    cell = cell.strip()
    if cell in relativeDataSets['LOCATION_TYPE']:
        return True
    return False

def checkPark(cell):
    cell = cell.strip()
    words = re.findall(r'[\w]+', cell)
    for word in words:
        if word in relativeDataSets['PARK_PLAYGROUND']:
            return True
    return False

regex_coordinate = re.compile('^\([-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)\)$')
def checkCoordinate(cell):
    return regex_coordinate.search(cell) != None
    
regex_phonenumber = re.compile('^((\+?)1)?((\(\d{3}\)?)|(\d{3}))([\s\-.\/]?)(\d{3})([\s\-.\/]?)(\d{4})$')
def checkPhoneNumber(cell):
    return regex_phonenumber.search(cell) != None

regex_website = re.compile('^(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$')
def checkWebsite(cell):
    return regex_website.search(cell.lower()) != None
    
regex_zipcode = re.compile('^[0-9]{4,5}(\-[0-9]{3,4})?$')
regex_nonzero = re.compile('[1-9]')
regex_nonnine = re.compile('[0-8]')
def checkZipcode(cell):
    if regex_zipcode.search(cell)!= None:
        return regex_nonnine.search(cell)!= None and regex_nonzero.search(cell)!= None
    else:
        return False

school_keywords = ['SCHOOL', 'ACAD ', 'ACADEMIC', 'ACADEMY', \
'INSTITUTE', 'CENTER', 'MS ', 'PS ', 'IS ', 'JHS ', 'SCH ', \
'SCH-', 'ELEMENTARY', " HS", "PREPARATORY"]
def checkSchoolName(cell):
    for keyword in school_keywords:
        if keyword.lower() in cell.lower():
            return True
    return False
    
university_keywords = ['COLLEGE', 'UNIVERSITY', 'ACADEMY']
def checkUniversityName(cell):
    for keyword in university_keywords:
        if keyword.lower() in cell.lower():
            return True
    return False
    
regex_address = re.compile('\d{1,4}(th|st|rd)?\s+(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)')
def checkAddress(cell):
    return regex_address.search(cell.lower())!= None
    
regex_streetname = re.compile('(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd|place|pl)\s*$')
def checkStreetname(cell):
    return regex_streetname.search(cell.lower())!= None

regex_businessname = re.compile('(^|\s)((LLC|GROUP|INC|LTD|LLP|PC|CO|P.C|PLLC|CORP|PE|RA|P.E|R.A)(\s|$|\.))|(ARCHITECT|ENGINEER|CONSULT|DESIGN|ASSOC)')
def checkBusinessname(cell):
    return regex_businessname.search(cell.upper())!= None

def noneMatchCondition(cell):
    return False

def splitText(x):
    cell, freq = x.split('\t', 1)
    return (cell, freq)

def judgeSemanticType(x):
    for possible_type in relativeTypes:
        if functionDict[possible_type](x[0]):
            return (possible_type, x[1])
    return ('OTHER', x[1])

def getTopTwoFreq(dictionary):
    candidates = []
    for key in dictionary:
        if key != 'OTHER':
            candidates.append((key, dictionary[key]))
    total = 0
    for cand in candidates:
        total += cand[1]
    if total == 0:
        return []
    output = []
    for cand in candidates:
        if cand[1]/total > 0.4:
            output.append(cand[0])
    return output


def readFilenameList():
    outputList = None
    with open('cluster2.txt') as textFile:
        outputList = textFile.read()
        outputList = outputList[1:len(outputList)-1].replace('\'', '').split(', ')
    print('filename list length: ', len(outputList))
    return outputList



# since we can't install local python environment in dumbo, we complete the output data
# by local computer. However, we provide a extra Spark-style code on bottom.

for fn in readFilenameList():
    print('start process file: ', fn)

    fileName = fn.upper()
    fileName = fileName.split('.')
    cells, freqs = readData('NYCColumns/' + fn)
    relativeTypes = checkFileName(fileName[1])
    relativeDataSets = dict()
    if 'PERSON_NAME' in relativeTypes:
        relativeDataSets['PERSON_NAME'] = NameDataset()
    if 'CITY' in relativeTypes:
        relativeDataSets['CITY'] = readSingleColumnDataset('dataset/us_cities.csv')
    if 'NEIGHBORHOOD' in relativeTypes:
        relativeDataSets['NEIGHBORHOOD'] = readNeighborhoodDataset('dataset/neighborhood.csv')
    if 'BOROUGH' in relativeTypes:
        relativeDataSets['BOROUGH'] = readSingleColumnDataset('dataset/Borough.csv')
    if 'COLOR' in relativeTypes:
        relativeDataSets['COLOR'] = readDoubleColumnDataset('dataset/color_names.csv')
    if 'CAR_MAKE' in relativeTypes:
        relativeDataSets['CAR_MAKE'] = readSingleColumnDataset('dataset/car_makes.csv')
    if 'CITY_AGENCY' in relativeTypes:
        relativeDataSets['CITY_AGENCY'] = readSingleColumnDataset('dataset/agency_abbreviation.csv')
    if 'AREA_OF_STUDY' in relativeTypes:
        relativeDataSets['AREA_OF_STUDY'] = readSingleColumnDataset('dataset/AreasOfStudy.csv')
    if 'SUBJECT_IN_SCHOOL' in relativeTypes:
        relativeDataSets['SUBJECT_IN_SCHOOL'] = readSingleColumnDataset('dataset/subjects.csv')
    if 'SCHOOL_LEVEL' in relativeTypes:
        relativeDataSets['SCHOOL_LEVEL'] = readSingleColumnDataset('dataset/school_levels.csv')
    if 'BUILDING_CLASSIFICATION' in relativeTypes:
        relativeDataSets['BUILDING_CLASSIFICATION'] = readBuildingClassificationDataset('dataset/building_classification.csv')
    if 'VEHICLE_TYPE' in relativeTypes:
        relativeDataSets['VEHICLE_TYPE'] = readSingleColumnDataset('dataset/vehicle_types.csv')
    if 'LOCATION_TYPE' in relativeTypes:
        relativeDataSets['LOCATION_TYPE'] = readSingleColumnDataset('dataset/Type_of_location.csv')
    if 'PARK_PLAYGROUND' in relativeTypes:
        relativeDataSets['PARK_PLAYGROUND'] = {'PARK', 'PLAYGROUND', 'FIELD', 'SQUARE', 'BEACH',
                                               'PARKWAY', 'PLAZA', 'SENIOR CENTER',
                                               'TRIANGLE', 'GARDEN', 'RINK'}

    functionDict = {'PERSON_NAME': checkPersonName, 'BUSINESS_NAME': checkBusinessname,
                    'PHONE_NUMBER': checkPhoneNumber, 'ADDRESS': checkAddress,
                    'STREET_NAME': checkStreetname, 'CITY': checkCity,
                    'NEIGHBORHOOD': checkNeighborhood, 'LAT_LON_CORD': checkCoordinate,
                    'ZIP_CODE': checkZipcode, 'BOROUGH': checkBorough,
                    'SCHOOL_NAME': checkSchoolName, 'COLOR': checkColor,
                    'CAR_MAKE': checkCarMake, 'CITY_AGENCY': checkAgency,
                    'AREA_OF_STUDY': checkAreasOfStudy, 'SUBJECT_IN_SCHOOL': checkSubjects,
                    'SCHOOL_LEVEL': checkSchoolLevel, 'COLLEGE_NAME': checkUniversityName,
                    'WEBSITE': checkWebsite, 'BUILDING_CLASSIFICATION': checkBuildingClassification,
                    'VEHICLE_TYPE': checkVehicleType, 'LOCATION_TYPE': checkTypeLocation,
                    'PARK_PLAYGROUND': checkPark, 'NONE MATCH': noneMatchCondition}



    freqDict = dict()
    for possible_type in relativeTypes:
        freqDict[possible_type] = 0
    freqDict['OTHER'] = 0

    for i in range(len(cells)):
        flag = False
        for possible_type in relativeTypes:
            if functionDict[possible_type](cells[i]):
                freqDict[possible_type] += freqs[i]
                flag = True
                break
        if not flag:
            freqDict['OTHER'] += freqs[i]

    result = []
    for key in freqDict:
        if freqDict[key] == 0:
            continue
        result.append((key, freqDict[key]))


    predictLabel = getTopTwoFreq(freqDict)
    output = dict()

    # output result as json file
    labelList = getLabelList()
    actualTypes = []
    for word in re.findall(r'[\w]+', fileName[1]):
        if word in labelList:
            actualTypes.append(word)
    output['column_name'] = fileName[1]
    output['actualTypes'] = actualTypes
    output['semantic_types'] = [dict() for i in range(len(result))]
    for i in range(len(result)):
        output['semantic_types'][i]['semantic_type'] = result[i][0]
        output['semantic_types'][i]['count'] = result[i][1]
    output['predictLabel'] = predictLabel

    with open('result/'+fileName[0]+'.'+fileName[1]+'.json', 'w') as textFile:
        textFile.write(json.dumps(output))
    print('successfully handle file: ', fn)







'''
# Spark-Style Code 

fileName = sys.argv[1].upper()
fileName = fileName.split('.')
cells, freqs = readData('NYCColumns/' + sys.argv[1])
relativeTypes = checkFileName(fileName[1])
relativeDataSets = dict()
if 'PERSON_NAME' in relativeTypes:
    relativeDataSets['PERSON_NAME'] = NameDataset()
if 'CITY' in relativeTypes:
    relativeDataSets['CITY'] = readSingleColumnDataset('dataset/us_cities.csv')
if 'NEIGHBORHOOD' in relativeTypes:
    relativeDataSets['NEIGHBORHOOD'] = readNeighborhoodDataset('dataset/neighborhood.csv')
if 'BOROUGH' in relativeTypes:
    relativeDataSets['BOROUGH'] = readSingleColumnDataset('dataset/Borough.csv')
if 'COLOR' in relativeTypes:
    relativeDataSets['COLOR'] = readDoubleColumnDataset('dataset/color_names.csv')
if 'CAR_MAKE' in relativeTypes:
    relativeDataSets['CAR_MAKE'] = readSingleColumnDataset('dataset/car_makes.csv')
if 'CITY_AGENCY' in relativeTypes:
    relativeDataSets['CITY_AGENCY'] = readSingleColumnDataset('datset/agency_abbreviation.csv')
if 'AREA_OF_STUDY' in relativeTypes:
    relativeDataSets['AREA_OF_STUDY'] = readSingleColumnDataset('dataset/AreasOfStudy.csv')
if 'SUBJECT_IN_SCHOOL' in relativeTypes:
    relativeDataSets['SUBJECT_IN_SCHOOL'] = readSingleColumnDataset('dataset/subjects.csv')
if 'SCHOOL_LEVEL' in relativeTypes:
    relativeDataSets['SCHOOL_LEVEL'] = readSingleColumnDataset('dataset/school_levels.csv')
if 'BUILDING_CLASSIFICATION' in relativeTypes:
    relativeDataSets['BUILDING_CLASSIFICATION'] = readBuildingClassificationDataset('dataset/building_classification.csv')
if 'VEHICLE_TYPE' in relativeTypes:
    relativeDataSets['VEHICLE_TYPE'] = readSingleColumnDataset('dataset/vehicle_types.csv')
if 'LOCATION_TYPE' in relativeTypes:
    relativeDataSets['LOCATION_TYPE'] = readSingleColumnDataset('dataset/vehicle_types.csv')
if 'PARK_PLAYGROUND' in relativeTypes:
    relativeDataSets['PARK_PLAYGROUND'] = {'PARK', 'PLAYGROUND', 'FIELD', 'SQUARE', 'BEACH',
                                           'PARKWAY', 'PLAZA', 'SENIOR CENTER',
                                           'TRIANGLE', 'GARDEN', 'RINK'}

functionDict = {'PERSON_NAME': checkPersonName, 'BUSINESS_NAME': checkBusinessname,
                'PHONE_NUMBER': checkPhoneNumber, 'ADDRESS': checkAddress,
                'STREET_NAME': checkStreetname, 'CITY': checkCity,
                'NEIGHBORHOOD': checkNeighborhood, 'LAT_LON_CORD': checkCoordinate,
                'ZIP_CODE': checkZipcode, 'BOROUGH': checkBorough,
                'SCHOOL_NAME': checkSchoolName, 'COLOR': checkColor,
                'CAR_MAKE': checkCarMake, 'CITY_AGENCY': checkAgency,
                'AREA_OF_STUDY': checkAreasOfStudy, 'SUBJECT_IN_SCHOOL': checkSubjects,
                'SCHOOL_LEVEL': checkSchoolLevel, 'COLLEGE_NAME': checkUniversityName,
                'WEBSITE': checkWebsite, 'BUILDING_CLASSIFICATION': checkBuildingClassification,
                'VEHICLE_TYPE': checkVehicleType, 'LOCATION_TYPE': checkTypeLocation,
                'PARK_PLAYGROUND': checkPark}

sc = SparkContext()
reader = sc.textFile(sys.argv[1]).map(splitText)
freqDict = dict()
for possible_type in relativeTypes:
    freqDict[possible_type] = 0
freqDict['Other'] = 0
resultRDD = reader.map(judgeSemanticType).reduceByKey(lambda x: x+y)
result = resultRDD.collect()
resultDict = dict()
for res in result:
    resultDict[res[0]] = res[1]
predictLabel = getTopTwoFreq(resultDict)
output = dict()

output['column_name'] = fileName[1]
output['semantic_types'] = [dict() for i in range(len(result))]
for i in range(len(result)):
    output['semantic_types'][i]['semantic_type'] = result[i][0]
    output['semantic_types'][i]['count'] = result[i][1]
output['predictLabel'] = predictLabel

rdd = spark.sparkContext.parallelize([json.dumps(output)])
rdd.saveAsTextFile(fileName[0]+fileName[1]+'.json')

'''



