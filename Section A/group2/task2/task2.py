import sys
import pyspark
import string
import json
import re
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

rowsNum = 0
acturalLabel = {}
correctPred = {}
predictLabel = {}

def editDis(str1, str2):
    len_str1 = len(str1) + 1
    len_str2 = len(str2) + 1
    #create matrix
    matrix = [0 for n in range(len_str1 * len_str2)]
    #init x axis
    for i in range(len_str1):
        matrix[i] = i
    #init y axis
    for j in range(0, len(matrix), len_str1):
        if j % len_str1 == 0:
            matrix[j] = j // len_str1
        
    for i in range(1, len_str1):
        for j in range(1, len_str2):
            if str1[i-1] == str2[j-1]:
                cost = 0
            else:
                cost = 1
            matrix[j*len_str1+i] = min(matrix[(j-1)*len_str1+i]+1,
                                        matrix[j*len_str1+(i-1)]+1,
                                        matrix[(j-1)*len_str1+(i-1)] + cost)      
    return matrix[-1]

def cosSim(str1, str2):
    edit_distance = editDis(str1, str2)
    edit_distance_similarity=1 - float(edit_distance) / max(len(str1), len(str2))
    return edit_distance_similarity
#zip code
zipPat = r'^[0-9]{5}(?:-[0-9]{4})?$'
#phone number 
phonePatList = [r'^[2-9]\d{2}-\d{3}-\d{4}$', r'((\(\d{3}\) ?)|(\d{3}-))?\d{3}-\d{4}'\
    r'^(1?(-?\d{3})-?)?(\d{3})(-?\d{4})$']
#website
webSitePat = r'^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$'
#brouugh
boroughList = ['k', 'm', 'q', 'r', 'x', 'new york', 'brooklyn', 'manhattan', 'queens', 'bronx', 'the bronx', 'staten island', 'clifto', \
    'baldwin', 'astoria','mt.kisco', 'charlotte', 'bklyn', 'dobbs ferry', 'staten island', 'elmhurst', 'maspeth', 'nyc']
#car make
carMakeList = ['peter', 'inter', 'chevr', 'nissa', 'workh', 'acura', 'alfa romeo', 'aston martin', 'audi', 'bentley', \
               'bmw', 'bugatti', 'buick', 'cadillac', 'chevrolet',  'datsun', 'olds', 'chev', 'chrysl', 'mercur'\
    'chrysler', 'citroen', 'dodge', 'ferrari', 'fiat', 'ford', 'geely', 'general motors', 'gmc', 'honda', 'hyundai', \
        'infiniti', 'jaguar', 'jeep', 'kia', 'koenigsegg', 'lamborghini', 'land rover', 'lexus', 'masrati', \
            'mazda', 'mclaren', 'mercedes benz', 'mercedes-benz', 'mini', 'mitsubishi', 'nissan', 'pagani', 'peugeot', 'porsche', \
                'ram', 'renault', 'rolls royce', 'saab', 'subaru', 'suzuki', 'tata motors', 'tesla', 'chevy', 'chev',\
                    'toyota', 'volkswagen', 'volvo', 'cadi', 'unk', 'pont', 'hobbs', 'pontia', 'linc', 'plym', 'lincol',\
              'v.w', 'vw', 'gm', 'volks']
#color 
colorList = ['gry', 'gr', 'blk',  'orang', 'yellow', 'blue', 'red', 'green', 'black', 'brown', 'azure', 'ivory', 'teal', \
    'silver', 'purple', 'navy blue', 'navy', 'pea green', 'gray', 'orange', 'maroon', 'charcoal', 'aquamarine', 'coral', 'aquamarine', 'coral', \
        'fuchsia', 'wheat', 'lime', 'crimson', 'khaki', 'hot pink', 'megenta', 'olden', 'plum', 'olive', 'cyan', 'tan', 'biege',\
            'bl', 'bk', 'gy', 'wht', 'wh', 'gy', 'ltg', 'white', 'rd', 'silve', 'silvr', 'tn', 'gray', 'yw', 'dkg', 'grn', 'brn']
#business name,
businessList = ['market', 'pizza', 'restaurant', 'kitchen', 'shop', 'cafe', 'sushi', 'panda', 'noodle',\
               'bar', 'deli', 'hotel', 'service', 'pub', 'transportation', 'svce', 'cars', 'line']
#person name
personNamePat = r"^[a-z ,.'-]+$" 
#vehicle type
vehicleTypeList = ['station wagon/sport utility vehicle', 'ambulance', 'boat', 'trailer', 'motorcycle', 'bus', 'taxi', 'van', 'sedan', 'truck', \
                   'box truck', 'passenger vehicle', 'sport utility / station wagon', 'beverage truck', 'garbage or refuse', 'pick-up truck',\
                  'motorcycle', 'pk', 'tractor truck diesel', 'flat bed', 'bike']
#parks/playgrounds
ppPat = r"([a-zA-Z0-9]{1,10} ){1,5}(park|playground)$"
#street name
streetPat = r"([a-zA-Z0-9]{1,10} ){1,2}(place|avenue|ave|ave\.|court|ct|street|st|drive|dr|lane|ln|road|rd|blvd|plaza|parkway|pkwy)$"
#type of location
typeLocationList = ['abandoned building', 'airport terminal', 'airport', 'bank', 'church', 'clothing', 'boutique']
#lat/lon coordinates
latLonCoordPat = r"^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$"
latLonCoordPat2 = r"^(([1-9]\d?)|(1[0-7]\d))(\.\d{3,7})|180|0(\.\d{1,6})?"
#address
addressPat = r"^\d+?[A-Za-z]*\s\w*\s?\w+?\s\w{2}\w*\s*\w*"
#school name
school_pat = r"([a-z0-9]{1,10} ){1,5}(high school|middle school|studies|charter school|elementary school|(school|academy){1} (of|for){1} ([a-z0-9]{1,10}){1,5})$"
#school level
schoollevel_list = ['k-1', 'k-2', 'k-3', 'k-4','k-5','k-6','k-7','k-8','k-9','k-10','k-11','k-12',\
    'elementary', 'elementary school', 'primary', 'primary school', 'high school', 'middle', 'middle school', 'high school transfer', 'yabc', \
        'senior high school', 'college']
#area_of_study
area_of_study_list = ['business', 'health professions', 'law & government', 'science & math',\
                      'architecture','visual art & design', 'engineering', 'film/video', 'hospitality, travel, & tourism',\
                      'environmental science', 'communications', 'teaching', 'jrotc', 'zoned', 'animal science']
#subject_in_school
subject_in_school_list = ['english', 'math', 'science', 'social studies']
#colledge name list
colledgeList = ['university', 'college']

#building_classification
buildingClassificationPat = r"[a-zA-Z][0-9]{1,2}-(walk-up|elevator|condops)"

#city names dict, which will be loaded in main function
cityDict = {}
#city agency list
agencyDict = {}
#neighborhood List
neighborhoodDict = {}
#last name list
lastNameDict = {}
#first name list
firstNameDict = {}

def semanticMap(x):
    mat = str(x[0])
    lowerMat = mat.lower()
    #city
    if lowerMat in cityDict:
        return ('city', x[1])
    #city agency
    if lowerMat in agencyDict:
        return ('city_agency', x[1])
    #college name
    for c in colledgeList:
        if lowerMat.find(c) >= 0:
            return ('college_name', x[1])
    #type of location
    # if lowerMat in typeLocationList:
    #     return ('location_type', x[1])
    #vehicle type
    if lowerMat in vehicleTypeList:
        return ('vehicle_type', x[1])
    #school level
    if lowerMat in schoollevel_list:
        return ('school_level', x[1])
    #business name
    for business in businessList:
        if lowerMat.find(business) >= 0:
            return ('business_name', x[1])
    #area_of_study_list
    if lowerMat in area_of_study_list:
        return ('area_of_study', x[1])
    #color 
    if lowerMat in colorList:
        return ('color', x[1])
    # for color in colorList:
    #     if cosSim(color, lowerMat ) >= 0.8:
    #         return ('color', x[1])
    #zip code
    if re.match(zipPat, lowerMat):
        return ('zip_code', x[1])
    #buildingClassification
    if re.match(buildingClassificationPat, lowerMat):
        return ('building_classification', x[1])
    #website
    if re.match(webSitePat, lowerMat):
        return ('website', x[1])
    latlonMat = lowerMat.replace(")","").replace("(","").replace(" ","")
    #lat/lon coordinates
    if re.match(latLonCoordPat, latlonMat):
        return ('lat_lon_cord', x[1])
    llMat = lowerMat.replace("+","").replace("-","")
    if re.match(latLonCoordPat2, llMat):
        return ('lat_lon_cord', x[1])
    #phone number 
    for pat in phonePatList:
        if re.match(pat, lowerMat):
            return ('phone_number', x[1])  
    #school name
    if re.match(school_pat, lowerMat):
        return ('school_name', x[1])     
    #parks/playgrounds
    if re.match(ppPat, lowerMat):
        return ('park_playground', x[1]) 
    #street name
    if re.match(streetPat, lowerMat):
        return ('street_name', x[1])
    #address
    if re.match(addressPat, lowerMat):
        return ('address', x[1]) 
    #borough
    # if  lowerMat in boroughList:
    #     return ('Borough', x[1])
    for borough in boroughList:
        if cosSim(borough, lowerMat) >= 0.8:
            return ('borough', x[1])
    #car make
    # if lowerMat in carMakeList:
    #     return ('Car make', x[1])
    for carMake in carMakeList:
        if cosSim(carMake, lowerMat) >= 0.8:
            return ('car_make', x[1])
    for subj in subject_in_school_list:
        if cosSim(subj, lowerMat) >= 0.8:
            return ('subject_in_school', x[1])
    #neighborhood
    if lowerMat in neighborhoodDict:
        return ('neighborhood', x[1])
    for nei in neighborhoodDict.keys():
        if (cosSim(lowerMat, nei)) >= 0.8:
            return ('neighborhood', x[1])
    #person name
    if lowerMat in lastNameDict or lowerMat in firstNameDict:
        return ('person_name', x[1])
    return ('other', x[1])

emptyWordList = ["", "-", "no data", "n/a", "null", "na", "unspecified"]
perList = []
exceptList = []

def checkEmpty(x):
    if not x[0]:
        return ('number_empty_cells',x[1])
    mat = str(x[0])
    if mat in emptyWordList:
        return ('number_empty_cells',x[1])
    return ('number_non_empty_cells',x[1])

def outErrorList(i):
    erlst = []
    print('processing file index {} excepted'.format(i))
    if os.path.isfile("./errorList2.txt"):
        with open('./errorList2.txt', 'r', encoding='UTF-8') as f:
            erStr = f.readlines()
            for line in erStr:
                line = line.replace("\n","")
                erlst.append(line)
        if str(i) not in erlst:
            with open('./errorList2.txt', 'a', encoding='UTF-8') as f:
                line = str(i)+"\n"
                f.write(line)
    else:
        with open("./errorList2.txt", 'w') as f:
            for i in exceptList:
                line = str(i)+"\n"
                f.write(line)

# this is the precision of each column
def outPerList(i, p):
    plst = []
    if os.path.isfile("./col_precision.txt"):
        with open("./col_precision.txt", 'r', encoding='UTF-8') as f:
            pStr = f.readlines()
            for line in pStr:
                line = line = line.split(" ")[0]
                plst.append(line)
        with open("./col_precision.txt", 'a', encoding='UTF-8') as f:
            if str(i) not in plst:
                line = str(i) + " " + str(p)+"\n"
                f.write(line)
    else:
        with open("./col_precision.txt", 'w', encoding='UTF-8') as f:
            for j in range(len(perList)):
                line = str(perList[j][0]) + " " + str(perList[j][1])+"\n"
                f.write(line)
    print('finised output precision for one column')

# output predict info
def outDict(fp, dct):
    with open(fp, 'w') as f:
        json.dump(dct,f, sort_keys= True, indent = 4, separators=(',', ': '))
if __name__ == "__main__":
    directory = "/user/hm74/NYCOpenData"
    outDir = "./task2out"
    labelList = []
    sc = SparkContext()
    fileLst = []
    perList = []
    ### label list
    with open('./labellist.txt', 'r') as f:
        labels = f.readlines()
        for label in labels:
            label = label.replace("\n","")
            llst = label.split(" ")[1].split(",")
            labelList.append(llst)
            for l in llst:
                if l not in acturalLabel:
                    acturalLabel[l] = 1
                else:
                    acturalLabel[l] += 1
        outDict("./actural_label.json", acturalLabel)
    ### cluster
    with open('./cluster1.txt', 'r') as f:
        contentStr = f.read()
        fileLst = contentStr.replace('[',"").replace(']',"").replace("'","").replace("\n","").split(', ')
    ### city names list
    with open('./citylist.txt', 'r', encoding='utf-8') as f:
        cityNames = f.readlines()
        for cityName in cityNames:
            cityDict[cityName.replace("\n","").strip().lower()] = 1
    print("Loaded {} city names".format(len(cityDict.keys())))
    ### city agencies list
    cityAgencyDir = "./cityagencylist.txt"
    with open(cityAgencyDir, 'r', encoding='utf-8') as f:
        agencys = f.readlines()
        for agency in agencys:
            if agency.find("(") >= 0:
                agencyL = agency.split("(")
                for a in agencyL:
                    agencyDict[a.strip().replace(")","").lower()] = 1
    print("Loaded {} city agency names(Abbreviations and full names)".format(len(agencyDict.keys())))
    ### neighborhood list
    with open('./neighborhood.txt', 'r', encoding='utf-8') as f:
        neighborhoodList = f.readlines()
        for neighborhood in neighborhoodList:
            neighborhoodDict[neighborhood.replace("\n","").strip().lower()] = 1
    print("Loaded {} neighborhood names".format(len(neighborhoodDict.keys())))
    ### last name list
    with open('./commonlastname.txt', 'r') as f:
        lastNameList = f.readlines()
        for lastName in lastNameList:
            lastName = lastName.split("\t")[0].lower()
            lastNameDict[lastName] = 1
    print("Loaded {} common last names".format(len(lastNameDict.keys())))
    ### first name list
    with open('./commonfirstname.txt', 'r') as f:
        firstNameList = f.readlines()
        for firstName in firstNameList:
            firstName = firstName.split("\t")[0].lower()
            firstNameDict[firstName] = 1
    print("Loaded {} common first names".format(len(firstNameDict.keys())))
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    fNum = len(fileLst)
    for i in range(0, fNum):
        try:
            fileInfo = fileLst[i]
            fStr = fileInfo.split(".")
            fileName = fStr[0]
            colName = fStr[1] 
            print('*'*50)
            print('Processing file: {} with column: {}, current step: {}/{}'.format( \
                fileName, colName, i+1, fNum))
            outputDicts = {} 
            outputDicts['dataset_name'] = fileName 
            outputDicts['column_name'] = colName  
            outputDicts['actual_types'] = labelList[i]            
            outputDicts['semantic_types'] = []
            filePath = directory + "/" + fileName +".tsv.gz"
            fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t', encoding = 'UTF-8', multiLine = True).load(filePath)
            columns = fileDF.columns
            if colName not in columns:
                if colName.find('CORE_SUBJECT') >= 0:
                    colName = 'CORE SUBJECT'
                else:
                    colName = colName.replace("_", " ") 
                print('Renamed selected column name')
            if colName not in columns:
                if colName == 'CORE SUBJECT':
                    for c in columns:
                        if c.find(colName) >= 0:
                            colName = c
                            print('Renamed selected column name')
                            break
                else:
                    for c in columns:
                        if cosSim(colName, c) >=0.8:
                            colName = c
                            print('Renamed selected column name')
                            break
            fileRDD = fileDF.select(colName).rdd.filter( \
                lambda x: (x[colName] is not None and str(x[colName]).lower() not in emptyWordList)).cache()
            print('Finished selecting column from dataframe: {}'.format(fileName))
            rddCol = fileRDD.map(lambda x: (x[colName], 1))
            disRDD = rddCol.reduceByKey(lambda x,y:(x+y))
            rowsNum = len(disRDD.collect())
            sRDD = disRDD.map(lambda x: semanticMap(x))
            SemRDD = sRDD.reduceByKey(lambda x,y:(x+y))
            SemList = SemRDD.collect()
            correctCnt = 0
            neCnt = 0
            SemList.sort(key=lambda x:x[1], reverse=True)
            semCnt = 0
            for sem in SemList:
                semCnt += 1
                if sem[0] == 'other':
                    outputDicts['semantic_types'].append({
                    'semantic_type': sem[0],
                    'label': labelList[i],
                    'count': int(sem[1])
                    })
                else:
                    outputDicts['semantic_types'].append({
                        'semantic_type': sem[0],
                        'count': int(sem[1])
                    })
                neCnt += int(sem[1])
                if sem[0] in labelList[i]:
                        correctCnt += sem[1]
                if semCnt <= len(labelList[i]):              
                    if sem[0] not in predictLabel:
                        predictLabel[sem[0]] = 1
                    else:
                        predictLabel[sem[0]] += 1
                    if sem[0] in labelList[i]:
                        if sem[0] not in correctPred:
                            correctPred[sem[0]] = 1
                        else:
                            correctPred[sem[0]] += 1    
            with open(outDir+"/"+fileName+"_"+fStr[1]+".json", 'w', encoding='UTF-8') as fw:
                json.dump(outputDicts,fw,indent=1)
            print('Finished output file: {}, the index is: {}'.format(fileName, i))
            per = float(correctCnt)/neCnt
            perList.append((i, per))
            outPerList(i, per)
        except Exception as e:
            exceptList.append(i)
            outErrorList(i)
    outDict("./correct_predict.json", correctPred)
    outDict("./predict_label.json", predictLabel)
    #### output precision and recall
    allTypes = 0
    for label in acturalLabel.keys():
        allTypes += acturalLabel[label]
    finalPerRecall = {}
    for label in acturalLabel.keys():
        if label in correctPred:
            finalPerRecall[label] = {
                'precision': float(correctPred[label])/predictLabel[label],
                'recall': float(correctPred[label])/acturalLabel[label]
            }
        else:
            finalPerRecall[label] = {
                'precision': 0.0,
                'recall': 0.0
            }
    outDict('final_precision_recall.json', finalPerRecall)