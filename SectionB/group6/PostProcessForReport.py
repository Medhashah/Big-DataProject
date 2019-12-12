import os
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import collections

def combine_json(path,root_name,new_file_name):
    files = os.listdir(path)
    new_dict = {}
    contentlist = []
    for file in files:
        with open(path + '/' + file) as f:
            jsoncontent = json.load(f)
        contentlist.append(jsoncontent)
    new_dict[root_name] = contentlist
    newjsondict = json.dumps(new_dict)
    f = open(new_file_name, 'w')
    f.write(newjsondict)
    f.close()

path1 = 'Result-Task1'
root_name1 = 'datasets'
new_file_name1 = 'task1.json'
combine_json(path1,root_name1,new_file_name1)

path2 = 'Result-Task2'
root_name2= 'presicted_types'
new_file_name2 = 'task2.json'
combine_json(path2,root_name2,new_file_name2)

files = os.listdir(path1)
integerCountList = []
realCountList = []
datetimeCountList = []
textCountList = []
totalTypeList = []
for file in files:
    with open(path1 + '/' + file) as f:
        jsoncontent = json.load(f)
    integerCount = 0
    realCount = 0
    datetimeCount = 0
    textCount = 0
    columnList = jsoncontent['columns']
    for column in columnList:
        types = set()
        dataTypeList = column['data_types']
        if len(dataTypeList) == 0:
            print(jsoncontent['dataset_name'])
            print(column['column_name'])
            print('-------------')
        for dataType in dataTypeList:
            typeName = dataType['type']
            types.add(typeName)
            if typeName == 'INTEGER(LONG)':
                integerCount += 1
            elif typeName == 'REAL':
                realCount += 1
            elif typeName == 'DATE/TIME':
                datetimeCount += 1
            else:
                textCount += 1
        totalTypeList.append(types)
    integerCountList.append(integerCount)
    realCountList.append(realCount)
    datetimeCountList.append(datetimeCount)
    textCountList.append(textCount)

print('Number of columns contain INTEGER(LONG) type:', sum(integerCountList))
print('Number of columns contain REAL type:', sum(realCountList))
print('Number of columns contain DATE/TIME type:', sum(datetimeCountList))
print('Number of columns contain TEXT type:', sum(textCountList))

plt.clf()
plt.hist(integerCountList, bins=50, color='#607c8e')
plt.title('Histogram for INTEGER/LONG type')
plt.xlabel('Number of columns with INTEGER(LONG) type')
plt.ylabel('Count of datasets')
plt.grid(axis='y', alpha=0.75)
plt.savefig('integer.png')

plt.clf()
plt.hist(integerCountList, bins=[0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100], color='#607c8e')
plt.title('Histogram for INTEGER/LONG type (# of columns <100)')
plt.xlabel('Number of columns with INTEGER(LONG) type (under 100)')
plt.ylabel('Count of datasets')
plt.grid(axis='y', alpha=0.75)
plt.savefig('integer_under100.png')

plt.clf()
plt.hist(realCountList, bins=50, color='#607c8e')
plt.title('Histogram for REAL type')
plt.xlabel('Number of columns with REAL type')
plt.ylabel('Count of datasets')
plt.grid(axis='y', alpha=0.75)
plt.savefig('real.png')

plt.clf()
plt.hist(datetimeCountList, bins=50, color='#607c8e')
plt.title('Histogram for DATE/TIME type')
plt.xlabel('Number of columns with DATE/TIME type')
plt.ylabel('Count of datasets')
plt.grid(axis='y', alpha=0.75)
plt.savefig('datetime.png')

plt.clf()
plt.hist(textCountList, bins=50, color='#607c8e')
plt.title('Histogram for TEXT type')
plt.xlabel('Number of columns with TEXT type')
plt.ylabel('Count of datasets')
plt.grid(axis='y', alpha=0.75)
plt.savefig('text.png')

plt.clf()
plt.hist(textCountList, bins=[0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100], color='#607c8e')
plt.title('Histogram for TEXT type (# of columns <100)')
plt.xlabel('Number of columns with TEXT type (under 100)')
plt.ylabel('Count of datasets')
plt.grid(axis='y', alpha=0.75)
plt.savefig('text_under100.png')

typeSet = []
for i in totalTypeList:
    if i == {'INTEGER(LONG)'}:
        typeSet.append('I')
    elif i == {'REAL'}:
        typeSet.append('R')
    elif i == {'DATE/TIME'}:
        typeSet.append('D')
    elif i == {'TEXT'}:
        typeSet.append('T')
    elif i == {'INTEGER(LONG)', 'REAL'}:
        typeSet.append('IR')
    elif i == {'INTEGER(LONG)', 'DATE/TIME'}:
        typeSet.append('ID')
    elif i == {'INTEGER(LONG)', 'TEXT'}:
        typeSet.append('IT')
    elif i == {'REAL', 'DATE/TIME'}:
        typeSet.append('RD')
    elif i == {'REAL', 'TEXT'}:
        typeSet.append('RT')
    elif i == {'DATE/TIME', 'TEXT'}:
        typeSet.append('DT')
    elif i == {'INTEGER(LONG)', 'REAL', 'DATE/TIME'}:
        typeSet.append('IRD')
    elif i == {'INTEGER(LONG)', 'REAL', 'TEXT'}:
        typeSet.append('IRT')
    elif i == {'INTEGER(LONG)', 'DATE/TIME', 'TEXT'}:
        typeSet.append('IDT')
    elif i == {'REAL', 'DATE/TIME', 'TEXT'}:
        typeSet.append('RDT')
    elif i == {'INTEGER(LONG)', 'REAL', 'DATE/TIME', 'TEXT'}:
        typeSet.append('IRDT')
    else:
        typeSet.append('Empty')

typeDict = dict((x,typeSet.count(x)) for x in set(typeSet))
sortedSetList = sorted(typeDict, key=len, reverse=False)
sortedDict = {}
for i in sortedSetList:
    sortedDict[i] = typeDict[i]
plt.clf()
plt.bar(sortedDict.keys(), sortedDict.values(), color='#607c8e')
plt.title('Frequent Itemsets')
plt.xlabel('Frequent Itemsets')
plt.ylabel('Count')
plt.grid(axis='y', alpha=0.75)
plt.savefig('frequent.png')

optimized_precision = [1.0, 1.0, 0.9166666666666666, 0.75, 1.0, 0.8846153846153846, 0.6363636363636364, 0.6666666666666666, 0.6666666666666666, 0.3333333333333333, 1.0, 0.5714285714285714, 0.6551724137931034, 0.9090909090909091, 0.5294117647058824, 1.0, 0.8, 0.7, 0.5, 1.0, 1.0, 1.0, 0.0, 0.4858490566037736]
optimized_recall = [0.8387096774193549, 0.8888888888888888, 1.0, 1.0, 1.0, 1.0, 0.9333333333333333, 1.0, 1.0, 0.5, 0.7857142857142857, 0.7619047619047619, 0.9047619047619048, 0.8333333333333334, 1.0, 0.9, 0.8421052631578947, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.9363636363636364]

original_precision = [1.0, 1.0, 0.9166666666666666, 1.0, 1.0, 1.0, 1.0, 1.0, 0.125, 0.7142857142857143, 1.0, 1.0, 0.8095238095238095, 0.9090909090909091, 0.0, 1.0, 0.8461538461538461, 0.7, 0.0, 1.0, 1.0, 1.0, 0.0, 0.584]
original_recall = [0.8387096774193549, 0.8888888888888888, 1.0, 0.7142857142857143, 1.0, 0.43478260869565216, 0.8666666666666667, 0.5909090909090909, 1.0, 0.5, 0.7857142857142857, 0.38095238095238093, 0.8095238095238095, 0.8333333333333334, 0.0, 0.9, 0.5789473684210527, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.6636363636363637]
semantic_type = ['Person_name', 'Business_name', 'City_agency', 'Neighborhood', 'Building_Classification', 'Areas_of_study',
                 'School_Levels', 'Borough', 'Subjects_in_school', 'Parks_Playgrounds', 'Zip_code', 'Address', 'Street_name',
                 'Phone_Number', 'City', 'LAT_LON_coordinates', 'School_name', 'Car_make', 'Vehicle_Type', 'Type_of_location',
                 'Websites', 'Color', 'College_University_names', 'Other']
avg_origin_precision = sum(original_precision)/len(original_precision)
avg_origin_recall = sum(original_recall)/len(original_recall)
avg_opt_precision = sum(optimized_precision)/len(optimized_precision)
avg_opt_recall = sum(optimized_recall)/len(optimized_recall)
improve_precision = avg_opt_precision - avg_origin_precision
improve_recall = avg_opt_recall - avg_origin_recall

plt.clf()
plt.figure(figsize=(10,8))
plt.plot(semantic_type, original_precision, color = 'b', label = 'Original')
plt.plot(semantic_type, optimized_precision, color = 'r', label = 'Optimized')
plt.title('Original precision vs. Optimized precision')
plt.xlabel('Semantic Type')
plt.ylabel('Precision')
plt.xticks(rotation=-90, fontsize=5)
plt.legend()
plt.savefig('precision.png')

plt.clf()
plt.figure(figsize=(10,8))
plt.plot(semantic_type, original_recall, color='b', label='Original')
plt.plot(semantic_type, optimized_recall, color='r', label='Optimized')
plt.title('Original recall vs. Optimized recall')
plt.xlabel('Semantic Type')
plt.ylabel('Recall')
plt.xticks(rotation=-90, fontsize=5)
plt.legend()
plt.savefig('recall.png')

files = os.listdir(path2)
semanticTypeCountList = []
for file in files:
    with open(path2 + '/' + file) as f:
        jsoncontent = json.load(f)
    semanticTypeList = jsoncontent['semantic_types']
    semanticTypeCountList.append(len(semanticTypeList))

semanticDict = dict((x,semanticTypeCountList.count(x)) for x in semanticTypeCountList)
semanticDict = collections.OrderedDict(sorted(semanticDict.items()))
plt.clf()
plt.bar(semanticDict.keys(), semanticDict.values(), color='#607c8e')
plt.title('Prevalence of heterogeneous columns')
plt.xlabel('Number of semantic types in the column')
plt.ylabel('Count of columns')
plt.grid(axis='y', alpha=0.75)
plt.savefig('heterogeneous.png')