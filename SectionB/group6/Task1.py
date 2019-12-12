import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, DecimalType, DateType, StringType, LongType, TimestampType, BooleanType
import re
from datetime import datetime
import json
import dateutil.parser
import sys
import traceback

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("Project-Task1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

# Read file list from file path
filePath = '/user/hm74/NYCOpenData'
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
fileStatusList = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(filePath))
fileInfoList = []
for fileStatus in fileStatusList:
    fileInfoList.append((fileStatus.getPath().getName(), fileStatus.getLen()))

# Remove datasets.csv from the list
fileNameList = [fileInfo[0] for fileInfo in fileInfoList]
datasetInfoIndex = fileNameList.index('datasets.tsv')
del fileInfoList[datasetInfoIndex]
# Sort the file list by file size in ascending order
fileInfoList.sort(key = lambda s: s[1])

# Load file by fileName using spark.sql.dataFrame
def loadData(fileName):
    dataFrame = spark.read.csv(filePath + '/' + fileName, header='true', inferSchema='true', multiLine='true', sep='\t')
    header = dataFrame.columns
    data = dataFrame.rdd.map(tuple)
    # file = sc.textFile(filePath + '/' + fileName)
    # header = file.first().split('\t')
    # data = file.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda x: x[0]).map(lambda x: x.split('\t'))
    return header, data, dataFrame

# Get value in string type by index i
def getValueByIndex(x, i):
    if i >= len(x) or x[i] is None:
        return ''
    else:
        return str(x[i])

# Detect data type
def addDataType(dataType, x):
    # Regular expression for integer and real type
    integer_expr = r'^[+-]?([1-9]\d*|0)$'
    integer_expr_comma = r'^[+-]?[1-9]\d{0,2}(,\d{3})*$'
    real_expr = r'[+-]?(\d+\.\d*|\d*\.\d+)$'
    real_expr_comma = r'^[+-]?[1-9]\d{0s,2}(,\d{3})*\.\d*$'
    if x == None or x.strip() == '':
        return ('TEXT', x, 0)
    elif dataType == 'INTEGER(LONG)':
        return (dataType, x, int(x))
    elif dataType == 'REAL':
        return (dataType, x, float(x))
    elif dataType == 'DATE/TIME':
        return (dataType, x, dateutil.parser.parse(x, ignoretz = True, default = datetime(1900, 1, 1, 0, 0, 0, 000000)))
    elif (re.match(integer_expr, x)):
        return ('INTEGER(LONG)', x, int(x))
    elif (re.match(integer_expr_comma, x)):
        return ('INTEGER(LONG)', x, int(x.replace(',', '')))
    elif (re.match(real_expr, x)):
        return ('REAL', x, float(x))
    elif (re.match(real_expr_comma, x)):
        return ('REAL', x, float(x.replace(',', '')))
    else:
        try:
            new_x = dateutil.parser.parse(x, ignoretz = True, default = datetime(1900, 1, 1, 0, 0, 0, 000000))
            return ('DATE/TIME', x, new_x)
        except:
            pass
        try:
            new_x = datetime.strptime(x+'M', '%I%M%p')
            return ('DATE/TIME', x, new_x)
        except:
            pass
        return ('TEXT', x, len(x))

# Process each column
def processColumn(header, data, dataFrame):
    columnNumber = len(header)
    columnList = []
    tableKeyList = []
    for i in range(columnNumber):
        columnDict = {}
        columnName = header[i]
        columnDict['column_name'] = columnName
        # Get the values for each column
        columnValue = data.map(lambda x: getValueByIndex(x, i))
        nonEmptyCellNumber = columnValue.filter(lambda x: x.strip() != '').count()
        columnDict['number_non_empty_cells'] = nonEmptyCellNumber
        emptyCellNumber = columnValue.filter(lambda x: x.strip() == '').count()
        columnDict['number_empty_cells'] = emptyCellNumber
        valueNumber = columnValue.count()
        distinctValueNumber = columnValue.distinct().count()
        columnDict['number_distinct_values'] = distinctValueNumber
        # Detect table key when all the values are distinct
        if valueNumber == distinctValueNumber:
            tableKeyList.append(columnName)
        top5FrequentValues = columnValue.map(lambda x: (x, 1)).reduceByKey(lambda x1, x2: x1 + x2).takeOrdered(5, key=lambda x: -x[1])
        columnDict['frequent_values'] = list(map(lambda x: x[0], top5FrequentValues))
        dfDataType = dataFrame.schema[i].dataType
        # The map for dataFrame dataType and the type we use for this task
        typeMap = {IntegerType: 'INTEGER(LONG)',
                   LongType: 'INTEGER(LONG)',
                   DecimalType: 'REAL',
                   DoubleType: 'REAL',
                   DateType: 'DATE/TIME',
                   TimestampType: 'DATE/TIME',
                   BooleanType: 'TEXT',
                   StringType: 'TEXT'}
        dataType = typeMap[type(dfDataType)]
        # Add type according to the type result after mapping and the value
        columnWithType = columnValue.map(lambda x: addDataType(dataType, x))
        # Get the type list of this column and the count for each type
        typeList = columnWithType.keys().distinct().collect()
        typeCount = columnWithType.countByKey()
        # Calculate max, min and other values for each type
        dataTypeList = []
        for columnType in typeList:
            dataTypeDict = {}
            dataTypeDict['type'] = columnType
            dataTypeDict['count'] = typeCount[columnType]
            # Filter column with type
            typeRow = columnWithType.filter(lambda x: x[0] == columnType)
            # Get the value use for comparison, for TEXT type it is the length of the string
            typeValue = typeRow.map(lambda x: x[2])
            if columnType == 'TEXT':
                top5LongestText = typeRow.takeOrdered(5, key=lambda x: -x[2])
                top5ShortestText = typeRow.takeOrdered(5, key=lambda x: x[2])
                avgLength = typeValue.sum() / typeValue.count()
                dataTypeDict['shortest_values'] = list(map(lambda x: x[1], top5ShortestText))
                dataTypeDict['longest_values'] = list(map(lambda x: x[1], top5LongestText))
                dataTypeDict['average_length'] = avgLength
            else:
                maxVal = typeValue.max()
                minVal = typeValue.min()
                dataTypeDict['max_value'] = typeRow.filter(lambda x: x[2] == maxVal).map(lambda x: x[1]).max()
                dataTypeDict['min_value'] = typeRow.filter(lambda x: x[2] == minVal).map(lambda x: x[1]).min()
                if columnType != 'DATE/TIME':
                    dataTypeDict['mean'] = typeValue.mean()
                    dataTypeDict['stddev'] = typeValue.stdev()
            # Add information for each type
            dataTypeList.append(dataTypeDict)
        columnDict['data_types'] = dataTypeList
        columnList.append(columnDict)
    return columnList, tableKeyList

# Write the dictionary for each file into a JSON file
def writeToJson(fileName, dataDict):
    jsondict = json.dumps(dataDict)
    f = open(fileName[0:9]+'.json', 'w')
    f.write(jsondict)
    f.close()

# Code for get the value for date/time type to check their format
def dateTimeTest(fileNameList):
    dateTimeList = []
    for k, fileName in enumerate(fileNameList):
        dataFrame = spark.read.csv(filePath + '/' + fileName, header='true', inferSchema='true', sep='\t')
        schema = dataFrame.schema
        length = len(schema)
        for i in range(length):
            columnType = schema[i].dataType
            columnName = dataFrame.columns[i]
            if columnType == DateType or columnType == TimestampType or 'date' in columnName.lower() or 'time' in columnName.lower():
                dataList = dataFrame.select(columnName).take(5)
                for d in dataList:
                    dateTimeList.append(d[columnName])


for file in fileInfoList:
    try:
        fileName = file[0]
        print('Start:',fileName)
        dataDict = {}
        dataDict['dataset_name'] = fileName
        header, data, dataFrame = loadData(fileName)
        columnList, tableKeyList = processColumn(header, data, dataFrame)
        dataDict['columns'] = columnList
        dataDict['key_column_candidates'] = tableKeyList
        writeToJson(fileName, dataDict)
        print('-----',fileName,'Finished ! -----')
    except Exception as ex:
        print('-----',fileName,'Failed ! -----')
        # Get current system exception
        ex_type, ex_value, ex_traceback = sys.exc_info()
        # Extract unformatter stack traces as tuples
        trace_back = traceback.extract_tb(ex_traceback)
        # Format stacktrace
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(
                "File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
        print("Fail %s----------------------------------------\n" % fileName)
        print("Exception type : %s \n" % ex_type.__name__)
        print("Exception message : %s \n" % ex_value)
        print("Stack trace : %s \n" % stack_trace)
        print("----------------------------------------\n")
        pass

sc.stop()