import csv
import json

trueList = {}
with open('task2.csv', 'rU')as f:
    f_csv = csv.reader(f)
    for row in f_csv:
    	trueList[row[0]] = row[1]

with open('task2_type_count.json') as f:
	truecount = json.load(f)

rightcount = {}
predictcount = {}

for key in truecount:
	rightcount[key] = 0
	predictcount[key] = 0

with open('result.json') as f:
	predict = json.load(f)
	for item in predict:
		p_type = predict[item]['semantic_types']
		# if p_type == 'School Levels':
		# 	print(item)
		# 	print(trueList[item[:-4]])
		if p_type in predictcount:
			predictcount[p_type] += 1
		else:
			predictcount[p_type] = 1
		if trueList[item[:-4]] in p_type:
			if p_type in rightcount:
				rightcount[p_type] += 1
			else:
				rightcount[p_type] = 1

for t in truecount:
	if predictcount[t] == 0:
		print('The number of column predicted as ' + t + ' is 0' + '\n')
	else:
		print('precision of ' + t + ' is: ' + str(round(float(rightcount[t]) / predictcount[t], 2)))
		print('recall of ' + t + ' is: ' + str(round(float(rightcount[t]) / float(truecount[t]), 2)) + '\n')

count = 0
for item in rightcount:
	count += rightcount[item]
print(count, round(float(count) / 265, 2))
