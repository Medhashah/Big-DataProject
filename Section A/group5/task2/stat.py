import pandas as pd
import json
import os

df = pd.ExcelFile('task2_true_label.xlsx').parse('Sheet 1 - task2_label copy') #you could add index_col=0 if there's an index

new_df = df[['Dataset','Label']]
label_map = dict()
for i in range(len(new_df)):
	dataset = new_df.loc[i]['Dataset'].strip('\'')
	f1,f2 = dataset.split('.', maxsplit=1)
	f2 = f2.split('.',maxsplit=1)[0]
	label = new_df.loc[i]['Label']
	if label not in label_map:
		label_map[label] = set()
	label_map[label].add((f1,f2))

output_files = os.listdir('results')
predicted_label_map = dict()
for file in output_files:
	f1,f2 = file.split('_',maxsplit=1)
	f2 = f2.split('.')[0]

	with open('results/{}'.format(file)) as f:
		data = json.load(f)
		for item in data['columns'][0]['semantic_types']:
			label = item['semantic_type']
			if label not in predicted_label_map:
				predicted_label_map[label] = set()
			predicted_label_map[label].add((f1,f2))

# this is total columns of type (number of columns belong to a single type)
total_columns_to_type = {item[0]:len(item[1]) for item in label_map.items()}
# this is total columns predicted as type. (number of columns belong to a single type after prediction)
total_columns_to_type_predicted = {item[0]:len(item[1]) for item in predicted_label_map.items() if item[0] in label_map}

# need to computer correctly predicted as type
correctly_columns_predicted = dict()
for label in total_columns_to_type_predicted:
	cnt = 0
	for dataset in predicted_label_map[label]:
		# if this dataset is also in label_map (ground true), then this dataset is correctly labeled
		if dataset in label_map[label]:
			cnt +=1
	correctly_columns_predicted[label] = cnt

# precision = correctly predict / total predict
# recall = correctly predict / total in reality
output = []
for item in total_columns_to_type.items():
	label = item[0]
	total_columns_count = item[1]
	# assign correctly columns predicted
	if label not in correctly_columns_predicted:
		correctly_label = 0
	else:
		correctly_label = correctly_columns_predicted[label]

	# assign total columns predicted as type
	# if label not in total_columns_to_type_predicted:
	# 	total_columns_predicted_count = 2
	# else:
	# 	total_columns_predicted_count = total_columns_to_type_predicted[label]
	total_columns_predicted_count = total_columns_to_type_predicted[label]

	precision = correctly_label / total_columns_predicted_count
	recall = correctly_label / total_columns_count

	output.append({label:[{'precision':precision},{'recall': recall}]})

# print(output)
res = {'results': output}
with open('scores.json', 'w') as f:
	json.dump(res, f, indent=2)





	

