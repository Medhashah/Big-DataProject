import json
import os
import pandas as pd


output_files = os.listdir('results')

res = []
for file in output_files:
	with open('results/{}'.format(file)) as f:
		data = json.load(f)
		res.append(data['columns'][0])

output = {'predicted_types': res}

with open('task2.json','w') as f:
	json.dump(output, f, indent=2)


df = pd.ExcelFile('task2_true_label.xlsx').parse('Sheet 1 - task2_label copy')
new_df = df[['Dataset','Label']]

res = []
for i in range(len(new_df)):
	dataset = new_df.loc[i]['Dataset'].strip('\'')
	f1,f2 = dataset.split('.', maxsplit=1)
	colname = f2.split('.',maxsplit=1)[0]
	label = new_df.loc[i]['Label']
	res.append({'column_name':colname, 'manual_labels':[{'semantic_type':label}]})

output = {'actual_types': res}
with open('task2-manual-labels.json','w') as f:
	json.dump(output, f, indent=2)
