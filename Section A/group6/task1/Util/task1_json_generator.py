import os
import simplejson as json

directory_in_str = '../raw_output'

final_json = []
for file in os.listdir(directory_in_str):
	filename = os.fsdecode(file)
	if filename.endswith(".json"):
		with open(directory_in_str + '/' + filename) as f:
			obj = json.load(f)

			resulting_list = []
			for col in obj['columns']:
				resulting_list.append(obj['columns'][col])

			obj['columns'] = resulting_list

			final_json.append(obj)

outfile = open('../deliverable/task1.json', "w")
outfile.write(json.dumps(final_json, indent=4))
outfile.close()