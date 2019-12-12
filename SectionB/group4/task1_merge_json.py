import json
import os

import pandas as pd


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


path = "./task1_data/"
files = os.listdir(path)
output = dict()
output['datasets'] = []
for file in files:
    try:
        with open(path + file) as json_file:
            cur = json.load(json_file)
            output['datasets'].append(cur)
            cur = pd.read_json(path + '/' + file, typ='series')
    except:
        print(file + " error")
with open("./result/task1.json", 'w+') as fp:
    json.dump(output, fp, cls=MyEncoder)
