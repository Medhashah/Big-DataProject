import os, json

files = os.listdir("../T1data")
ret = dict()
ret["datasets"] = list()
for file in files:

    with open('../T1data/'+file, 'r') as jsonFile:
        try:
            text = jsonFile.read()
            ret["datasets"].append(json.loads(text))
        except:
            pass

with open("task1.json", 'w') as f:
    json.dump(ret, f)
