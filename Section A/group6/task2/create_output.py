from os import listdir
from os.path import isfile, join
import pprint
import json
import re
data = []
labels = []
multi = []
label_array = ["lastname","firstname","middlename","fullname",
                   "businessname","phone","address","street",
                   "city","neighborhood","coordinates","zip","boro",
                   "schoolname","color","make","agency","study","subject",
                   "level","college","university","website","building","type","location",
                   "park","playground","dba","interest"
                  ]
                  
def link(label):
    if label == "lastname" or label =="firstname" or label =="middlename" or label == "fullname":
        return "Person name"
    if label == "coordinates" or label =="location":
        return "LAT/LON coordinates"
    if label == "businessname" or label == "dba":
        return "Business name"
    if label == "neighborhood":
        return "Neighborhood"
    if label == "phone":
        return "Phone Number"
    if label == "address":
        return "Address"
    if label == "street":
        return "Street name"
    if label == "city":
        return "City"
    if label == "zip":
        return "Zip code"
    if label == "schoolname":
        return "School name"
    if label == "color":
        return "Color"
    if label == "make":
        return "Car make"
    if label == "agency":
        return "City agency"
    if label == "website":
        return "Websites"
    if label == "boro":
        return "Borough"
    if label == "subject":
        return "Subjects in school" 
    if label == "study" or label =="interest":
        return "Areas of study"
    if label == "level":
        return "School Levels"
    if label == "college" or label == "university":
        return "College/University names"
    if label == "building":
        return "Building Classification"
    if label == "type":
        return "Vehicle Type"
    if label == "location":
        return "Type of location"
    if label == "park" or label =="playground":
        return "Parks/Playgrounds"
    if label == "other":
        return "Other"
    return label



main_directory = "./labelled_data/"
log = []
# get all files in the labelled_data folder
onlyfiles = [f for f in listdir(main_directory) if isfile(join(main_directory, f))]
for i in range(0,len(onlyfiles)):
    self_out = {"column_name":0,"semantic_types":None}
    item = main_directory + onlyfiles[i] 
    print("this is the " + str(i)+" item out of "+str(len(onlyfiles))+" items")
    fileName = item # file name
    item = item.replace(".txt","").replace(main_directory,"").strip()
    vp = item.split("_",1)
    table = vp[0] # table name e.g. abjx-bcde
    column = vp[1] # column name e.g. SCHOOL_LEVEL

    ################################################
    # predict file label here
    # TODO: use more strategy to identify other labels
    predict_list = [] # could be multiple labels

    # strategy 3: merged strategy
    processed_name = column.replace("_","").replace(".","").replace("-","").strip().lower()
    for l in label_array:
        if l in processed_name:
            predict_list.append(link(l))
    sf = open(fileName, 'r')
    line = sf.readline()
    sf.close()
    line = line.replace("',","\t")
    keyword = line.split("\t")[1].replace("'","")
    if re.search("^(.*[0-9]*.*[0-9]*,.*[0-9]*.*[0-9]*)$",keyword):
        if link("coordinates") not in predict_list:
            predict_list.append(link("coordinates"))
    if re.search("[0-9]{5}",keyword):
        if link("zip") not in predict_list:
            predict_list.append(link("zip"))
    if re.search("[0-9]{3}.*[0-9]{3}.*[0-9]{4}",keyword):
        if link("phone") not in predict_list:
            predict_list.append(link("phone"))
    if len(predict_list) <= 0:
        if link("other") not in predict_list:
            predict_list.append(link("other"))

    #################################################
    # get key_list, the actual labels of the column
    fp = open(fileName,"r")
    line = fp.readline()
    key_list = {}
    while line:
        try:
            l_key = line.split(",")[0].replace("'","").strip()
            l_value = int(line.split(", ")[-1].strip())
            #TODO: l_key needs to be updated using the script in get_column_names
            if l_key == "Business Name":
                l_key = "Business name"
            if l_key == "Last name" or l_key =="First Name" or l_key == "First name" or l_key =="Middle name" or l_key == "Full name":
                l_key = "Person name"
            if l_key == "Letter":
                l_key = "Other"
            if l_key == "Park":
                l_key = "Parks/Playgrounds"
            if l_key == "Phone number":
                l_key = "Phone Number"
            if l_key == "School levels":
                l_key = "School Levels"
            if l_key == "other":
                l_key = "Other"
            if l_key not in key_list:
                key_list[l_key] = l_value # add to the key_list array
            else:
                key_list[l_key] += l_value
            line = fp.readline()
        except Exception:
            line = fp.readline()
    fp.close()
    ################################################
    self_out['column_name'] = table+"_"+column
    self_out['semantic_types'] = {}
    for item in key_list:
        #colname = item
        colvalue = key_list[item]
        self_out['semantic_types']['semantic_type'] = predict_list[0]
        #self_out['semantic_types']['label'] = colname
        self_out['semantic_types']['count'] = colvalue

    log.append(self_out)


pp = pprint.PrettyPrinter(indent=4)
pp.pprint(log)
with open('Task2.json', 'w') as outfile:
   json.dump(log, outfile,indent=4, sort_keys=True)