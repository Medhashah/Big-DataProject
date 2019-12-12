from fuzzywuzzy import fuzz
import re
import numpy as np
from urllib.request import urlopen
import matplotlib.pyplot as plt
import seaborn as sns



#task2_datasets = open('cluster3.txt')
#dataset_file_names = task2_datasets.readline()[1:-2].split(',')
#dataset_file_names = [x.replace("'", "").replace(" ", "").split('.')[0:2] for x in dataset_file_names]

#print(dataset_file_names[0])

similar_words = {                                                               \
    'person_name': ['First Name', 'Name', 'Person Name', 'Names', 'LastName', 'Last Name', 'Middle Name', 'MiddleName', 'MI'],
    'business_name': ['business name', 'dba', 'organization', 'org name', 'organization name'],
    'phone_number': ['phone no', 'cell phone no', 'cell phone', 'phone number'],
    'address': ['address', 'home address', 'house address', 'house number', 'house no', 'street address'],
    'street_name': ['Street Name', 'Street', 'Streets', 'StreetName', 'street address'],
    'city': ['district', 'city', 'city name'],
    'neighborhood': ['neighborhood', 'area', 'location'],
    'lat_lon_cord': ['lat', 'long', 'lattitude', 'longitude', 'lat long'],
    'zip_code': ['zip', 'zip code', 'zipcode', 'postcode', 'postal code', 'post'],
    'borough': ['borough', 'boro'],
    'school Name': ['school name'],
    'color': ['color', 'colors'],
    'car_make': ['car make', 'vehicle make'],
    'city_agency': ['Agency Name', 'Agency'],
    'area_of_study': ['area of study', 'interest'],
    'subject_in_school': ['subject', 'subject name'],
    'school_level': ['school levels', 'level'],
    'college_name': ['college name', 'university name'],
    'website': ['website', 'url', 'websites'],
    'building_classification': ['building type', 'building classification', 'type of building'],
    'vehicle_type': ['type of vehicle', 'vehicle type', 'car type'],
    'location_type': ['type of location', 'location type'],
    'park_playground': ['parks', 'park name', 'playground', 'park', 'playground name'],
    'other': ['other']
    }


"""
regex list
"""
BOROUGH_LIST = ['brooklyn', 'manhattan', 'queens', 'bronx', 'staten island']

street_address   = re.compile('\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)', re.IGNORECASE)


COLOR_NAMES_LIST = ['White', 'Yellow', 'Blue', 'Red', 'Green', 'Black', 'Brown', 'Azure', 'Ivory', 'Teal', 'Silver', 'Purple', 'Navy blue', 'Gray', 'Orange', 'Maroon', 'Charcoal', 'Aquamarine', 'Coral', 'Fuchsia', 'Wheat', 'Lime', 'Crimson', 'Khaki', 'pink', 'Magenta', 'Olden', 'Plum', 'Olive', 'Cyan']

def isValidURL(url):
    import re
    regex = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return url is not None and regex.search(url)




def isValidStreetName(streetName):
    return re.match(street_address)



"""
loop through all the column names
"""
line_list = []
with open('manual_labeling.csv', 'r') as csvf:
    while(True):
        line = csvf.readline()
        if(line):
            line_list.append(line[:-1])
        else:
            break



header = line_list[0]
line_list = line_list[1:]
categories = similar_words.keys()
no_of_categories = len(categories)
manual_labels = [line.split(',') for line in line_list]
conf_matrix = np.zeros((no_of_categories, no_of_categories))



def predict_category(col_name):
    pred = np.zeros(no_of_categories)
    cat_token_scores = []
    for category in categories:
        max_cat_token_score = 0
        for sim_word in similar_words[category]:
            tok_score = fuzz.partial_ratio(sim_word, col_name) + fuzz.token_sort_ratio(sim_word, col_name)
            if(tok_score >= max_cat_token_score):
                max_cat_token_score = tok_score
        cat_token_scores.append(max_cat_token_score)
    
    max_tok_score = max(cat_token_scores)
    if(max_tok_score < 80):
        cat_token_scores[-1] = 1
        for i in range(len(cat_token_scores)-1):
            cat_token_scores[i] = 0
        return cat_token_scores
    for cati in range(len(cat_token_scores)):
        if(cat_token_scores[cati] < max_tok_score):
            cat_token_scores[cati] = 0
        else:
            cat_token_scores[cati] = 1
    return cat_token_scores




for rowi, row in enumerate(manual_labels[1:]):
    ds_fname = row[1]
    col_name = row[2:-24]
    pred_cat = predict_category(str(col_name).lower())
    pred_cat = np.array(pred_cat)
    act_cat = np.array([int(x) for x in row[-24:]])
    pred_res = np.all(pred_cat == act_cat)
    #print(rowi)
    #print(np.where(act_cat == 1))
    ar = np.where(act_cat == 1)[0][0]
    pc = np.where(pred_cat == 1)[0][0]
    conf_matrix[ar][pc] += 1
    """
    print(rowi)
    if(not pred_res):
        proceed = 'n'
        while(proceed != 'y'):
            print(ds_fname, col_name, pred_cat, act_cat)
            proceed = input('proceed?')
    """



sns.heatmap(conf_matrix, annot = True, cbar = False)
plt.ylabel('True Label')
plt.xlabel('Predicted Label')
plt.show()            
"""    
"""
prec = []
rec = []
cat_list = list(categories)

# precision, recall
for ci in range(no_of_categories):
    prec.append(conf_matrix[ci][ci]/np.sum(conf_matrix[:, ci]))
    rec.append(conf_matrix[ci][ci]/np.sum(conf_matrix[ci, :]))

for ci in range(no_of_categories):
    print(cat_list[ci], ',', prec[ci], ',', rec[ci])





