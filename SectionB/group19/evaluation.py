import json

import matplotlib.pyplot as plt
with open("task2.json", 'r') as f:
    predict = json.load(f)
    print(len(predict))
predict_dict = {"PERSON": 0,
               "Business name": 0,
               "Phone number": 0,
               "Address": 0,
               "Street name":0,
               "City": 0,
               "Neighborhood": 0,
               "LAT/LON coordinates": 0,
               "Zip code": 0,
               "Borough": 0,
               "School name": 0,
               "Color": 0,
               "Car make": 0,
               "City agency": 0,
               "Areas of study": 0,
               "Subjects in school": 0,
               "School Levels": 0,
               "College/University names": 0,
               "Websites": 0,
               "Building Classification": 0,
               "Vehicle Type": 0,
               "Type of location": 0,
               "Parks": 0,
               "other": 0}
graph_dict  = {"PERSON": 0,
               "Business name": 0,
               "Phone number": 0,
               "Address": 0,
               "Street name":0,
               "City": 0,
               "Neighborhood": 0,
               "LAT/LON coordinates": 0,
               "Zip code": 0,
               "Borough": 0,
               "School name": 0,
               "Color": 0,
               "Car make": 0,
               "City agency": 0,
               "Areas of study": 0,
               "Subjects in school": 0,
               "School Levels": 0,
               "College/University names": 0,
               "Websites": 0,
               "Building Classification": 0,
               "Vehicle Type": 0,
               "Type of location": 0,
               "Parks": 0,
               "other": 0}
predict_column_type = []
for item in predict:
    semantic_list = item["semantic_types"]
    max = 0
    column_type = ""
    for type in semantic_list:
        graph_dict[type["semantic_type"]]+=type["count"]
        if type["semantic_type"]!="other" and type["count"]>max:
            max = type["count"]
            column_type=type["semantic_type"]
    predict_dict[column_type]+=1
    predict_column_type.append(column_type)
# print(predict_dict)
# print(predict_column_type)
# print(len(predict_column_type))
# print(len(set(predict_column_type)))
print("graph",graph_dict)
i =0
# graph_type_list =[]
# graph_count_list  = []
# for item in graph_dict:
#     graph_type_list.append(item[0:3])
#     graph_count_list.append(graph_dict[item])
# print("type",graph_type_list)
# print("count", graph_count_list)
# plt.bar(range(len(graph_count_list)), graph_count_list,color='b',tick_label=graph_type_list)
# plt.show()

# graph1_type = []
# graph1_count =[]
# for item in predict_dict:
#     graph1_type.append(item[0:3])
#     graph1_count.append(predict_dict[item])
# plt.bar(range(len(graph1_count)), graph1_count, color='b', tick_label=graph1_type)
# plt.show()

# actual
with open("task2-manual-labels.json", 'r') as f:
    acutal = json.load(f)
    # print(acutal)

i = 0

acutal_dict = {"PERSON": 0,
               "Business name": 0,
               "Phone number": 0,
               "Address": 0,
               "Street name":0,
               "City": 0,
               "Neighborhood": 0,
               "LAT/LON coordinates": 0,
               "Zip code": 0,
               "Borough": 0,
               "School name": 0,
               "Color": 0,
               "Car make": 0,
               "City agency": 0,
               "Areas of study": 0,
               "Subjects in school": 0,
               "School Levels": 0,
               "College/University names": 0,
               "Websites": 0,
               "Building Classification": 0,
               "Vehicle Type": 0,
               "Type of location": 0,
               "Parks": 0,
               "other": 0}
for item in acutal:
    semantic_type = item["manual_labels"][0]["semantic_type"]
    # print(semantic_type)
    acutal_dict[semantic_type]+=1
    i = i + 1
print(acutal_dict)

graph1_type = []
graph1_count =[]
for item in acutal_dict:
    graph1_type.append(item[0:3])
    graph1_count.append(acutal_dict[item])
plt.bar(range(len(graph1_count)), graph1_count, color='b', tick_label=graph1_type)
plt.show()
print(i)
correct_dict =  {"PERSON": 0,
               "Business name": 0,
               "Phone number": 0,
               "Address": 0,
               "Street name":0,
               "City": 0,
               "Neighborhood": 0,
               "LAT/LON coordinates": 0,
               "Zip code": 0,
               "Borough": 0,
               "School name": 0,
               "Color": 0,
               "Car make": 0,
               "City agency": 0,
               "Areas of study": 0,
               "Subjects in school": 0,
               "School Levels": 0,
               "College/University names": 0,
               "Websites": 0,
               "Building Classification": 0,
               "Vehicle Type": 0,
               "Type of location": 0,
               "Parks": 0,
               "other": 0,}
for i in range(len(predict)):
    if acutal[i]["manual_labels"][0]["semantic_type"]==predict_column_type[i]:
        correct_dict[predict_column_type[i]]+=1
print(correct_dict)
precision_list = []
recall_list = []
for item in correct_dict:
    correct = correct_dict[item]
    predict = predict_dict[item]
    acutal = acutal_dict[item]
    if predict !=0 :
        precision = correct/float(predict)
    else:
        precision = 0
    if acutal!=0:
        recall = correct/float(acutal)
    else:
        recall=0
    precision_list.append({"semantic_type": item,
                           "precision":precision})
    recall_list.append({"semantic_type": item,
                           "recall":recall})

# print("precision:",precision_list)
# print("recall",recall_list)
# print(len(precision_list))

# for item in precision_list:
#     print(item["semantic_type"], item["precision"])
#
# for item in recall_list:
#     print(item["semantic_type"], item["recall"])







