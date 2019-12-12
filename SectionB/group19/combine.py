import os
import json

fileName_list = []
with open("pre_index_for_task2.txt", 'r') as f:
    lines = f.readlines()  # 读取全部内容 ，并以列表方式返回
    for line in lines:
        fileName_list.append(line.strip())

print(fileName_list)
print(len(fileName_list))
result_list = []
for file_name in fileName_list:
    with open("output_for_task2/"+"index*"+file_name+".json", 'r') as f:
        data = json.load(f)
        # print(data)
        result_list.append(data)
print(result_list[0])
print(len(result_list))
result_str = json.dumps(result_list)
with open("task2" + '.json', 'w') as json_file:
    json_file.write(result_str)



# manual_label = []
# for file_name in fileName_list:
#     manual_label.append({"id :": file_name.split("*")[0],
#                          "column_name:": file_name.split(".")[1],
#                          "manual_labels": [{"semantic_type": ""}]})
# # print(final_data)
# label_str = json.dumps(manual_label)
# with open("manual_label" + '.json', 'w') as json_file:
#     json_file.write(label_str)
