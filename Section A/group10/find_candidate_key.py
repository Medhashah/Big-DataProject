import json
import os

def find_candidate_keys(all_json):
    all_dataset_candidate = []
    for json_stats in all_json:
        dataset_name = json_stats['dataset_name']
        candidate_keys = []
        for column in json_stats['columns']:
            column_name = column['column_name']
            num_empty = int(column['number_empty_cells'])
            num_none_empty = int(column['number_non_empty_cells'])
            num_dist = int(column['number_distinct_values'])
            try:
                if num_none_empty > 400:
                    per_missing = num_dist / (num_empty + num_none_empty)
                    if per_missing > .98:
                        candidate_keys.append(column_name)
            except ZeroDivisionError:
                per_missing = 0
        if candidate_keys:
            all_dataset_candidate.append({'dataset': dataset_name, 'candidate_keys': candidate_keys})
    with open('candidate_key.json', 'w') as f:
        json.dump(all_dataset_candidate, f, ensure_ascii=False)


def main():
    all_json = []
    os_path = os.path.join(os.getcwd(), 'all_files_12_10')
    files = os.listdir(os_path)
    for file in files:
        with open(os.path.join(os_path,file)) as f:
            file_dict = json.loads(f.read())
            all_json.append(file_dict)
    find_candidate_keys(all_json)

if __name__ == '__main__':
    main()