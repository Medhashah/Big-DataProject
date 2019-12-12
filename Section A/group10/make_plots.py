import os
import json
from dateutil import parser

from matplotlib import pyplot as plt


def map_each_type_counts(all_json, type_data):
    all_count_type_list = []
    total_columns = 0
    for json_stats in all_json:
        for column in json_stats['columns']:
            total_columns += 1
            for data_type in column['data_type']:
                if data_type['type'] == type_data:
                    count_type = int(data_type['count'])
                    all_count_type_list.append(count_type)
    plt.hist(all_count_type_list, color='green')
    plt.title("Number of {} in columns".format(type_data))
    if type_data == "DATE/TIME":
        type_data = "DATE"
    if type_data == "INTERGER(LONG)":
        type_data = "INT"
    plt.savefig("count_{}".format(type_data))


def map_column_counts(all_json):
    all_none_empties_list = []
    for json_stats in all_json:
        for column in json_stats['columns']:
            num_none_empty = int(column['number_non_empty_cells'])
            all_none_empties_list.append(num_none_empty)
    plt.hist(all_none_empties_list, color='green')
    plt.title("Column Count Histogram")
    plt.savefig("column_count")
    plt.show()


def map_column_null_counts(all_json):
    all_empties_list = []
    for json_stats in all_json:
        for column in json_stats['columns']:
            num_empty = int(column['number_empty_cells'])
            all_empties_list.append(num_empty)
    plt.hist(all_empties_list, bins=5, color='green')
    plt.title("Column Count Null Histogram")
    plt.show()


def map_percentage_missing(all_json):
    percent_empty = []
    for json_stats in all_json:
        for column in json_stats['columns']:
            num_empty = int(column['number_empty_cells'])
            num_none_empty = int(column['number_non_empty_cells'])
            try:
                per_missing = num_empty / (num_empty + num_none_empty)
            except ZeroDivisionError:
                if num_empty:
                    per_missing = 1
                else:
                    per_missing = 0
            percent_empty.append(per_missing)
    plt.hist(percent_empty, bins=10, color='green')
    plt.title("Percentage missing columns")
    plt.savefig("missing columns")
    plt.show()


def map_top_freq(all_json, numeric=True):
    all_freq = []
    freq_dict = {}
    for json_stats in all_json:
        for column in json_stats['columns']:
            freqs = column['frequent_values']
            all_freq.extend(freqs)
            for freq in freqs:
                freq_dict[freq] = freq_dict.get(freq, 0) + 1
    all_freq = []
    for freq_item in freq_dict.items():
        if numeric:
            all_freq.append(freq_item)
        else:
            if not is_int(freq_item[0]):
                all_freq.append(freq_item)
    all_freq = sorted(all_freq, key=lambda x: x[1])
    all_freq_limit = all_freq[-10:]
    freq_key = [x[0] for x in all_freq_limit]
    freq_val = [x[1] for x in all_freq_limit]
    plt.bar(freq_key, height=freq_val, color='purple')
    plt.savefig("top_freq")
    plt.show()


def map_top_array(all_json):
    all_shortest_dict = {}
    all_longest_dict = {}
    for json_stats in all_json:
        for column in json_stats['columns']:
            for data_type in column['data_type']:
                if data_type['type'] == 'TEXT':
                    # remember to change for list
                    all_shortest_dict[data_type['shortest_value']] = data_type['shortest_value']
                    all_longest_dict[data_type['longest_value']] = data_type['longest_value']


def map_each_datatype(all_json):
    int_count = 0
    real_count = 0
    date_count = 0
    text_count = 0
    for json_stats in all_json:
        for column in json_stats['columns']:
            for data_type in column['data_type']:
                if data_type['type'] == 'TEXT':
                    text_count += data_type['count']
                if data_type['type'] == "DATE/TIME":
                    date_count += data_type['count']
                if data_type['type'] == 'REAL':
                    real_count += data_type['count']
                if data_type['type'] == 'INTERGER(LONG)':
                    int_count += data_type['count']
    type_list = ['TEXT', "DATE/TIME", 'REAL', 'INTERGER(LONG)']
    type_value = [text_count, date_count, real_count, int_count]
    plt.bar(type_list, height=type_value, color='purple')
    plt.show()


def map_hetero_vs_not(all_json):
    hetero = 0
    not_hetero = 0
    for json_stats in all_json:
        for column in json_stats['columns']:
            if len(column['data_type']) > 1:
                not_hetero += 1
            elif len(column['data_type']) == 1:
                hetero += 1
    type_list = ['heterogeneous', 'not_heterogeneous']
    type_value = [hetero, not_hetero]
    bar_plt = plt.bar(type_list, height=type_value)
    bar_plt[0].set_color("green")
    bar_plt[1].set_color("purple")
    plt.title("Heterogeneous columns")
    plt.ylabel("columns")
    plt.show()

def is_int(val):
    try:
        int(val)
        return True
    except:
        return False


def is_real(val):
    try:
        float(val)
        return True
    except:
        return False


def is_datetime(val):
    try:
        parser.parse(val)
        return True
    # raw exception here, I tried to catch none raw dateutil error exception, but it's giving some errors
    # not sure I will need to fix up.
    except:
        return False

def main():
    all_json = []
    os_path = os.path.join(os.getcwd(), 'all_files_12_10')
    files = os.listdir(os_path)
    for file in files:
        with open(os.path.join(os_path,file)) as f:
            file_dict = json.loads(f.read())
            all_json.append(file_dict)

    map_column_counts(all_json)
    map_column_null_counts(all_json)
    map_top_freq(all_json)
    map_top_freq(all_json, numeric=False)
    map_each_datatype(all_json)
    map_percentage_missing(all_json)
    map_hetero_vs_not(all_json)
    map_each_type_counts(all_json, "REAL")
    map_each_type_counts(all_json, "TEXT")
    map_each_type_counts(all_json, "DATE/TIME")
    map_each_type_counts(all_json, "INTERGER(LONG)")
if __name__ == '__main__':
    main()