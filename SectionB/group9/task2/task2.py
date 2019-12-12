# -*- coding: UTF-8 -*-
import json
import os
import re
import heapq
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import SparkSession
import re
import matplotlib
import nltk
from nltk.tag.stanford import StanfordNERTagger
import numpy as np


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return str(o, encoding='utf-8')
        return json.JSONEncoder.default(self, o)


def get_dataset_column():
    to_save = "dataset.csv"
    to_save_df = spark.read.option("delimiter", ",").csv(to_save)
    to_save_list = to_save_df.rdd.map(lambda x: (x[0], x[1])).collect()
    return to_save_list


def get_neigh():
    neigh = open("neig.txt")
    for line in neigh:
        neigh_list = line.split(",")
    return neigh_list


def check_person(to_person, to_person_num):
    out = person_name.tag(to_person)
    rest = []
    rest_num = []
    person_num = 0
    count_others = 0
    for index in range(len(out)):
        if out[index][1] == 'PERSON' or re.fullmatch('^[A-Z]$', data) or re.fullmatch(person_name_pattern, data):
            person_num += to_person_num[index]
        else:
            rest = rest + [to_person[index]]
            rest_num = rest_num + [to_person_num[index]]
    for i in range(len(rest)):
        count_others += int(rest_num[i])
    return [person_num, count_others]


def add_pred(file, coutner, others):
    label_count = []
    max_key = max(counter, key=counter.get)
    max_count = counter[max_key]
    if max_count < other:
        label = "others"
        return (label, others)
    threshold = 0.5
    del counter[max_key]
    second_key = max(counter, key=counter.get)
    second_count = counter[second_key]
    label_count.append((max_key, max_count))
    if second_count != 0 and second_count / max_count > threshold:
        label_count.append((second_key, second_count))
    return label_count


if __name__ == "__main__":
    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("task2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # -------------------------------------------------------------------------------------------------------------------
    """   
    
    This part:
    define regular expressions of some types, 
    initialize Stanford NER Tagger
    give the match list of some types
    
    """
    person_name = StanfordNERTagger(
        'stanford-ner-2018-10-16/classifiers/english.all.3class.caseless.distsim.crf.ser.gz',
        'stanford-ner-2018-10-16/stanford-ner.jar')
    person_name_pattern = re.compile(r'^[A-Z]{1,3}[\s.]{1,2}[A-Z]{1,10}$', re.IGNORECASE)
    web_pattern = re.compile(
        r'(?i)((?:https?://|www\d{0,3}[.])?[a-z0-9.\-]+[.](?:(?:international)|(?:construction)|(?:contractors)|(?:enterprises)|(?:photography)|(?:immobilien)|(?:management)|(?:technology)|(?:directory)|(?:education)|(?:equipment)|(?:institute)|(?:marketing)|(?:solutions)|(?:builders)|(?:clothing)|(?:computer)|(?:democrat)|(?:diamonds)|(?:graphics)|(?:holdings)|(?:lighting)|(?:plumbing)|(?:training)|(?:ventures)|(?:academy)|(?:careers)|(?:company)|(?:domains)|(?:florist)|(?:gallery)|(?:guitars)|(?:holiday)|(?:kitchen)|(?:recipes)|(?:shiksha)|(?:singles)|(?:support)|(?:systems)|(?:agency)|(?:berlin)|(?:camera)|(?:center)|(?:coffee)|(?:estate)|(?:kaufen)|(?:luxury)|(?:monash)|(?:museum)|(?:photos)|(?:repair)|(?:social)|(?:tattoo)|(?:travel)|(?:viajes)|(?:voyage)|(?:build)|(?:cheap)|(?:codes)|(?:dance)|(?:email)|(?:glass)|(?:house)|(?:ninja)|(?:photo)|(?:shoes)|(?:solar)|(?:today)|(?:aero)|(?:arpa)|(?:asia)|(?:bike)|(?:buzz)|(?:camp)|(?:club)|(?:coop)|(?:farm)|(?:gift)|(?:guru)|(?:info)|(?:jobs)|(?:kiwi)|(?:land)|(?:limo)|(?:link)|(?:menu)|(?:mobi)|(?:moda)|(?:name)|(?:pics)|(?:pink)|(?:post)|(?:rich)|(?:ruhr)|(?:sexy)|(?:tips)|(?:wang)|(?:wien)|(?:zone)|(?:biz)|(?:cab)|(?:cat)|(?:ceo)|(?:com)|(?:edu)|(?:gov)|(?:int)|(?:mil)|(?:net)|(?:onl)|(?:org)|(?:pro)|(?:red)|(?:tel)|(?:uno)|(?:xxx)|(?:ac)|(?:ad)|(?:ae)|(?:af)|(?:ag)|(?:ai)|(?:al)|(?:am)|(?:an)|(?:ao)|(?:aq)|(?:ar)|(?:as)|(?:at)|(?:au)|(?:aw)|(?:ax)|(?:az)|(?:ba)|(?:bb)|(?:bd)|(?:be)|(?:bf)|(?:bg)|(?:bh)|(?:bi)|(?:bj)|(?:bm)|(?:bn)|(?:bo)|(?:br)|(?:bs)|(?:bt)|(?:bv)|(?:bw)|(?:by)|(?:bz)|(?:ca)|(?:cc)|(?:cd)|(?:cf)|(?:cg)|(?:ch)|(?:ci)|(?:ck)|(?:cl)|(?:cm)|(?:cn)|(?:co)|(?:cr)|(?:cu)|(?:cv)|(?:cw)|(?:cx)|(?:cy)|(?:cz)|(?:de)|(?:dj)|(?:dk)|(?:dm)|(?:do)|(?:dz)|(?:ec)|(?:ee)|(?:eg)|(?:er)|(?:es)|(?:et)|(?:eu)|(?:fi)|(?:fj)|(?:fk)|(?:fm)|(?:fo)|(?:fr)|(?:ga)|(?:gb)|(?:gd)|(?:ge)|(?:gf)|(?:gg)|(?:gh)|(?:gi)|(?:gl)|(?:gm)|(?:gn)|(?:gp)|(?:gq)|(?:gr)|(?:gs)|(?:gt)|(?:gu)|(?:gw)|(?:gy)|(?:hk)|(?:hm)|(?:hn)|(?:hr)|(?:ht)|(?:hu)|(?:id)|(?:ie)|(?:il)|(?:im)|(?:in)|(?:io)|(?:iq)|(?:ir)|(?:is)|(?:it)|(?:je)|(?:jm)|(?:jo)|(?:jp)|(?:ke)|(?:kg)|(?:kh)|(?:ki)|(?:km)|(?:kn)|(?:kp)|(?:kr)|(?:kw)|(?:ky)|(?:kz)|(?:la)|(?:lb)|(?:lc)|(?:li)|(?:lk)|(?:lr)|(?:ls)|(?:lt)|(?:lu)|(?:lv)|(?:ly)|(?:ma)|(?:mc)|(?:md)|(?:me)|(?:mg)|(?:mh)|(?:mk)|(?:ml)|(?:mm)|(?:mn)|(?:mo)|(?:mp)|(?:mq)|(?:mr)|(?:ms)|(?:mt)|(?:mu)|(?:mv)|(?:mw)|(?:mx)|(?:my)|(?:mz)|(?:na)|(?:nc)|(?:ne)|(?:nf)|(?:ng)|(?:ni)|(?:nl)|(?:no)|(?:np)|(?:nr)|(?:nu)|(?:nz)|(?:om)|(?:pa)|(?:pe)|(?:pf)|(?:pg)|(?:ph)|(?:pk)|(?:pl)|(?:pm)|(?:pn)|(?:pr)|(?:ps)|(?:pt)|(?:pw)|(?:py)|(?:qa)|(?:re)|(?:ro)|(?:rs)|(?:ru)|(?:rw)|(?:sa)|(?:sb)|(?:sc)|(?:sd)|(?:se)|(?:sg)|(?:sh)|(?:si)|(?:sj)|(?:sk)|(?:sl)|(?:sm)|(?:sn)|(?:so)|(?:sr)|(?:st)|(?:su)|(?:sv)|(?:sx)|(?:sy)|(?:sz)|(?:tc)|(?:td)|(?:tf)|(?:tg)|(?:th)|(?:tj)|(?:tk)|(?:tl)|(?:tm)|(?:tn)|(?:to)|(?:tp)|(?:tr)|(?:tt)|(?:tv)|(?:tw)|(?:tz)|(?:ua)|(?:ug)|(?:uk)|(?:us)|(?:uy)|(?:uz)|(?:va)|(?:vc)|(?:ve)|(?:vg)|(?:vi)|(?:vn)|(?:vu)|(?:wf)|(?:ws)|(?:ye)|(?:yt)|(?:za)|(?:zm)|(?:zw))(?:/[^\s()<>]+[^\s`!()\[\]{};:\'".,<>?\xab\xbb\u201c\u201d\u2018\u2019])?)',
        re.IGNORECASE)
    street_pattern = re.compile(
        r'^[\w\s]{1,20}(?:street|lane|way|st|avenue|loop|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)',
        re.IGNORECASE)
    business_pattern = re.compile(r'(Inc|Corp|Co|Ltd)', re.IGNORECASE)
    college_pattern = re.compile('(University|College)', re.IGNORECASE)
    phone_pattern = re.compile(
        r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
    zip_pattern = re.compile(r'\b\d{5}(?:[-\s]\d{4})?\b')
    building_class_pattern = re.compile(r'^[A-Z]\d{1}[\s-].*')
    house_number_pattern = re.compile(r'^(?:\d{1,4}\W\d{1,4}|\d{1,4})$')
    coord_pattern = re.compile(r'(\()([-+]?)([\d]{1,2})(((\.)(\d+)(,)))(\s*)(([-+]?)([\d]{1,3})((\.)(\d+))?(\)))$')
    car_list = ['ACURA', 'ALFA', 'AMC', 'ASTON', 'AUDI', 'AVANTI', 'BENTL', 'BMW', 'BUICK', 'CAD', 'CHEV', 'CHRY',
                'DAEW', 'DAIHAT', 'DATSUN', 'DELOREAN', 'DODGE', 'EAGLE', 'FER', 'FIAT', 'FISK', 'FORD', 'FREIGHT',
                'GEO', 'GMC', 'HONDA', 'AMGEN', 'HYUND', 'INFIN', 'ISU', 'JAG', 'JEEP', 'KIA', 'LAM', 'LAN', 'ROV',
                'LEXUS', 'LINC', 'LOTUS', 'MAS', 'MAYBACH', 'MAZDA', 'MCLAREN', 'MB', 'MERC', 'MERKUR', 'MINI', 'MIT',
                'NISSAN', 'OLDS', 'PEUG', 'PLYM', 'PONT', 'POR', 'RAM', 'REN', 'RR', 'SAAB', 'SATURN', 'SCION', 'SMART',
                'SRT', 'STERL', 'SUB', 'SUZUKI', 'TESLA', 'TOYOTA', 'TRI', 'VOLKS', 'VOLVO', 'YUGO']
    borough_list = ['BROOKLYN', 'NEW YORK', 'STATEN ISLAND', 'QUEENS', 'MANHATTAN', 'BRONX', 'R', 'Q', 'M', 'X', 'K']
    school_level = ['K-1', 'K-2', 'K-3', 'K-4', 'K-5', 'K-6', 'K-7', 'K-8', 'K-9', 'K-10', 'K-11', 'K-12',
                    'ELEMENTARY', 'MIDDLE', 'HIGH', 'YABC', 'TRANSFER', 'SCHOOL', 'ACADEMY']
    school_name = ['ELEMENTARY', 'MIDDLE', 'HIGH', 'YABC', 'TRANSFER', 'SCHOOL', 'ACADEMY', 'P.S. ', 'TECHNICAL',
                   'CENTER', 'P.S./I.S.', 'M.S.', 'M.S./H.S.', 'J.H.S.', 'I.S.', 'HS', 'ACAD', 'SCL']
    vehicle_type = ["SEDAN", "AMBULANCE", "TRUCK", "BICYCLE", "BUS", "CONVERTIBLE", "MOTORCYCLE", "VEHICLE", "MOPED",
                    "SCOOTER", "TAXI", "PEDICAB", "BOAT", "VAN"]
    business_name = ["DELI", "PIZZA", "RESTAURANT", "CHINESE", "SHUSHI", "BAR", "SNACK", "CAFE", "COFFEE", "KITCHEN",
                     "GROCERY", "FOOD", "FARM", "MARKET", "WOK", "GOURMET", "BURGER", "LAUNDROMAT", "WINE", "LIQUORS",
                     "GARDEN", "DINER", "CUISINE", "PLACE", "CLEANERS", "PIZZERIA", "SERVICE"]
    parks = ["PARK", "PLAYGROUND", "GARDEN"]
    color_abb = ["AL", "ALL", "AM", "AO", "AT", "BG", "BK", "BL", "BN", "BR", "BZ", "CH", "CL", "CT", "Dk", "GL", "GR",
                 "GD", "GN", "GT", "GY", "IV", "LT", "NC", "OL", "OP", "PK", "RD", "SM", "TL", "TN", "TP", "VT", "WT", "YL"]
    location_list = sc.textFile("location.txt").collect()
    city_agency = sc.textFile("city_agency.txt").collect()
    city_list = sc.textFile("city.txt").collect()
    subject = sc.textFile("subject.txt").collect()
    area = sc.textFile("area.txt").collect()
    neighborhood = get_neigh()
    color = matplotlib.colors.cnames
# -------------------------------------------------------------------------------------------------------------------
    """
    
    This part:
    initialize data, empty dictionary to store the result.
    
    """
    files = get_dataset_column()
    data_dir = "/user/hm74/NYCColumns/"
    json_list = []
    pred_right_dict = {'person_name': 0, 'business_name': 0, 'phone_number': 0, 'address': 0, 'street_name': 0,
                       'city': 0,
                       'neighborhood': 0, 'lat_lon_cord': 0, 'zip_code': 0, 'borough': 0, 'school_name': 0, 'color': 0,
                       'car_make': 0,
                       'city_agency': 0, 'subject_in_school': 0, 'school_level': 0, 'college_name': 0, 'website': 0,
                       'building_classification': 0, 'vehicle_type': 0, 'park_playground': 0, 'location_type': 0,
                       'area_of_study': 0, 'house_number': 0}
    pred_dict = {'person_name': 0, 'business_name': 0, 'phone_number': 0, 'address': 0, 'street_name': 0, 'city': 0,
                 'neighborhood': 0, 'lat_lon_cord': 0, 'zip_code': 0, 'borough': 0, 'school_name': 0, 'color': 0,
                 'car_make': 0,
                 'city_agency': 0, 'subject_in_school': 0, 'school_level': 0, 'college_name': 0, 'website': 0,
                 'building_classification': 0, 'vehicle_type': 0, 'park_playground': 0, 'location_type': 0,
                 'area_of_study': 0, 'house_number': 0}
    total = {'person_name': 0, 'business_name': 0, 'phone_number': 0, 'address': 0, 'street_name': 0, 'city': 0,
             'neighborhood': 0, 'lat_lon_cord': 0, 'zip_code': 0, 'borough': 0, 'school_name': 0, 'color': 0,
             'car_make': 0,
             'city_agency': 0, 'subject_in_school': 0, 'school_level': 0, 'college_name': 0, 'website': 0,
             'building_classification': 0, 'vehicle_type': 0, 'park_playground': 0, 'location_type': 0,
             'area_of_study': 0, 'house_number': 0}
    # -------------------------------------------------------------------------------------------------------------------
    for file in files:

        print("%s  %s start" % (file[0], file[1]))
        total[file[1]] += 1
        counter = {'person_name': 0, 'business_name': 0, 'phone_number': 0, 'address': 0, 'street_name': 0, 'city': 0,
                   'neighborhood': 0, 'lat_lon_cord': 0, 'zip_code': 0, 'borough': 0, 'school_name': 0, 'color': 0,
                   'car_make': 0,
                   'city_agency': 0, 'subject_in_school': 0, 'school_level': 0, 'college_name': 0, 'website': 0,
                   'building_classification': 0, 'vehicle_type': 0, 'park_playground': 0, 'location_type': 0,
                   'area_of_study': 0, 'house_number': 0}
        full_file = data_dir + file[0]
        dataset_name = file[0].split(".")[0] + "." + file[0].split(".")[1]
        df = spark.read.option("delimiter", "\t").csv(full_file)
        other = 0
        to_person = []
        to_person_num = []
        datas = df.rdd.collect()

        for row in datas:
            data = row[0]
            num = int(row[1])
            if data is None:
                other += num
            elif re.match(web_pattern, data):
                counter['website'] += num
            elif re.match(zip_pattern, data):
                counter['zip_code'] += num
            elif re.match(coord_pattern, data):
                counter['lat_lon_cord'] += num
            elif re.match(phone_pattern, data):
                counter['phone_number'] += num
            elif data.lower() in color or data.upper() in color_abb:
                counter['color'] += num
            elif re.match(building_class_pattern, data):
                counter['building_classification'] += num
            elif any(name in data.upper() for name in business_name) or re.match(business_pattern, data):
                counter['business_name'] += num
            elif data.upper() in vehicle_type:
                counter['vehicle_type'] += num
            elif data.upper() in car_list:
                counter['car_make'] += num
            elif data.upper() in subject:
                counter['subject_in_school'] += num
            elif data.upper() in neighborhood:
                counter['neighborhood'] += num
            elif data.upper() in city_list:
                counter['city'] += num
            elif data.upper() in borough_list:
                counter['borough'] += num
            elif re.match(college_pattern, data):
                counter['college_name'] += num
            elif data.upper() in location_list:
                counter['location_type'] += num
            elif data.upper() in city_agency:
                counter['city_agency'] += num
            elif data.upper() in area:
                counter['area_of_study'] += num
            elif all(name in school_level for name in data.upper().split(" ")):
                counter['school_level'] += num
            elif any(name in data.upper().split(" ") for name in school_name):
                counter['school_name'] += num
            elif any(name in parks for name in data.upper().split(" ")):
                counter['park_playground'] += num
            elif re.match(house_number_pattern, data):
                counter['house_number'] += num
            elif re.fullmatch(street_pattern, data):
                counter['street_name'] += num
            elif re.match(street_pattern, data):
                counter['address'] += num
            else:
                to_person = to_person + [data]
                to_person_num = to_person_num + [num]
        [person_num, others] = check_person(to_person, to_person_num)
        ohters = others + other
        counter['person_name'] += person_num

        output = dict()
        output['column_name'] = dataset_name
        output["semantic_types"] = list(dict())
        labels_count = add_pred(file, counter, others)
        for pred in labels_count:
            label = pred[0]
            count = pred[1]
            if label == "house_number":
                output["semantic_types"].append({"semantic_type": "other", "label": label, "count": count})
            else:
                output["semantic_types"].append({"semantic_type": label, "count": count})
            pred_dict[label] += 1
            print("Prediction: %s" % label)
            if label == file[1]:
                pred_right_dict[label] += 1
        json_list.append(output)
        print("%s %s finish \n" % (file[0], file[1]))
# ------------------------------------------------------------------------------------------------------------------
    """
    
    This part:
    save result to json
    print prediction statistics
    
    """
    os.mkdir("./fuck_final_data")
    list_output = dict()
    list_output['predicted_types'] = json_list
    with open("./fuck_final_data/task2" + ".json", 'w') as f:
        json.dump(list_output, f, cls=JsonEncoder)
    print("total:\n", total)
    print("pre_dict:\n", pred_dict)
    print("pre_dict_right:\n", pred_right_dict)

