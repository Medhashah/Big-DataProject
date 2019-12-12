import pyspark
import json
import re
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

given_types = ['person_name', 'business_name', 'phone_number', 'address', 'street_name', 'city', 'neighborhood', \
                'lat_lon_cord', 'zip_code', 'borough', 'school_name', 'color', 'car_make', 'city_agency', 'area_of_study',\
                'subject_in_school', 'school_level', 'college_name', 'website', 'building_classification', 'vehicle_type', \
                'location_type', 'park_playground', 'other']

def check_phone_number(value):
    if re.match(r'^\d{10}$', value) != None:
        return True
    elif re.match(r'^\d{3}\-\d{3}\-\d{4}$', value) != None:
        return True
    elif re.match(r'^\d{3}\d{3}\-\d{4}$', value) != None:
        return True

    elif re.match(r'^\d{3}\-\d{3}\d{4}$', value) != None:
        return True
    else:
        return False

def check_vehicle_type(value):
    type_list = ["AMBULANCE","VAN","TAXI","BUS","SDN","SUBN","12PU","2C","2CV","2D","2DR",
    '2DSD','4DSD','AMBU','ATV','BOAT','BUS','CMIX','CONV','CUST','DCOM','DELV','DUMP','EMVR',
    'FIRE','FLAT','FPM','H/IN','HRSE','H/TR','H/WH','LIM','LOCO','LSV','LSVT','LTRL','MCC',
    'MCY','MFH','MOPD','PICK','POLE','P/SH','RBM','RD/S','REFG','RPLC','R/RD','SEDN','SEMI',
    'SNOW','SN/P','S/SP','STAK','SUBN','SWT','TANK','T/CR','TOW','TRAC','TRAV','TR/C','TR/E',
    'TRLR','UTIL','VAN','W/DR','W/SR']
    for type in type_list:
        if type in value.upper():
            return True
    return False

def check_school_level(value):
    school_level = ['k-8', 'elementary', 'elementary school', 'middle','Middle School',\
                    'Transfer School','High','High School']
    if value.lower() in school_level:
        return True
    else:
        return False

def check_zip_code(value):
    regex = re.compile(r"(^[0-9]{5}(?:-[0-9]{4})?$)")
    if re.fullmatch(regex,value) != None:
        return True
    else:
        return False


def check_color(value):
    color_list = ['white','black','pink','purple','blue','gray','green','beige','yellow',\
                        'ivory','gold','orange','red','brown']
    for color in color_list:
        if color in value.lower():
            return True
    return False

def check_car_make(value):
    car_make = ['BMW','TOYOT','AUDI','NISSA','DODGE','ACURA','HYUND','MINI','LEXUS','SUBAR', \
                'HONDA','KIA','CHEVR','FORD','BUICK','VOLKS','GMC','VOLVO','INFIN','JEEP']

    for brand in car_make:
        if brand in value.upper():
            return True
    return False

def check_website(value):
    regex = re.compile(r'(http|ftp|https):\/\/[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?')
    if re.match(regex,value.lower()) != None:
        return True
    else:
        return False


def check_borough(value):
    borough_list = ['brooklyn', 'bronx','queens','manhattan','staten island']
    for borough in borough_list:
        if borough in value.lower():
            return True
    return False


def check_city(value):
    city_list = ['new york','albany','amsterdam','auburn','buffalo','batavia','beacon','binghamton','rochester','yonkers',\
                    'syracuse','new rochelle','cheektowaga','mount vernon','schenectady','brentwood','utica','white plains',\
                    'freeport','hempstead','levittown','troy','hicksville']
    for city in city_list:
        if city in value.lower():
            return True
    return False

def check_person_name(value):
    lastname = ['smith', 'johnson','williams','jones','brown','davis','miller','wilson','moore','taylor','anderson','thomas',\
                'jackson','white','harris','martin','thompson','garcia','Martinez','robinson']
    firstname =['rakib','james','anthony','jacob','daniel','alexander','michael','william','jayden','ethan','ryan',\
                'emma','isabella','madison','ava','ashley','chloe','olivia','emily','paola']
    lastname.extend(firstname)
    for name in lastname:
        if name in value.lower():
            return True
    return False

def check_subjects(value):
    subjects = ['math', 'algebra','geometry','science','biology','physics','chemistry','geography','history',\
                'citizenship','physical education','p.e','art','music','business','economics','social']
    for subject in subjects:
        if subject in value.lower():
            return True
    return False

def check_address(value):
    street_name = ['street','st.','avenue','boulevard']
    for street in street_name:
        if street in value.lower() and value[0].isdigit() == True:
            return True
    return False

def check_street_name(value):

    street_name = ['street','st.','ave','boulevard','parkway','road','rd','blvd','pkwy','drive', 'lane',]

    for street in street_name:
        if street in value.lower() and value[0].isdigit() == False:
            return True
    return False

def check_neighborhood(value):
    manhattan = ['Alphabet','Battery Park','Carnegie Hill','Chelsea','Chinatown','East Harlem','East Village',\
                'Financial District','Flatiron District','Gramercy Park','Greenwich Village','Harlem','Clinton','Inwood',\
                'Kips Bay','Lincoln Square','Lower East Side','Manhattan Valley','Midtown East','Midtown West',\
                'Morningside Heights','Murray Hill','NoLita','Little Italy','Roosevelt Island','SoHo','Tribeca','Upper East Side',\
                'Upper West Side','Washington Heights','West Village']
    bronx =['Baychester','Bedford Park','Belmont','Bronxdale','Castle Hill','City Island','Concourse Village',\
            'Grand Concourse','Morrisania','Country Club','Fieldston','Fordham','Hunts Point','Kingsbridge','Kingsbridge Heights',\
            'Melrose','Morris Heights','Morris Park','Mott Haven','Parkchester','Pelham Bay','Pelham Gardens','Pelham Parkway',\
            'Port Morris','Riverdale','Soundview','Throgs Neck','Tremont','University Heights','Wakefield','Williamsbridge','Woodlawn']
    brooklyn =['Bath Beach','Bay Ridge','Bedford-Stuyvesant','Bensonhurst','Bergen Beach','Boerum Hill','Borough Park',\
            'Brighton Beach','Brooklyn Heights','Brownsville','Bushwick','Canarsie','Carroll Gardens','Cobble Hill','Coney Island',\
            'Crown Heights','Cypress Hills','Downtown Brooklyn','Dumbo','Vinegar Hill','Dyker Heights','East New York','Flatbush',\
            'Flatlands','Fort Greene','Clinton Hill','Gerritsen Beach','Gowanus','Gravesend','Greenpoint','Greenwood Heights',\
            'Manhattan Beach','Marine Park','Midwood','Mill Basin','Park Slope','Prospect Heights','Prospect Park South','Kensington',\
            'Red Hook','Sea Gate','Sheepshead Bay','Sunset Park','Williamsburg','Windsor Terrace']
    queens = ['Arverne','Astoria','Bayside','Beechhurst','Belle Harbor','Neponsit','Bellerose','Briarwood','Broad Channel','Cambria Heights',\
            'College Point','Corona','Douglaston','East Elmhurst','Elmhurst','Far Rockaway','Floral Park','Flushing','Forest Hills',\
            'Fresh Meadows','Glen Oaks','Glendale','Hillcrest','Hollis','Hollis Hills','Howard Beach','Jackson Heights','Jamaica',\
            'Jamaica Estates','Jamaica Hills','Kew Gardens','Laurelton','Little Neck','Long Island City','Maspeth','Middle Village',\
            'Oakland Gardens','Ozone Park','Queens Village','Rego Park','Richmond Hill','Ridgewood','Rockaway Park','Rosedale',\
            'South Jamaica','South Ozone Park','Springfield Gardens','St. Albans','Sunnyside','Whitestone','Woodhaven','Woodside']
    staten = ['Annadale','Arden Heights','Arrochar','Bay Street','Bulls Head','Castleton Corners','Charleston','Clifton','Dongan Hills',\
            'Eltingville','Emerson Hill','Graniteville','Grant City','Grasmere','Concord','Great Kills','Grymes Hill','Huguenot',\
            'Livingston','Manor Heights','Mariners Harbor','Midland Beach','New Brighton','New Dorp','New Dorp Beach','New Springville',\
            'Oakwood','Pleasant Plains','Port Richmond','Prince Bay','Richmondtown','Rosebank','Rossville','Shore Acres','Silver Lake',\
            'South Beach','St. George','Stapleton','Sunnyside','Todt Hill','Tompkinsville','Tottenville','Travis','West New Brighton',\
            'Westerleigh','Willowbrook','Woodrow']
    all_neighborhoods = []
    all_neighborhoods.extend(manhattan)
    all_neighborhoods.extend(bronx)
    all_neighborhoods.extend(brooklyn)
    all_neighborhoods.extend(queens)
    all_neighborhoods.extend(staten)
    for neighborhood in all_neighborhoods:
        if neighborhood.lower() in value.lower():
            return True
    return False



def check_location(value):
    locations = ['building','airport',' bank','church','clothing','boutique','park']
    for location in locations:
        if location in value.lower():
            return True
    return False

def check_LAT_LON_coordinates(value):
    regex = re.compile(r'^[\-\+]?(0(\.\d{1,10})?|([1-9](\d)?)(\.\d{1,10})?|1[0-7]\d{1}(\.\d{1,10})?|180\.0{1,10})?')
    try :
        lat = value.split(",")[0][1:]
        if re.match(regex,lat) != None:
            return True
        else:
            return False
    except:
        pass

def check_building_classify(value):
    first = value[0:1]
    second = value[1:2]
    third = value[2:3]

    if (first.isupper() & second.isdigit()) & (third == '-'):
        return True
    else:
        return False

def check_city_agency(value):
    agencies = ['3-1-1','311','NYPD','NYCHA','NEW YORK CITY HOUSING AUTHORITY','SCA','SCHOOL CONSTRUCTION AUTHORITY',\
                'SBS','SMALL BUSINESS SERVICES','DCLA','CULTURAL AFFAIRS','DOE','DEPARTMENT OF EDUCATION',\
                'HPD','HOUSING PRESERVATION AND DEVELOPMENT','DYCD','DEPARTMENT OF YOUTH & COMMUNITY DEVELOPMENT',\
                'DOF','DEPARTMENT OF FINANCE','DOB','DEPARTMENT OF BUILDINGS','ACS','ADMIN FOR CHILDREN\'S SERVICES',\
                'NYCEM','DEPARTMENT OF EMERGENCY MANAGEMENT','NYPL','NEW YORK PUBLIC LIBRARY','DCAS',\
                'DEPARTMENT OF CITYWIDE ADMIN SERVICE','CCHR','COMMISSION ON HUMAN RIGHTS','DPR','DEPARTMENT OF PARKS AND RECREATION',\
                'FD','FIRE DEPARTMENT','TLC','TAXI AND LIMOUSINE COMMISSION','DOHMH','DEPARTMENT OF HEALTH AND MENTAL HYGIENE','CUNY',\
                'CITY UNIVERSITY OF NEW YORK','DS','DEPARTMENT OF SANITATION']
    for agency in agencies:
        if agency in value.upper():
            return True
    return False

def check_school_name(value):
    schools = ['school','high school','secondary school','college','academy','university']
    for school in schools:
        if school in value.lower():
            return True
    return False

def check_business_name(value):
    business_suffix = ['inc','inc.','llc','l.l.c.','p.c.','pc','lp','l.p.','corp','corp.','corporation','company','co.','ltd','ltd.','capital','holdings','services']
    for business in business_suffix:
        if business in value.lower():
            return True
    return False

def check_college_name(value):
    colleges = ['college','academy','university']
    for college in colleges:
        if college in value.lower():
            return True
    return False

def check_park_playgrounds(value):
    keywords = ['park','playground']
    for keyword in keywords:
        if keyword in value.lower():
            return True
    return False

# def cal_recall_predict(true, pred):
#     dic = {}
#     for semantic_type in given_types:
        
#     pass


with open('cluster3.txt','r') as files:
    data = files.read().strip()[1:-1]
    cluster3 = data.split(',')

dic_total = {}
dic_correct = {}
print(len(cluster3))

output = {}
output["datasets"]=[]
for each in cluster3:
    each = each.strip()[1:-1]
    flag = 0
    filepath = os.path.join('./NYCColumns', each)
    DF = spark.read.option("header", "true").option("delimiter", "\t").csv(filepath).cache()

    rows = DF.collect()
    file_name = each
    devise = {}
    devise["column_name"] = file_name
    devise["semantic_types"] = {}
    count = 0
    for i in range(len(rows)):
        # print(rows[i])
        if len(rows[i]) <= 1:
            continue
        instance_name = rows[i][0].strip()
        instance_fre = int(rows[i][1])
        count += instance_fre 
        if instance_name == None:
            continue
        if (check_zip_code(instance_name) == True):
            label_name = "zip_code"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_person_name(instance_name) == True):
            label_name = "person_name"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_business_name(instance_name) == True):
            label_name = "business_name"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_school_level(instance_name) == True):
            label_name = "school_level"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_color(instance_name) == True):
            label_name = "color"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_car_make(instance_name) == True):
            label_name = "car_make"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_city(instance_name) == True):
            label_name = "city"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_borough(instance_name) == True):
            label_name = "borough"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_vehicle_type(instance_name) == True):
            label_name = "vehicle_type"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_neighborhood(instance_name) == True):
            label_name = "neighborhood"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_school_name(instance_name) == True):
            label_name = "school_name"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_college_name(instance_name) == True):
            label_name = "college_name"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_address(instance_name) == True):
            label_name = "address"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_street_name(instance_name) == True):
            label_name = "street_name"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_city_agency(instance_name) == True):
            label_name = "city_agency"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_subjects(instance_name) == True):
            label_name = "subject_in_school"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_website(instance_name) == True):
            label_name = "website"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_LAT_LON_coordinates(instance_name) == True):
            label_name = "lat_lon_cord"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_phone_number(instance_name) == True):
            label_name = "phone_number"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_location(instance_name) == True):
            label_name = "location_type"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_park_playgrounds(instance_name) == True):
            label_name = "park_playground"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_building_classify(instance_name) == True):
            label_name = "building_classification"
            if label_name in dic_total.keys():
                dic_total[label_name].add(each)
            else:
                dic_total[label_name] = set()
                dic_total[label_name].add(each)
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if flag == 22:
            label_name = "other"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = instance_fre
    l = []
    for key, value in devise["semantic_types"].items():
        if value >= 0.05 * count:
            new_dict = {}
            new_dict['semantic_type'] = key
            new_dict['label'] =""
            new_dict['count'] = value
            l.append(new_dict)
        else:
            if key == 'other' :
                continue
            dic_total[key].remove(each)
    devise['semantic_types'] = l
    json_file_path='./output/' + each
    with open(json_file_path, 'w') as json_file:
        json.dump(devise, json_file, indent=4)

true_file = './true_type.csv'
true_type = spark.read.option("header", "true").option("delimiter", ",").csv(true_file).cache()

for key, value in dic_total.items():
    total = len(value)
    print(total)
    count = 0
    for file in value:
        if true_type.filter(true_type.column_name == file).count() == 0:
            continue
        if true_type.filter(true_type.column_name == file).select(key).collect()[0][0] != None:
            count += 1
    print(count)
    print('%s precision: %f' %(key, count / total))
    print('%s recall: %f' %(key, count / int(true_type.filter(true_type.column_name == 'total').select(key).collect()[0][0])))
   





