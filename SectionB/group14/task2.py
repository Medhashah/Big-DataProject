# -*- coding: utf-8 -*-
import json
import re
import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
from collections import Counter
from difflib import SequenceMatcher
from names_dataset import NameDataset
from colour import Color
from commonregex import CommonRegex
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions, types

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("task2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

find_name = NameDataset()
parser = CommonRegex()

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def is_person_name(df):
    count = 0
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        tmp = re.sub('[^a-zA-Z0-9\n\.]','', row[0])
        if find_name.search_first_name(tmp) or find_name.search_last_name(tmp):
            count += row[1]
    # return 'person_name'] = count
    return 'person_name', count
def is_business_name(df):
    count = 0
    business_name = ['HOTEL', 'CAFE', 'KITCHEN', 'LOUNGE', 'RESTAURANT', 'COFFEE', 'SHOP', 'BAR', 'PIZZA', 'GRILL', 'DUMBO', 'EXPRESS', 'HOUSE', 'LADY', 'STEAK', 'LANE', 'COMPANY', 'LLC', 'INC', 'EAT', 'VINe', 'LOBSTER', 'TACO', 'CREAM', 'FOOD', 'CO', 'LIMITED', 'RAMEN', 'LTD', 'PC', 'CORP', 'L.L.C', 'L.C', 'P.C']
    for row in df.rdd.collect():
#         if str(row[0]).isdigit():
#             break
        if str(row[0]).isdigit():
                    continue
        if row[0].upper() in business_name:
            count += row[1]
        else:
            for i in business_name:
                if similarity(i, row[0].upper()) >=0.5:
                    count += row[1]
    # return 'business_name'] = count
    return 'business_name', count

def is_phone_number(df):
    count = 0
    phone_number = re.compile('\d\d\d[\.\-\s]\d\d\d[\.\-\s]\d\d\d\d|\d{10}')
    for row in df.rdd.collect():
        if phone_number.match(str(row[0])):
            count += row[1]
    return 'phone_number', count

def is_address(df):
    count = 0
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if parser.street_addresses(str(row[0])):
            count += row[1]
    return 'address', count

def is_street_name(df):
    count = 0
    street_name = ['STREET','AVENUE','ST','PLACE','AVE','ROAD','COURT','LANE','DRIVE','PARK','BOULEVARD','RD','BLVD','PARKWAY','PL','TERRACE','EXIT','LOOP','EXPRESSWAY','PKWY','PLAZA','BRIDGE','EN','ENTRANCE','DR','ET','BROADWAY','FLOOR','added_by_us','TUNNEL','ROUTE','CIRCLE','WAY','SQUARE','XPWY','EXPY','CRCL','WALK','PKW','CONCOURSE','BOARDWALK','FREEWAY','CHANNEL']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
                    continue
        if row[0].upper() in street_name:
            count += row[1]
        else:
            for i in street_name:
                if similarity(i, row[0].upper()) >=0.5:
                    count += row[1]
    return 'street_name', count

def is_city(df):
    count = 0
    city = ["New York","Buffalo","Rochester","Yonkers","Syracuse","Albany","New Rochelle","Mount Vernon","Schenectady",\
    "Utica","White Plains","Hempstead","Troy","Niagara Falls","Binghamton","Freeport","Valley Stream","Long Beach","Spring Valley","Rome",\
    "Ithaca","North Tonawanda","Poughkeepsie","Port Chester","Jamestown","Harrison","Newburgh","Saratoga Springs","Middletown","Glen Cove",\
    "Elmira","Lindenhurst","Auburn","Kiryas Joel","Rockville Centre","Ossining","Peekskill","Watertown","Kingston","Garden City","Lockport",\
    "Plattsburgh","Mamaroneck","Lynbrook","Mineola","Cortland","Scarsdale","Cohoes","Lackawanna","Amsterdam","Massapequa Park","Oswego","Rye",\
    "Floral Park","Westbury","Depew","Kenmore","Gloversville","Tonawanda","Glens Falls","Mastic Beach","Batavia","Johnson City","Oneonta","Beacon",\
    "Olean","Endicott","Geneva","Patchogue","Haverstraw","Babylon","Tarrytown","Dunkirk","Mount Kisco","Dobbs Ferry","Lake Grove","Fulton","Oneida",\
    "Woodbury","Suffern","Corning","Fredonia","Ogdensburg","West Haverstraw","Great Neck","Sleepy Hollow","Canandaigua","Lancaster","Massena",\
    "Watervliet","New Hyde Park","East Rockaway","Potsdam","Amityville","Rye Brook","Hamburg","Farmingdale","Rensselaer","New Square","Airmont","Newark",\
    "Monroe","Malverne","Port Jervis","Croton-on-Hudson","Johnstown","Brockport","Geneseo","Chestnut Ridge","Hornell","Briarcliff Manor","Baldwinsville",\
    "Hastings-on-Hudson","Port Jefferson","Ilion","Colonie","Scotia","Herkimer","Williston Park","East Hills","Northport","Great Neck Plaza","Pleasantville",\
    "Pelham","New Paltz","Nyack","Hudson Falls","Warwick","Bayville","Manorhaven","Cedarhurst","Walden","Lawrence","North Hills","North Syracuse","Tuckahoe",\
    "Irvington","Norwich","East Rochester","Canton","Bronxville","Monticello","Horseheads","Solvay","Larchmont","East Aurora","Hudson","Kaser","Wesley Hills","Albion",\
    "New Hempstead","Hilton","Washingtonville","Elmsford","Medina","Webster","Malone","Fairport","Pelham Manor","Bath","Salamanca","Wappingers Falls","Goshen","Kings Point",\
    "Saranac Lake","Williamsville","Ballston Spa","Sea Cliff","Waterloo","Island Park","Mechanicville","Flower Hill","Penn Yan","Old Westbury","Chittenango","Ardsley",\
    "Little Falls","Montebello","Cobleskill","Montgomery","Canastota","Dansville","Manlius","Wellsville","Springville","Chester","Le Roy","Hamilton","Alfred","Fayetteville",\
    "Liberty","Waverly","Owego","Ellenville","Menands","Maybrook","Spencerport","Elmira Heights","Highland Falls","Saugerties"]
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in city:
            count += row[1]
        else:
            for i in city:
                if similarity(i.upper(), row[0].upper()) >=0.5:
                    count += row[1]
    return 'city', count

def is_neighborhood(df):
    count = 0
    neighborhoods = ['melrose', 'mott haven', 'port morris', 'hunts point', 'longwood', 'claremont', 'concourse village', 'crotona park', 'morrisania', 'concourse', 'highbridge', 'fordham', 'morris heights', 'mount hope', 'university heights', 'bathgate', 'belmont', 'east tremont', 'west farms', 'bedford park', 'norwood', 'university heights', 'fieldston', 'kingsbridge', 'kingsbridge heights', 'marble hill', 'riverdale', 'spuyten duyvil', 'van cortlandt village', 'bronx river', 'bruckner', 'castle hill', 'clason point', 'harding park', 'parkchester', 'soundview', 'unionport', 'city island', 'co-op city', 'locust point', 'pelham bay', 'silver beach', 'throgs neck', 'westchester square', 'allerton', 'bronxdale', 'indian village', 'laconia', 'morris park', 'pelham gardens', 'pelham parkway', 'van nest', 'baychester', 'edenwald', 'eastchester', 'fish bay', 'olinville', 'wakefield', 'williamsbridge', 'woodlawn', 'greenpoint', 'williamsburg', 'boerum hill', 'brooklyn heights', 'brooklyn navy yard', 'clinton hill', 'dumbo', 'fort greene', 'fulton ferry', 'fulton mall', 'vinegar hill', 'bedford-stuyvesant', 'ocean hill', 'stuyvesant heights', 'bushwick', 'city line', 'cypress hills', 'east new york', 'highland park', 'new lots', 'starrett city', 'carroll gardens', 'cobble hill', 'gowanus', 'park slope', 'red hook', 'greenwood heights', 'sunset park', 'windsor terrace', 'crown heights', 'prospect heights', 'weeksville', 'crown heights', 'prospect lefferts gardens', 'wingate', 'bay ridge', 'dyker heights', 'fort hamilton', 'bath beach', 'bensonhurst', 'gravesend', 'mapleton', 'borough park', 'kensington', 'midwood', 'ocean parkway', 'bensonhurst', 'brighton beach', 'coney island', 'gravesend', 'sea gate', 'flatbush', 'kensington', 'midwood', 'ocean parkway', 'east gravesend', 'gerritsen beach', 'homecrest', 'kings bay', 'kings highway', 'madison', 'manhattan beach', 'plum beach', 'sheepshead bay', 'brownsville', 'ocean hill', 'ditmas village', 'east flatbush', 'erasmus', 'farragut', 'remsen village', 'rugby', 'bergen beach', 'canarsie', 'flatlands', 'georgetown', 'marine park', 'mill basin', 'mill island', 'battery park city', 'financial district', 'tribeca', 'chinatown', 'greenwich village', 'little italy', 'lower east side', 'noho', 'soho', 'west village', 'alphabet city', 'chinatown', 'east village', 'lower east side', 'two bridges', 'chelsea', 'clinton', 'hudson yards', 'midtown', 'gramercy park', 'kips bay', 'rose hill', 'murray hill', 'peter cooper village', 'stuyvesant town', 'sutton place', 'tudor city', 'turtle bay', 'waterside plaza', 'lincoln square', 'manhattan valley', 'upper west side', 'lenox hill', 'roosevelt island', 'upper east side', 'yorkville', 'hamilton heights', 'manhattanville', 'morningside heights', 'harlem', 'polo grounds', 'east harlem', "randall's island", 'spanish harlem', 'wards island', 'inwood', 'washington heights', 'astoria', 'ditmars', 'garden bay', 'long island city', 'old astoria', 'queensbridge', 'ravenswood', 'steinway', 'woodside', 'hunters point', 'long island city', 'sunnyside', 'woodside', 'east elmhurst', 'jackson heights', 'north corona', 'corona', 'elmhurst', 'fresh pond', 'glendale', 'maspeth', 'middle village', 'liberty park', 'ridgewood', 'forest hills', 'rego park', 'bay terrace', 'beechhurst', 'college point', 'flushing', 'linden hill', 'malba', 'queensboro hill', 'whitestone', 'willets point', 'briarwood', 'cunningham heights', 'flushing south', 'fresh meadows', 'hilltop village', 'holliswood', 'jamaica estates', 'kew gardens hills', 'pomonok houses', 'utopia', 'kew gardens', 'ozone park', 'richmond hill', 'woodhaven', 'howard beach', 'lindenwood', 'richmond hill', 'south ozone park', 'tudor village', 'auburndale', 'bayside', 'douglaston', 'east flushing', 'hollis hills', 'little neck', 'oakland gardens', 'baisley park', 'jamaica', 'hollis', 'rochdale village', 'st. albans', 'south jamaica', 'springfield gardens', 'bellerose', 'brookville', 'cambria heights', 'floral park', 'glen oaks', 'laurelton', 'meadowmere', 'new hyde park', 'queens village', 'rosedale', 'arverne', 'bayswater', 'belle harbor', 'breezy point', 'edgemere', 'far rockaway', 'neponsit', 'rockaway park', 'arlington', 'castleton corners', 'clifton', 'concord', 'elm park', 'fort wadsworth', 'graniteville', 'grymes hill', 'livingston', 'mariners harbor', 'meiers corners', 'new brighton', 'port ivory', 'port richmond', 'randall manor', 'rosebank', 'st. george', 'shore acres', 'silver lake', 'stapleton', 'sunnyside', 'tompkinsville', 'west brighton', 'westerleigh', 'arrochar', 'bloomfield', 'bulls head', 'chelsea', 'dongan hills', 'egbertville', 'emerson hill', 'grant city', 'grasmere', 'midland beach', 'new dorp', 'new springville', 'oakwood', 'ocean breeze', 'old town', 'south beach', 'todt hill', 'travis', 'annadale', 'arden heights', 'bay terrace', 'charleston', 'eltingville', 'great kills', 'greenridge', 'huguenot', 'pleasant plains', "prince's bay", 'richmond valley', 'rossville', 'tottenville', 'woodrow']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].lower() in neighborhoods:
            count += row[1]
        else:
            for i in neighborhoods:
                if similarity(i, row[0].lower()) >=0.5:
                    count += row[1]
    return 'neighborhood', count

def is_lat_lon_cord(df):
    count = 0
    lat_lon_cord = re.compile('\D\d\d\D\d+\D*\d\d\D\d+\D')
    for row in df.rdd.collect():
        if lat_lon_cord.match(str(row[0])):
            count += row[1]
    return 'lat_lon_cord', count

def is_zip_code(df):
    count = 0
    zip_code = re.compile('^[0-9]{5}([- /]?[0-9]{4})?$')
    for row in df.rdd.collect():
        if zip_code.search(str(row[0])):
            count += row[1]
    return 'zip_code', count

def is_borough(df):
    count = 0
    boro_abb = ['K', 'M', 'Q', 'R', 'X']
    borough = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in boro_abb or row[0].upper() in borough:
            count += row[1]
        else:
            for i in borough:
                if similarity(i, row[0].upper()) >=0.5:
                    count += row[1]
    return 'borough', count

def is_school_name(df):
    count = 0
    school_name = ['middle', 'high', 'elementary', 'academy', 'center', 'jhs', 'j.h.s', 'ms', 'm.s', 'ps', 'p.s', 'is', 'i.s', 'i.s.']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        for name in school_name:
#             tmp = re.sub('[^a-zA-Z0-9\n\.]', '', row[0])
            if name in row[0].lower():
                count += row[1]
    return 'school_name', count

def is_color(df):
    count = 0
    color_abb = ['BK', 'BL', 'BG', 'BR', 'GL', 'GY', 'MR', 'OR', 'PK', 'PR', 'RD', 'TN', 'WH', 'YW']
    color = ['black', 'blue', 'beige', 'brown', 'gold', 'gray', 'maroon', 'orange', 'pink', 'purple', 'red', 'tan', 'white', 'yellow']

    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in color_abb or row[0].lower() in color:
            count += row[1]
    return 'color', count

def is_car_make(df):
    count = 0
    car_make = ["ACUR", "ALFA", "AMGN", "AMER", "ASTO", "AUDI", "AUST", "AVTI", "AUTU", "BENT", "BERO", "BLUI", "BMW", "BRIC", "BROC", "BSA", "BUIC", "CADI", "CHEC", "CHEV", "CHRY", "CITR", "DAEW", "DAIH", "DATS",
"DELO", "DESO", "DIAR", "DINA", "DIVC", "DODG", "DUCA", "EGIL", "EXCL", "FERR", "FIAT", "FORD", "FRHT", "FWD", "GZL", "GMC", "GRUM", "HD", "HILL", "HINO", "HOND", "HUDS", "HYUN", "CHRY", "INFI", "INTL",
 "ISU", "IVEC", "JAGU", "JENS", "AMER", "AMER", "KAWK", "KW", "KIA", "LADA", "LAMO", "LNCI", "LNDR", "LEXS", "LINC", "LOTU", "MACK", "MASE", "MAYB", "MAZD", "MCIN", "MERZ", "MERC", "MERK", "MG", "MITS", "MORG", "MORR", "MOGU", "NAVI", "NEOP", "NISS", "NORT", "OLDS", "OPEL", "ONTR", "OSHK", "PACK", "PANZ", "PTRB", "PEUG", "PLYM","PONT", "PORS", "RELA", "RENA", "ROL", "SAA", "STRN", "SCAN", "SIM", "SIN", "STLG", "STU", "STUZ", "SUBA", "SUNB", "SUZI", "THMS", "TOYT", "TRIU", "TVR", "UD", "VCTY", "VOLK", "VOLV", "WSTR", "WHIT", "WHGM", "AMER", "YAMA", "YUGO"]
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in car_make:
            count += row[1]
        else:
            for i in car_make:
                if similarity(i, row[0].upper()) >=0.5:
                    count += row[1]
    return 'car_make', count

def is_city_agency(df):
    count = 0
    city_agency = ['ACS', "ADMINISTRATION FOR CHILDREN'S SERVICES", 'BIC', 'BUSINESS INTEGRITY COMMISSION', 'BNYDC', 'BROOKLYN NAVY YARD DEVELOPMENT CORP', 'BOC', 'BOARD OF CORRECTION', 'BPL', 'BROOKLYN PUBLIC LIBRARY', 'BSA', 'BOARD OF STANDARDS AND APPEALS', 'BXDA', 'BRONX DISTRICT ATTORNEY', 'CCRB', 'CIVILIAN COMPLAINT REVIEW BOARD', 'CFB', 'CAMPAIGN FINANCE BOARD', 'COIB', 'CONFLICTS OF INTEREST BOARD', 'CPC', 'CITY PLANNING COMMISSION', 'CUNY', 'CITY UNIVERSITY OF NEW YORK', 'DANY', 'NEW YORK COUNTY DISTRICT ATTORNEY', 'DCA', 'DEPARTMENT OF CONSUMER AFFAIRS', 'DCAS', 'DEPARTMENT OF CITYWIDE ADMINISTRATIVE SERVICES', 'DCLA', 'DEPARTMENT OF CULTURAL AFFAIRS', 'DCP', 'DEPARTMENT OF CITY PLANNING', 'DDC', 'DEPARTMENT OF DESIGN AND CONSTRUCTION', 'DEP', 'DEPARTMENT OF ENVIRONMENTAL PROTECTION', 'DFTA', 'DEPARTMENT FOR THE AGING', 'DHS', 'DEPARTMENT OF HOMELESS SERVICES', 'DOB', 'DEPARTMENT OF BUILDINGS', 'DOC', 'DEPARTMENT OF CORRECTION', 'DOF', 'DEPARTMENT OF FINANCE', 'DOHMH', 'DEPARTMENT OF HEALTH AND MENTAL HYGIENE', 'DOI', 'DEPARTMENT OF INVESTIGATION', 'DOITT', 'DEPARTMENT OF INFORMATION TECHNOLOGY AND TELECOMMUNICATIONS', 'DOP', 'DEPARTMENT OF PROBATION', 'DORIS', 'DEPARTMENT OF RECORDS AND INFORMATION SERVICES', 'DOT', 'DEPARTMENT OF TRANSPORTATION', 'DPR', 'DEPARTMENT OF PARKS AND RECREATION', 'DSNY', 'DEPARTMENT OF SANITATION', 'DYCD', 'DEPARTMENT OF YOUTH AND COMMUNITY DEVELOPMENT', 'ECB', 'ENVIRONMENTAL CONTROL BOARD', 'EDC', 'ECONOMIC DEVELOPMENT CORPORATION', 'EEPC', 'EQUAL EMPLOYMENT PRACTICES COMMISSION', 'FCRC', 'FRANCHISE AND CONCESSION REVIEW COMMITTEE', 'FDNY', 'FIRE DEPARTMENT OF NEW YORK', 'FISA', 'FINANCIAL INFORMATION SERVICES AGENCY', 'HHC', 'HEAL AND HOSPITALS CORPORATION', 'HPD', 'DEPARTMENT OF HOUSING PRESERVATION AND DEVELOPMENT', 'HRA', 'HUMAN RESOURCES ADMINISTRATION', 'IBO', 'INDEPENDENT BUDGET OFFICE', 'KCDA', 'KINGS COUNTY DISTRICT ATTORNEY', 'LPC', 'LANDMARKS PRESERVATION COMMISSION', 'MOCJ', 'MAYOR’S OFFICE OF CRIMINAL JUSTICE', 'MOCS', "MAYOR'S OFFICE OF CONTRACT SERVICES", 'NYCDOE', 'DEPARTMENT OF EDUCATION', 'NYCEM', 'EMERGENCY MANAGEMENT', 'NYCERS', 'NEW YORK CITY EMPLOYEES RETIREMENT SYSTEM', 'NYCHA', 'NEW YORK CITY HOUSING AUTHORITY', 'NYCLD', 'LAW DEPARTMENT', 'NYCTAT', 'NEW YORK CITY TAX APPEALS TRIBUNAL', 'NYPD', 'NEW YORK CITY POLICE DEPARTMENT', 'NYPL', 'NEW YORK PUBLIC LIBRARY', 'OATH', 'OFFICE OF ADMINISTRATIVE TRIALS AND HEARINGS', 'OCME', 'OFFICE OF CHIEF MEDICAL EXAMINER', 'OMB', 'OFFICE OF MANAGEMENT & BUDGET', 'OSNP', 'OFFICE OF THE SPECIAL NARCOTICS PROSECUTOR', 'PPB', 'PROCUREMENT POLICY BOARD', 'QCDA', 'QUEENS DISTRICT ATTORNEY', 'QPL', 'QUEENS BOROUGH PUBLIC LIBRARY', 'RCDA', 'RICHMOND COUNTY DISTRICT ATTORNEY', 'RGB', 'RENT GUIDELINES BOARD', 'SBS', 'SMALL BUSINESS SERVICES', 'SCA', 'SCHOOL CONSTRUCTION AUTHORITY', 'TBTA', 'TRIBOROUGH BRIDGE AND TUNNEL AUTHORITY', 'TLC', 'TAXI AND LIMOUSINE COMMISSION', 'TRS', "TEACHERS' RETIREMENT SYSTEM"]

    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in city_agency:
            count += row[1]
        else:
            for i in city_agency:
                if similarity(i, row[0].upper()) >=0.5:
                    count += row[1]
    return 'city_agency', count

def is_area_of_study(df):
    count = 0
    area_study = ['ANIMAL SCIENCE', 'ARCHITECTURE', 'BUSINESS', 'COMMUNICATIONS', 'COMPUTER SCIENCE & TECHNOLOGY', 'COMPUTER SCIENCE, MATH & TECHNOLOGY', 'COSMETOLOGY', 'CULINARY ARTS', 'ENGINEERING', 'ENVIRONMENTAL SCIENCE', 'FILM/VIDEO', 'HEALTH PROFESSIONS', 'HOSPITALITY, TRAVEL, & TOURISM', 'HUMANITIES & INTERDISCIPLINARY', 'JROTC', 'LAW & GOVERNMENT', 'PERFORMING ARTS', 'PERFORMING ARTS/VISUAL ART & DESIGN', 'PROJECT-BASED LEARNING', 'SCIENCE & MATH', 'TEACHING', 'VISUAL ART & DESIGN', 'ZONED']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in area_study:
            count += row[1]
    return 'area_study', count

def is_subject_in_school(df):
    count = 0
    subject_in_school = ['ENGLISH', 'MATH', 'SCIENCE', 'SOCIAL STUDIES']
    course = ['ALGEBRA', 'ASSUMED TEAM TEACHING', 'CHEMISTRY', 'EARTH SCIENCE', 'ECONOMICS', 'ENGLISH 10', 'ENGLISH 11', 'ENGLISH 12', 'ENGLISH 9', 'GEOMETRY', 'GLOBAL HISTORY 10', 'GLOBAL HISTORY 9', 'LIVING ENVIRONMENT', 'MATCHED SECTIONS', 'MATH A', 'MATH B', 'OTHER', 'PHYSICS', 'US GOVERNMENT', 'US GOVERNMENT & ECONOMICS', 'US HISTORY']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in subject_in_school or row[0].upper() in course:
            count += row[1]
    return 'subject_in_school', count

def is_school_level(df):
    count = 0
    school_level = ['ELEMENTARY', 'HIGH', 'TRANSFER', 'K-2', 'K-3', 'K-8', 'MIDDLE', 'YABC']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        for i in school_level:
            if i in row[0].upper():
                count += row[1]
    return 'school_level', count

def is_college_name(df):
    count = 0
    college_name = ['university', 'college', 'school', 'academy']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].lower() in college_name:
            count += row[1]
    return 'college_name', count

def is_website(df):
    count = 0
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if parser.links(row[0]):
            count += row[1]

    return 'website', count

def is_building_classification(df):
    count = 0
    buildings = ['A0', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'B1', 'B2', 'B3', 'B9', 'C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'D0', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'E1', 'E3', 'E4', 'E5', 'E7', 'E9', 'F1', 'F2', 'F4', 'F5', 'F8', 'F9', 'G0', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8', 'H9', 'HB', 'HH', 'HR', 'HS', 'I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I9', 'J1', 'J2', 'J3', 'J4', 'J5', 'J6', 'J7', 'J8', 'J9', 'K1', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8', 'K9', 'L1', 'L2', 'L3', 'L8', 'L9', 'M1', 'M2', 'M3', 'M4', 'M9', 'N1', 'N2', 'N3', 'N4', 'N9', 'O1', 'O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'Q0', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'RA', 'RB', 'RC', 'RD', 'RG', 'RH', 'RI', 'RK', 'RM', 'RR', 'RS', 'RW', 'RX', 'RZ', 'S0', 'S1', 'S2', 'S3', 'S4', 'S5', 'S9', 'T1', 'T2', 'T9', 'U0', 'U1', 'U2', 'U3', 'U4', 'U5', 'U6', 'U7', 'U8', 'U9', 'V0', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7', 'Y8', 'Y9', 'Z0', 'Z1', 'Z2', 'Z3', 'Z4', 'Z5', 'Z6', 'Z7', 'Z8', 'Z9']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0] in buildings:
            count += row[1]
        else:
            for i in buildings:
                if i in row[0]:
                    count += row[1]
    return 'building_classification', count

def is_vehicle_type(df):
    count = 0
    vehicle_types = ['2dsd', '4dsd', 'ambu', 'atv', 'boat', 'bus', 'cmix', 'conv', 'cust', 'dcom', 'delv', 'dump', 'emvr', 'fire', 'flat', 'fpm', 'h/in', 'h/tr', 'h/wh', 'hrse', 'lim', 'loco', 'lsv', 'lsvt', 'ltrl', 'mcc', 'mcy', 'mfh', 'mopd', 'p/sh', 'pick', 'pole', 'r/rd', 'rbm', 'rd/s', 'refg', 'rplc', 's/sp', 'sedn', 'semi', 'sn/p', 'snow', 'stak', 'subn', 'swt', 't/cr', 'tank', 'taxi', 'tow', 'tr/c', 'tr/e', 'trac', 'trav', 'trlr', 'util', 'van', 'w/dr', 'w/sr']

    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].lower() in vehicle_types:
            count += row[1]
        else:
            for i in vehicle_types:
                if similarity(i, row[0].lower()) >=0.5:
                    count += row[1]
    return 'vehicle_types', count

def is_location_type(df):
    count = 0
    location_type = ['ABANDONED BUILDING', 'AIRPORT TERMINAL', 'ATM', 'BANK', 'BAR/NIGHT CLUB', 'BEAUTY & NAIL SALON', 'BOOK/CARD', 'BRIDGE', 'BUS (NYC TRANSIT)', 'BUS (OTHER)', \
                    'BUS STOP', 'BUS TERMINAL', 'CANDY STORE', 'CEMETERY', 'CHAIN STORE', 'CHECK CASHING BUSINESS', 'CHURCH', 'CLOTHING/BOUTIQUE', \
                    'COMMERCIAL BUILDING', 'CONSTRUCTION SITE', 'DAYCARE FACILITY', 'DEPARTMENT STORE', 'DOCTOR/DENTIST OFFICE', 'DRUG STORE', 'DRY CLEANER/LAUNDRY', 'FACTORY/WAREHOUSE', \
                    'FAST FOOD', 'FERRY/FERRY TERMINAL', 'FOOD SUPERMARKET', 'GAS STATION', 'GROCERY/BODEGA', 'GYM/FITNESS FACILITY', \
                    'HIGHWAY/PARKWAY', 'HOMELESS SHELTER', 'HOSPITAL', 'HOTEL/MOTEL', 'JEWELRY', 'LIQUOR STORE', 'LOAN COMPANY', \
                    'MAILBOX INSIDE', 'MAILBOX OUTSIDE', 'MARINA/PIER', 'MOSQUE', 'OPEN AREAS (OPEN LOTS)', 'OTHER', 'OTHER HOUSE OF WORSHIP', 'PARK/PLAYGROUND', 'PARKING LOT/GARAGE (PRIVATE)', 'PARKING LOT/GARAGE (PUBLIC)', \
                    'PHOTO/COPY', 'PRIVATE/PAROCHIAL SCHOOL', 'PUBLIC BUILDING', 'PUBLIC SCHOOL', 'RESIDENCE - APT. HOUSE', \
                    'RESIDENCE - PUBLIC HOUSING', 'RESIDENCE-HOUSE', 'RESTAURANT/DINER', 'SHOE', 'SMALL MERCHANT', 'SOCIAL CLUB/POLICY', 'STORAGE FACILITY', 'STORE UNCLASSIFIED', 'STREET', 'SYNAGOGUE',
                    'TAXI (LIVERY LICENSED)', 'TAXI (YELLOW LICENSED)', 'TAXI/LIVERY (UNLICENSED)', 'TELECOMM. STORE', 'TRAMWAY', 'TRANSIT - NYC SUBWAY', 'TRANSIT FACILITY (OTHER)', 'TUNNEL', 'VARIETY STORE', 'VIDEO STORE']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0] in location_type:
            count += row[1]
    return 'location_type', count

def is_park_playground(df):
    count = 0
    playground = ['park', 'playground', 'green', 'plots', 'square', 'plaza']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].lower() in playground:
            count += row[1]
        else:
            for i in playground:
                if i in row[0].lower():
                    count += row[1]
    return 'park_playground', count

def is_letter(df):
    count = 0
    letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    for row in df.rdd.collect():
        if str(row[0]).isdigit():
            continue
        if row[0].upper() in letters:
            count += row[1]
    return 'letter', count

def is_other(df):
    other_dic = {}
    parser = CommonRegex()
    find_name = NameDataset()

    other_label, other_count = is_person_name(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_business_name(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_phone_number(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_address(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_street_name(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_city_agency(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_city(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_neighborhood(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_lat_lon_cord(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_zip_code(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_borough(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_school_name(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_color(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_car_make(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_area_of_study(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_subject_in_school(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_school_level(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_college_name(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_website(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_building_classification(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_vehicle_type(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_location_type(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_park_playground(df)
    other_dic[other_label] = other_count

    other_label, other_count = is_letter(df)
    other_dic[other_label] = other_count

    other_label_list = []
    other_label_count = []
    k = Counter(other_dic)
    top_three = k.most_common(3)
    for i in top_three:
        other_label_list.append(i[0])
        other_label_count.append(i[1])

    return other_label_list, other_label_count

with open('cluster2.txt') as f:
    cluster = json.loads(f.read().replace("'", '"'))

output = {}
# output_with_pre = {}
predicted_semantic_list = []
tp = 0
fp = 0
fn = 0
frequency_dic = {'person_name' : 0, 'business_name': 0, 'phone_number': 0, 'address': 0, 'street_name': 0, 'city': 0, 'neighborhood': 0, 'lat_lon_cord': 0, 'zip_code': 0, 'borough': 0, 'school_name': 0, 'color': 0, 'car_make': 0, 'city_agency': 0, 'area_study': 0, 'subject_in_school': 0, 'school_level': 0, 'college_name': 0, 'website': 0, 'building_classification': 0, 'vehicle_types': 0, 'location_type': 0, 'park_playground': 0, 'letter': 0}


for filename in cluster:
    predicted_dic = {}

    df = (spark.read.format('csv')
           .options(inferschema='true', sep='\t')
           .load('../user/hm74/NYCColumns/{}'.format(filename))
           .toDF('value', 'count'))
    [dataset_name, column_name] = filename[:-7].split('.')
    column_name = column_name.lower()
    full_name = filename[:-7]
    print('working on {}'.format(filename))
    if 'first' in column_name or 'last' in column_name or 'middle' in column_name or ('full' in column_name and 'name' in column_name):
        semantic_type, count = is_person_name(df)
        predicted_dic[semantic_type] = count
    if 'business' in column_name or 'dba' in column_name:
        semantic_type, count = is_business_name(df)
        predicted_dic[semantic_type] = count
    if 'phone' in column_name or 'number' in column_name:
        semantic_type, count = is_phone_number(df)
        predicted_dic[semantic_type] = count
    if 'address' in column_name:
        semantic_type, count = is_address(df)
        predicted_dic[semantic_type] = count
    if 'street' in column_name:
        semantic_type, count = is_street_name(df)
        predicted_dic[semantic_type] = count
    if 'agency' in column_name:
        semantic_type, count = is_city_agency(df)
        predicted_dic[semantic_type] = count
    if 'city' in column_name and 'agency' not in column_name:
        semantic_type, count = is_city(df)
        predicted_dic[semantic_type] = count
    if 'neighborhood' in column_name:
        semantic_type, count = is_neighborhood(df)
        predicted_dic[semantic_type] = count
    if ('lat' in column_name and 'lon' in column_name) or ('location' in column_name and 'city' not in column_name):
        semantic_type, count = is_lat_lon_cord(df)
        predicted_dic[semantic_type] = count
    if 'zip' in column_name:
        semantic_type, count = is_zip_code(df)
        predicted_dic[semantic_type] = count
    if 'boro' in column_name or 'borough' in column_name:
        semantic_type, count = is_borough(df)
        predicted_dic[semantic_type] = count
    if column_name == 'school' or 'school' in column_name :
        semantic_type, count = is_school_name(df)
        predicted_dic[semantic_type] = count
    if 'color' in column_name:
        semantic_type, count = is_color(df)
        predicted_dic[semantic_type] = count
    if 'make' in column_name or 'model' in column_name:
        semantic_type, count = is_car_make(df)
        predicted_dic[semantic_type] = count
    if 'interest' in column_name or 'study' in column_name:
        semantic_type, count = is_area_of_study(df)
        predicted_dic[semantic_type] = count
    if 'subject' in column_name or 'core' in column_name:
        semantic_type, count = is_subject_in_school(df)
        predicted_dic[semantic_type] = count
    if 'level' in column_name or 'school' in column_name:
        semantic_type, count = is_school_level(df)
        predicted_dic[semantic_type] = count
    if 'collge' in column_name or 'university' in column_name:
        semantic_type, count = is_college_name(df)
        predicted_dic[semantic_type] = count
    if 'website' in column_name or 'site' in column_name:
        semantic_type, count = is_website(df)
        predicted_dic[semantic_type] = count
    if 'building' in column_name or 'classification' in column_name:
        semantic_type, count = is_building_classification(df)
        predicted_dic[semantic_type] = count
    if 'vehicle' in column_name and 'type' in column_name:
        semantic_type, count = is_vehicle_type(df)
        predicted_dic[semantic_type] = count
    if 'prem' in column_name or 'typ' in column_name:
        semantic_type, count = is_location_type(df)
        predicted_dic[semantic_type] = count
    if 'park' in column_name:
        semantic_type, count = is_park_playground(df)
        predicted_dic[semantic_type] = count



    if predicted_dic:
        res = max(predicted_dic, key=predicted_dic.get)
        res_count = predicted_dic[res]

        print('{} and {}\n'.format(res, res_count))

        output[full_name] = {
            'column_name': full_name,
            'semantic_types': {
                    'semantic_type': res,
                    'count': res_count
            }
        }

#         predicted_semantic_list.append(res)
        tp += 1
        fp += len(predicted_dic) - 1
        for i in predicted_dic:
            frequency_dic[i] += 1



    else:

        other_list, other_count = is_other(df)

        print('{} and {}\n'.format(other_list, other_count))

        output[full_name] = {
            'column_name': full_name,
            'semantic_types': {
                    'semantic_type': 'other',
                    'label': other_list,
                    'count': other_count
            }
        }

#         predicted_semantic_list.append(other_list)
        fn += 2
        for i in other_list:
            frequency_dic[i] += 1

# output_with_pre[1] = {
#     'predicted_type': predicted_semantic_list
# }
# output_with_pre.update(output)

with open('2019-BigDataResults/task2/task2.json', 'w') as outfile:
    json.dump([a for a in output.values()], outfile, indent=4)

print('presicion = {}, recall = {}'.format(tp/(tp+fp), tp/(tp+fn)))

x_axis = frequency_dic.keys()
y_axis = frequency_dic.values()
plt.figure(figsize=(15,5))
plt.bar(x_axis, y_axis, width=0.5, color='g')
plt.xticks(rotation=50)
plt.xlabel('Semantic Types')
plt.ylabel('Prevalence')