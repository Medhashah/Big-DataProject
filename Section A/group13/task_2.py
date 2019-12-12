from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql import Row
import pandas as pd
import matplotlib.pyplot as plt
from fuzzywuzzy import fuzz
#from jellyfish import levenshtein_distance as ld
import re
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pyspark
import string
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import trim
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, RegexTokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics

sc = SparkContext()

spark = SparkSession \
    .builder \
    .appName("project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sqlContext = SQLContext(spark)

schema = StructType([StructField("Name", StringType(), True), StructField("count", IntegerType(), True)])

#with open('cluster3.txt') as fp:
 #   data = [line for line in fp]
#data = data[0].split(",")
#print(data[1])
#for row in data[:40]:
 #   print(row)



#types = [row.split('.')[1] for row in data]
# print(types)

list_to_match = ["Person name (Last name, First name, Middle name, Full name)","Business name", "Phone Number",
                 "Address", "Street name","City","Neighborhood", "Latitude Longitude", "Zip", "Borough",
                 "School name", "Color", "Car make", "City agency",
                 "Areas of study", "Subjects",
                 "School Levels", "College/University names","Websites",
                 "Building Classification","Vehicle Type",
                 "Type of location"]


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

def process(word):
    word = word.strip()
    lsword = word.split('_')
    wr = ""
    for w in lsword:
        if is_number(w):
            continue
        wr += w
    return wr.lower()


#for row in types:
 #   newrow = process(row)
  #  ls = []
   # for list_name in list_to_match:
    #    ls.append( (fuzz.partial_ratio(newrow, list_name.lower()), list_name))
    #ls.sort(reverse=True)
    #print(row," is most likely : ", ls[0][1])


def checkType(row):
    newrow = process(row)
    ls = []
    for list_name in list_to_match:
        ls.append( (fuzz.partial_ratio(newrow, list_name.lower()), list_name))
    ls.sort(reverse=True)
    return ls[0][1]

def preprocess(df):
    df = df.na.fill("null")
    return df


schl = ['school','academy','p.s','i.s','m.s','j.h.s','h.s','jhs','daycare','day care','learning']#ps,is,ms,hs

park = ['park','garden','recreation','ground','playground','play area','playing field','pool','field','green','woods','valley','forest','triangle','plant','woods','meadows','shore','country','pool','plaza','nature','beach','golf course']

subjects = ['english','math','science','social studies','social','biology', 'history', 'geography', 'algebra', 'economics','chemistry', 'geometry']

school_level = ['elementary','high school','middle','k-8','transfer school','high school transfer','k-3','k-2','yabc','elementary school','middle school','k-8 school']

interest = ["Performing Arts","Hospitality, Travel and Tourism","Cosmetology","Project-Based Learning","Computer Science,Math & Technology","Performing Arts/Visual Art & Design","Architecture","Teaching","Hospitality, Travel and Tourism","Animal Science","Performing Arts","Engineering","Humanities & Interdisciplinary","Science & Math","Computer Science & Technology","Health Professions","Visual Art & Design","Law & Government","Business","Performing Arts/Visual Art & Design","Zoned","Communications","Hospitality, Travel, & Tourism","Environmental Science","JROTC","Culinary Arts","Film/Video","Cosmetology","science & math","computer science & technology","humanities & interdisciplinary","performing arts/visual art & design","visual art & design"]

vehicles = ['Aerial tramway','Airplane','Aircraft','Ambulance','Baby carriage','Pram','Hot-air balloon','Bulldozer','Bicycle','Boat','Bus','Carriage','Cement mixer','Crane','Camper van','Caravan','Dump truck','Delivery van','Fire engine','Forklift','Helicopter','Motorcycle','Mountain bike','Moped','Police car','Rowboat','Scooter','Skateboard','Subway','Taxi','cab','Tractor','Train','Truck','Tram','Streetcar','Van','passenger vehicle','sport utility','station wagon','sedan','large com','small com','garbage or refuse','carry all','wagon','trail','tractor','tanker','livery vehicle','convertible','flat bed','motorbike','3-door','multi-wheeled','pickup','FDNY','flat rack','concrete mixer','usps','open body','minibike','limo','tow','motorized home','minicycle','cargo','uhaul','u-haul','nypd','lift boom','stake','rack','bike','snow plow','cycle','hopper','mail','well driller','golf','track','wagon','truck','pick', 'van','motor','door','moped','conv','garbage','mixer','ambul','passenger','tank','flat','bed','wheel','limo','vehicle','dump','train','delv','subn','dsd','util','refg','trlr','pkup','semi']

building_types = ["A0","A1","A2","A3","A4","A5","A6","A7","A8","A9","B1","B2","B3","B9","C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","CM","D0","D1","D2","D3","D4","D5","D6","D7","D8","D9","E1","E2","E3","E4","E7","E9","F1","F2","F4","F5","F8","F9","G0","G1","G2","G3","G4","G5","G6","G7","G8","G9","GU","GW","G9","HB","HH","HR","HS","H1","H2","H3","H4","H5","H6","H7","H8","H9","I1","I2","I3","I4","I5","I6","I7","I9","J1","J2","J3","J4","J5","J6","J7","J8","J9","K1","K2","K3","K4","K5","K6","K7","K8","K9","L1","L2","L3","L8","L9","M1","M2","M3","M4","M9","N1","N2","N3","N4","N9","O1","O2","O3","O4","O5","O6","O7","O8","O9","P1","P2","P3","P4","P5","P6","P7","P8","P9","Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","RA","RB","RG","RH","RK","RP","RR","RS","RT","RW","R0","R1","R2","R3","R4","R5","R6","R7","R8","R9","RR", "S0","S1","S2","S3","S4","S5","S9","T1","T2","T9","U0","U1","U2","U3","U4","U5","U6","U7","U8","U9","V0","V1","V2","V3","V4","V5","V6","V7","V8","V9","W1","W2","W3","W4","W5","W6","W7","W8","W9","Y1","Y2","Y3","Y4","Y5","Y6","Y7","Y8","Y9","Z0","Z1","Z2","Z3","Z4","Z5","Z7","Z8","Z9"]

locations = ['TRANSIT - NYC SUBWAY','ABANDONED BUILDING','TAXI/LIVERY (UNLICENSED)','BEAUTY & NAIL SALON','CHURCH','BANK','GAS STATION','SMALL MERCHANT','CONSTRUCTION SITE','DRY CLEANER/LAUNDRY','SYNAGOGUE','RESIDENCE - APT. HOUSE','VIDEO STORE','HIGHWAY/PARKWAY','FERRY/FERRY TERMINAL','AIRPORT TERMINAL','DEPARTMENT STORE','MARINA/PIER','DOCTOR/DENTIST OFFICE','COMMERCIAL BUILDING','STORE UNCLASSIFIED','BUS (NYC TRANSIT)','CHAIN STORE','GYM/FITNESS FACILITY','BUS TERMINAL','DRUG STORE','TELECOMM. STORE','BUS STOP','TUNNEL','TAXI (LIVERY LICENSED)','PRIVATE/PAROCHIAL SCHOOL','LOAN COMPANY','LIQUOR STORE','PHOTO/COPY','BUS TERMINAL','OPEN AREAS','MOSQUE','TAXI (LIVERY LICENSED)','RESTAURANT/DINER','CHECK CASHING BUSINESS','FAST FOOD','MAILBOX OUTSIDE','BUS (OTHER)','CEMETERY','PRIVATE/PAROCHIAL SCHOOL','RESIDENCE - PUBLIC HOUSING','ATM','BRIDGE','BRIDGE','RESTAURANT/DINER','HOTEL/MOTEL','TAXI (YELLOW LICENSED)','BOOK/CARD','FOOD SUPERMARKET','PUBLIC SCHOOL','MAILBOX OUTSIDE','FOOD SUPERMARKET','CANDY STORE','BUS (OTHER)','BOOK/CARD','JEWELRY','CLOTHING/BOUTIQUE','VARIETY STORE','HOMELESS SHELTER','RESIDENCE-HOUSE','GROCERY/BODEGA','TRANSIT FACILITY','PARK/PLAYGROUND','LIQUOR STORE','GROCERY/BODEGA','BAR/NIGHT CLUB','TELECOMM. STORE','OPEN AREAS (OPEN LOTS)','PHOTO/COPY','JEWELRY','TRAMWAY','PUBLIC BUILDING','PARKING LOT/GARAGE','PUBLIC BUILDING','ATM','HOSPITAL','CLOTHING/BOUTIQUE','PARKING LOT/GARAGE (PRIVATE)','BUS STOP','CANDY STORE','STORAGE FACILITY','TAXI (YELLOW LICENSED)','MOSQUE','SOCIAL CLUB/POLICY','PARK/PLAYGROUND','Classification','OPEN LOTS','OTHER HOUSE OF WORSHIP','DAYCARE FACILITY','PARKING LOT/GARAGE (PUBLIC)','TRAMWAY','MAILBOX INSIDE','DRUG STORE','HOSPITAL','FACTORY/WAREHOUSE','CHECK CASHING BUSINESS','TUNNEL','SOCIAL CLUB/POLICY','PARKING LOT/GARAGE','PUBLIC SCHOOL','RESIDENCE-HOUSE','FACTORY/WAREHOUSE','MAILBOX INSIDE','BAR/NIGHT CLUB','SHOE','OTHER HOUSE OF WORSHIP','VARIETY STORE','STORAGE FACILITY','FAST FOOD','RESIDENCE - PUBLIC HOUSING','HOTEL/MOTEL','SHOE','CEMETERY','LOAN COMPANY','TRANSIT FACILITY (OTHER)','residence','house','bar','club','subway','diner','market','street','store','hospital','grocery','bus']

neighborhoods = ["Melrose","Mott Haven","Port Morris","Hunts Point","Longwood","Claremont","Concourse Village","Crotona Park","Morrisania","Concourse","Highbridge","Fordham","Morris Heights","Mount Hope","University Heights","Bathgate","Belmont","East Tremont","West Farms","Bedford Park","Norwood","University Heights","Fieldston","Kingsbridge","Kingsbridge Heights","Marble Hill","Riverdale","Spuyten Duyvil","Van Cortlandt Village","Bronx River","Bruckner","Castle Hill","Clason Point","Harding Park","Parkchester","Soundview","Unionport","City Island","Co-op City","Locust Point","Pelham Bay","Silver Beach","Throgs Neck","Westchester Square","Allerton","Bronxdale","Indian Village","Laconia","Morris Park","Pelham Gardens","Pelham Parkway","Van Nest","Baychester","Edenwald","Eastchester","Fish Bay","Olinville","Wakefield","Williamsbridge","Woodlawn","Greenpoint","Williamsburg","Boerum Hill","Heights","Navy Yard","Clinton Hill","Dumbo","Fort Greene","Fulton Ferry","Fulton Mall","Vinegar Hill","Bedford-Stuyvesant","Ocean Hill","Stuyvesant Heights","Bushwick","City Line","Cypress Hills","East New York","Highland Park","New Lots","Starrett City","Carroll Gardens","Cobble Hill","Gowanus","Park Slope","Red Hook","Greenwood Heights","Sunset Park","Windsor Terrace","Crown Heights","Prospect Heights","Weeksville","Crown Heights","Prospect Lefferts Gardens","Wingate","Bay Ridge","Dyker Heights","Fort Hamilton","Bath Beach","Bensonhurst","Gravesend","Mapleton","Borough Park","Kensington","Midwood","Ocean Parkway","Bensonhurst","Brighton Beach","Coney Island","Gravesend","Sea Gate","Flatbush","Kensington","Midwood","Ocean Parkway","East Gravesend","Gerritsen Beach","Homecrest","Kings Bay","Kings Highway","Madison","Manhattan Beach","Plum Beach","Sheepshead Bay","Brownsville","Ocean Hill","Ditmas Village","East Flatbush","Erasmus","Farragut","Remsen Village","Rugby","Bergen Beach","Canarsie","Flatlands","Georgetown","Marine Park","Mill Basin","Mill Island","ManhattanBattery Park City","Financial District","Tribeca","ManhattanChinatown","Greenwich Village","Little Italy","Lower East Side","NoHo","SoHo","West Village","ManhattanAlphabet City","Chinatown","East Village","Lower East Side","Two Bridges","ManhattanChelsea","Clinton","Hudson Yards","ManhattanMidtown","ManhattanLincoln Square","Manhattan Valley","Upper West Side","ManhattanLenox Hill","Roosevelt Island","Upper East Side","Yorkville","ManhattanHamilton Heights","Manhattanville","Morningside Heights","ManhattanHarlem","Polo Grounds","ManhattanEast Harlem","Randall Island","Spanish Harlem","Wards Island","ManhattanInwood","Washington Heights","QueensAstoria","Ditmars","Garden Bay","Long Island City","Old Astoria","Queensbridge","Ravenswood","Steinway","Woodside","QueensHunters Point","Long Island City","Sunnyside","Woodside","QueensEast Elmhurst","Jackson Heights","North Corona","QueensCorona","Elmhurst","QueensFresh Pond","Glendale","Maspeth","Middle Village","Liberty Park","Ridgewood","QueensForest Hills"]

neigh = ["Rego Park","QueensBay Terrace","Beechhurst","College Point","Flushing","Linden Hill","Malba","Queensboro Hill","Whitestone","Willets Point","QueensBriarwood","Cunningham Heights","Flushing South","Fresh Meadows","Hilltop Village","Holliswood","Jamaica Estates","Kew Gardens Hills","Pomonok Houses","Utopia","QueensKew Gardens","Ozone Park","Richmond Hill","Woodhaven","QueensHoward Beach","Lindenwood","Richmond Hill","South Ozone Park","Tudor Village","QueensAuburndale","Bayside","Douglaston","East Flushing","Hollis Hills","Little Neck","Oakland Gardens","QueensBaisley Park","Jamaica","Hollis","Rochdale Village","St. Albans","South Jamaica","Springfield Gardens","QueensBellerose","Brookville","Cambria Heights","Floral Park","Glen Oaks","Laurelton","Meadowmere","New Hyde Park","Queens Village","Rosedale","QueensArverne","Bayswater","Belle Harbor","Breezy Point","Edgemere","Far Rockaway","Neponsit","Rockaway Park","Staten IslandArlington","Castleton Corners","Clifton","Concord","Elm Park","Fort Wadsworth","Graniteville","Grymes Hill","Livingston","Mariners Harbor","Meiers Corners","New Brighton","Port Ivory","Port Richmond","Randall Manor","Rosebank","St. George","Shore Acres","Silver Lake","Stapleton","Sunnyside","Tompkinsville","West Brighton","Westerleigh","Staten IslandArrochar","Bloomfield","Bulls Head","Chelsea","Dongan Hills","Egbertville","Emerson Hill","Grant City","Grasmere","Midland Beach","New Dorp","New Springville","Oakwood","Ocean Breeze","Old Town","South Beach","Todt Hill","Travis","Staten IslandAnnadale","Arden Heights","Bay Terrace","Charleston","Eltingville","Great Kills","Greenridge","Huguenot","Pleasant Plains","Prince Bay","Richmond Valley","Rossville","Tottenville","Woodrow"]

cache = ['avenue','street','road','lane','plaza','boulevard','suite','place','parkway','broadway','drive']

business = ["co","resto","restaurant","bistro","llc","ltd","inc","pizza","bar","cafe","donuts","grill", "beer","little caesars", "pret","kitchen","hotel","tavern","diner","pllc","group","solutions","pc","pe",'llp','ra']

ls_new = ["donut","pizz","food","bar","deli","house","bbq","corp","engineer",'associate',"architect","consult","service","design","studio"]

agency = ['nycem','nychh','dop','hra','doi','acs','dot','hpd','dca','dpr','ddc','tlc','dcp','dof','doris','cuny','oath','nypd','lpc','dob','qpl','sca','ccrb','dfta','dycd','dcla','nypl','doitt','bic','law','dep','boe','doc','sbs','doe','dohmh','dsny','bpl','dcas','dhs','311','nycha','cchr','fdny','edc','hhc','ocme','oem']

color = ['Amaranth','Amber','Amethyst','Apricot','Aquamarine','Azure','Baby blue','Beige','Black','Blue','Blue-green','Blue-violet','Blush','Bronze','Brown','Burgundy','Byzantium','Carmine','Cerise','Cerulean','Champagne','Chartreuse green','Chocolate','Cobalt blue','Coffee','Copper','Coral','Crimson','Cyan','Desert sand','Electric blue','Emerald','Erin','Gold','gray','Green','Harlequin','Indigo','ivory','jade','lavender','Jungle green','lemon','Lilac','lime','magenta','Magenta rose','maroon','mauve','Navy blue','Ochre','Olive','Orange','Orange-red','Orchid','Peach','Pear','Periwinkle','Persian blue','Pink','Plum','Prussian blue','Puce','Purple','Raspberry','Red','Red-violet','rose','ruby','salmon','Sangria','sapphire','Scarlet','silver','Slate gray','Spring bud','Spring green','Tan','taupe','teal','Turquoise','Ultramarine','Violet','viridian','White','yellow']

car_make = ['FORD', 'TOYOT', 'HONDA', 'NISSA', 'CHEVR', 'FRUEH', 'ME/BE', 'BMW', 'DODGE', 'JEEP', 'HYUND', 'GMC', 'LEXUS', 'INTER', 'ACURA', 'CHRYS', 'VOLKS', 'INFIN', 'SUBAR', 'AUDI', 'ISUZU', 'KIA', 'MAZDA', 'NS/OT', 'MITSU', 'LINCO', 'CADIL', 'VOLVO', 'ROVER', 'MERCU', 'HIN', 'HINO', 'KENWO', 'WORKH', 'BUICK', 'PETER', 'MINI', 'MACK', 'PORSC', 'SMART', 'PONTI', 'SATUR', 'JAGUA', 'FIAT', 'UD', 'SUZUK', 'SAAB', 'UTILI', 'WORK', 'SCION', 'VANHO', 'VESPA', 'OLDSM', 'STERL', 'UTIL', 'MASSA', 'HUMME', 'KAWAS', 'YAMAH', 'HARLE', 'TESLA', 'PLYMO', 'MCI', 'UPS', 'TRIUM', 'THOMA', 'BENTL', 'MASE', 'FONTA', 'PREVO', 'SPRI', 'FERRA', 'KW', 'NAVIS', 'IC', 'TRAIL', 'GEO', 'DUCAT', 'HYUN', 'GREAT', 'VPG', 'CHECK', 'STARC', 'KENTU', 'STAR', 'MER', 'DORSE', 'FRIE', 'WHITE', 'LND', 'FR/LI', 'SMITH', 'CARGO', 'EASTO', 'BL/B', 'UNIFL', 'JENSE', 'HUMBE', 'EAST', 'ROLLS', 'FUSO', 'SOLEC', 'GRUMM', 'BL/BI', 'ANCIA', 'FRH', 'FRE', 'FRLN', 'UT/M', 'DIAMO', 'LA RO', 'PET', 'GIDNY', 'UNIV', 'PIAGG', 'ALFAR', 'AUTOC', 'DAEWO', 'TESL', 'AUSTI', 'BERTO', 'FRGHT', 'FR/L', 'FHTL', 'AMC', 'WABAS', 'SUNBE', 'LAMBO', 'MICKE', 'FRIEG', 'MG', 'TRL', 'FRT', 'ORION', 'KENW', 'CHER', 'PREV', 'FREG', 'FR', 'STRIC', 'FRD', 'COLLI', 'FRTL', 'LOTUS', 'TRUCK', 'F/L', 'DATSU', 'VAN H', 'COACH', 'HIGHW', 'MAYBA', 'GNC', 'FEDEX', 'FR LI', 'RO/R', 'TES', 'WABA', 'VAN', 'OTHR', 'MAS', 'FRL', 'FRIGH', 'L R', 'STUDE', 'BRIDG', 'FRIG', 'WAB', 'VANH', 'MI FU', 'AS/M', 'RENAU', 'FL', 'MORGA', 'FRGH', 'PEUGE', 'CITRO', 'FRHT', 'KYMCO', 'APRIL', 'THEUR', 'TRAC', 'BSA', 'STR', 'BLUE', 'INTR', 'WO/CH', 'FRLI', 'GD', 'LAYTO', 'ELDO', 'GRUEH', 'INTEL', 'KTM', 'CIMC', 'MOTOR', 'NE/FL', 'ALEXA', 'FRG', 'GENES', 'PETE', 'WANC', 'FISKE', 'KI/M', 'TLR', 'TR/TE', 'AJAX', 'BUS', 'INTIL', 'WO/HO', 'OSHKO', 'BLUEB', 'FREGH', 'K W', 'MAGIR', 'PB', 'FERIG', 'GRE', 'M-B', 'REFG', 'FREIG', 'FREI', 'M B', 'SETR', 'TRLR', 'GILL', 'STL', 'UTI', 'NS', 'VA/HO', 'VN/H', 'WOR', 'WINN', 'GREIG', 'RTS', 'JINO', 'G DAN', 'CUST', 'TEMS', 'GOV', 'SMA', 'KRYS', 'STRI', 'WH/GM', 'WORTH', 'FRI', 'MINIC', 'SIMCA', 'HYTR', 'WE/ST', 'WKHS', 'WRK H', 'AL/R', 'THO', 'COLL', 'FT LI', 'BRTO', 'VAHO', 'INIFI', 'SCIO', 'BERI', 'ST/A', 'STE', 'DELV', 'KENMO', 'WK H', 'ME', 'MICK', 'NO/BU', 'TRA', 'MI/F', 'WO/C', 'WH', 'WO/H', 'N/S', 'VA/H', 'FT/LI', 'UNKNO', 'PRATT', 'UNK', 'ORIO', 'TITAN', 'WRK', 'FEDE', 'FLIN', 'METZ', 'NAVI', 'FR/LN', 'FT/LN', 'NA', 'HEIL', 'SEMI', 'HIND', 'GIL', 'W/H', 'FISK', 'VESP', 'MAYB']

citydf = spark.read.option("sep", "|").option("header", "true").csv("/user/sna354/us_cities_states_counties.csv")
citydf = citydf.filter(citydf["State short"]=="NY")
mvv = citydf.select("City").rdd.flatMap(lambda x: x).collect()
lowermvv = [c.lower() for c in mvv]


schema_name = StructType([StructField("Name", StringType(), True)])
names = spark.read.option("header","false").schema(schema_name).csv("/user/sna354/names.csv")
name_rdd = names.select("Name").rdd.flatMap(lambda x: x).collect()
name_list = [d.lower() for d in name_rdd]

def check_website(word):

    if word=="null" or word=="-":
        return "0"
    name = word.lower()
    name = name.replace("&","")
    name = name.replace("and","")
    name = name.replace("/"," ")
    name = name.replace("-","")
    name = name.strip()

    lsname = []
    for nm in name_list:
        lsname.append((fuzz.partial_ratio(name,nm.lower()), nm))
    lsname.sort(reverse=True)
    if lsname[0][0] > 90:
        return "Person_Name"

    #CHECKING FOR CITY NAMES
    if word is None:
        return "other"
    word = word.strip()
    word = word.lower()
    if word == "null":
        return "other"
    lst = mvv
    lscity = []
    for city in lowermvv:
        lscity.append((fuzz.partial_ratio(word.lower(), city.lower()), city))
    lscity.sort(reverse=True)
    #return str(ls)
    if lscity[0][0] > 90:
        return "City"

    #CHECKING FOR BUSINESS NAME
    name = word.strip()
    name = name.lower()
    name = name.replace(".","")

    lst = name.split(" ")
    if lst[0]=="the" or lst[0]=='le' or lst[0]=='la':
        return "business_name"
    if name == "null":
        return "other"
    if re.search("\w+\s*[a][n][d]\s*\w+", name):
        return "business_name"
    elif re.search("\s[b][y]\s", name):
        return "business_name"
    elif re.search("\w'[s]", name):
        return "business_name"
    elif any(item in business for item in lst):
        return "business_name"
    elif any("burger" in item for item in lst):
        return "business_name"
    elif any(it in item for it in ls_new for item in lst):
        return "business_name"


    #CHECKING FOR LATITUDE_LONGITUDE
    #if word[0] == '(' and word[len(word)-1] == ')':
    #    return "LAT_LON"

    number = word.replace('(', '')
    number = number.replace(')', '')
    lst = number.split(',')
    if len(lst)>1:
        try:
            lat = float(lst[0])
            lon = float(lst[1])
            if (lat >= -90 and lat <= 90) or (num>=-180 and num<=180):
                return "LAT_LON"
        except:
            pass
    else:
        try:
            val = float(lst[0])
            if(val >= -90 and val <= 90) or (val>=-180 and val <=180):
                return "LAT_LON"
        except:
            pass


    #CHECKING FOR WEBSITE
    if word=='null':
        return "Other"
    word = word.lower()
    ls = re.split('\.|\/',word)
    lst = ['gov', 'com', 'org', 'info', 'http', 'https', 'net','edu','www']
    if any(item in ls for item in lst):
        return "Website"


    #CHECKING FOR BOROUGH
    word = word.lower()
    if word in ["manhattan", "brooklyn", "bronx", "staten island", "queens"]:
        return "Borough"
   #CHECKING FOR ZIP CODES
    if len(word) == 5 or len(word) == 9:
        try:
            int(word)
            return "Zip"
        except:
            pass

    #CHECKING FoR PHONE NUMBERS
    number = word.strip()
    #number = number.replace("-","")
    #number = number.replace(".","")
    if re.search(r"^\(?(\d{3})\)?[-\. ]?(\d{3})[-\. ]?(\d{4})", word) != None:
        return "Number"

    #CHECKING FOR SCHOOL NAMES
    for key in schl:
        if key in word.lower():
            return "School Names"

    #CHECKING FOR PARK?PLAYGROUND
    for pk in park:
        if pk in word.lower():
            return "Park/Playground"

    #SCHOOL SUBJECTS
    for subs in subjects:
        if subs in word.lower():
            return "Subjects"

    #SCHOOL LEVEL
    for subs in school_level:
        if subs in word.lower():
            return "School Level"

    #AREAS OF STUDY
    for subs in interest:
        if subs.lower() == word.strip().lower():
            return "Areas of Study"

    #CHECK FOR COLOR
    lsc = []
    for list_name in color:
        lsc.append((fuzz.partial_ratio(word.lower(), list_name.lower()), list_name))
    lsc.sort(reverse=True)
    #return str(ls)
    if lsc[0][0] > 90:
        return lsc[0][0]


    #UNIVERSITY AND COLLEGE NAME
    name = word.strip()
    name = name.lower()
    name = name.replace(","," ")
    name = name.replace("&","")
    if name=="null":
        return "other"
    if "university" in name:
        return "college_name"
    if "college" in name and "school" not in name:
        return "college_name"


    name = word.strip()
    name = name.lower()
    name = name.replace("&","")
    name = name.replace("and","")
    name = name.replace("/"," ")
    
    lstv = []
    
    for item in vehicles:
        lstv.append((fuzz.partial_ratio(word.lower(),item.lower()), item))
    lstv.sort(reverse=True)
    
    if lstv[0][0] > 90:
        return "Vehicle Type"


    #BUILDING CLASSIFICATION
    for subs in building_types:
        if subs.lower() in word.strip().lower():
            return "Building Classification"

    #TYPE OF LOCATION
    for subs in locations:
        if subs.lower() in word.strip().lower():
            return "Type Of Location"

    #NEIGHBORHOOD
    for subs in neighborhoods:
        if subs.lower() in word.strip().lower():
            return "Neighborhood"
    for ne in neigh:
        if ne.lower() in word.strip().lower():
            return "Neighborhood"


    #CAR MAKE
    if len(word) <= 5:
        ls = []
        for cmake in car_make:
            ls.append((fuzz.partial_ratio(word.lower(),cmake.lower()),cmake))
        ls.sort(reverse = True)

        if ls[0][0] > 70:
            return "CAR MAKE"


    #CITY AGENCY
    name = word.strip()
    name = name.lower()
    if name=="null":
        return "0"
    if name in agency:
        return "city_agency"

    #ADDRESS AND STREET
    address = word.split()
    try:
        int(word)
        return "0"
    except:
           pass
    if 'po' == address[0].lower() or 'p.o.' == address[0].lower() or 'co' == address[0].strip().lower() or 'c/o' in word.lower() or '#' in word:
        return "Address"
    address[0] = address[0].split('-')[0]
    try:
        for c in cache:
            if address[1].strip().lower() in c or c in address[1].strip().lower():
                if len(address) <= 2:
                    return "Street"
                else:
                    return "Address"
    except:
        pass
    try:
        int(address[0])
        return "Address"
    except:
        pass
        #return "Street"
    return "0"




check_website = F.udf(check_website, StringType())

def json_for_website(df, col):
    df = preprocess(df)
    df = df.withColumn("check", check_website(F.col(col)))
    df.createOrReplaceTempView("df")
    temp = spark.sql("select check, count(*) as ctr from df group by check")
    ls = []
    output = {}
    output["column_name"] = col
    output["semantic_types"] = []

    for row in temp.rdd.collect():
        op = {}

        if row.check=="0":
            ls.append(("Other", row.ctr))            
            op["semantic_type"] = "Other"
            op["count"] = row.ctr
        else:
            ls.append((row.check, row.ctr))
            op["semantic_type"] = row.check
            op["count"] = row.ctr

        output["semantic_types"].append(op)
    return output


with open('cluster3.txt') as fp:
    line = fp.readline()
    finalop = {}
    finalop["predicted_types"] = []
    ctr = 1
    while line:
        print(ctr)
        line = line.strip()
        web1 = sqlContext.read.option("sep", "\t").option("header", "false").option("delimiter","\t").schema(schema).csv("/user/hm74/NYCColumns/"+line)    
        output = json_for_website(web1, "Name")
        finalop["predicted_types"].append(output) 
        line = fp.readline() 
        ctr +=1                                                                                     
    with open("task2.json","w") as outfile:
        json.dump(finalop,outfile) 
