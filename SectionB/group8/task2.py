import os
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from csv import reader
import time
import json
import subprocess
import re


sc = SparkContext()   

spark = SparkSession.builder.appName("semantic_profiling").config('spark.driver.memory', '35g').config('spark.executor.cores', 5).config('spark.executor.memory', '35g').config('spark.dynamicAllocation.enabled', True).config('spark.dynamicAllocation.maxExecutors', 25).config('spark.yarn.executor.memoryOverhead', '4096').getOrCreate()



def semantic_profiling(path, start_time, n):


    def stat1(df, c, n, function_list, column_name):       
        
        def number_of_non_empty_cells(col):    
            return F.count(col)

        
        def is_zip_code(col):                   
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'^(1(0|1)\d{3})$') == True, x))
        
        def is_phone_number(col):               
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$') == True, x))
        
        def is_Latitude(col):                   
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'^40[.]\d+$') == True, x))
                
        
        def is_Longitude(col):                  
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'^[-]7(3|4)[.]\d+$') == True, x))

                
        def is_school_name(col):                
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'school|academy') == True, x))
        
        
        def is_street_name(col):                
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'avenue|lane|road|boulevard|drive|street|ave|dr|rd|blvd|ln|st') == True, x))
        
        def is_lat_lon_cord(col):                   
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'^[(]40[.]\d+[,\s]{1,2}[-]7(3|4)[.]\d+[)]$') == True, x))
        
        def is_college_name(col):               
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'college|university') == True, x))
        
        
        def is_borough(col):              
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'^(manhattan|brooklyn|queens|bronx|staten island)$') == True, x))
        
        def is_park_playground(col):          
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'park') == True, x))
        
        
        def is_website(col):                   
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'^https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}$') == True, x))
        
        
        def is_business_name(col):              
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'corp|inc|llc|ltd') == True, x))
                
        
        
        def is_subject_in_school(col):             
            @F.udf
            def f1(v):
                l = ['algebra', 'art','biology', 'calculus','chemistry', 'cinema','composition','craft','drawing',               'economics','economy',  'english',
                'arts','geography','geometry', 'gym','history', 'humanities', 'language', 'literature', 'math', 'mathematics','music','painting',
                'physical development','physics','science', 'social science', 'social studies', 'statistics']
                if v in l:
                    return True
                else:
                    False
            x = col.cast('string')
            return F.count(F.when(f1(F.lower(x)) == True, x))
        
        
        
        def is_neighbourhood(col):               #function to check neighbourhood
            @F.udf
            def f1(v):
                l = ['allerton', 'alphabet city','annadale', 'arden heights','arlington', 'arrochar', 'arverne', 'astoria', 'auburndale','baisley park','bath beach', 'bathgate', 'battery park city','bay ridge','bay terrace','baychester','bayside','bayswater', 'bedford park',
                'bedford-stuyvesant', 'beechhurst','belle harbor','bellerose', 'belmont','bensonhurst', 'bergen beach','bloomfield','boerum hill', 
                'borough park', 'breezy point' ,'briarwood' ,'brighton beach' ,'bronx river' ,'bronxdale' ,'brooklyn heights' ,'brooklyn navy yard', 'brookville' ,'brownsville' ,'bruckner' ,'bulls head','bushwick','cambria heights' ,'canarsie' ,'carroll gardens',
                'castle hill' ,'castleton corners' ,'charleston' ,'chelsea' ,'chinatown', 'city island', 'city line' ,'claremont' ,
                'clason point' ,'clifton' ,'concord', 'clinton hill' ,'clinton' ,'co-op city', 'cobble hill', 'college point', 'concourse village', 
                'concourse' ,'coney island' , 'corona' ,'crotona park' ,'crown heights' ,'cunningham heights' ,'cypress hills' ,'ditmars' ,
                'ditmas village' ,'dongan hills' ,'douglaston' ,'dumbo' ,'dyker heights' ,'east elmhurst' ,'east flatbush' ,'east flushing' ,
                'east gravesend' ,'east harlem' ,'east new york', 'east tremont' ,'east village' ,'eastchester' ,'edenwald' ,'edgemere' ,
                'egbertville', 'elm park' ,'elmhurst','eltingville' ,'emerson hill' ,'erasmus' ,'far rockaway' ,'farragut' ,'fieldston' ,
                'financial district' ,'fish bay' ,'flatbush' ,'flatlands' ,'floral park' , 'flushing south' , 'flushing' ,'fordham' ,'forest hills' ,'fort greene' ,'fort hamilton','fort wadsworth', 'fresh meadows' ,'fresh pond' , 'grymes hill' ,'hamilton heights' ,
                'harding park' ,'harlem' ,'highbridge','highland park' ,'hilltop village', 'hollis hills' ,'hollis' ,'holliswood' ,'homecrest' ,
                'howard beach' ,'hudson yards','huguenot' ,'hunters point' ,'hunts point' ,'indian village' ,'inwood' ,'jackson heights' ,
                'jamaica estates' ,'jamaica' ,'kensington' ,'kew gardens hills' ,'kew gardens' ,'kings bay' ,'kings highway' ,'kingsbridge heights' ,'kingsbridge' ,'kips bay' ,'laconia' ,'laurelton' , 'lenox hill' ,'liberty park', 'lincoln square', 'linden hill' ,
                'lindenwood' ,'little italy', 'little neck' ,'livingston' ,'locust point', 'long island city' ,'longwood','lower east side' ,
                'madison' ,'malba' ,'manhattan beach' ,'manhattan valley' ,'manhattanville' ,'mapleton','marble hill' ,'marine park' ,'mariners harbor' ,'maspeth' ,'meadowmere' , 'meiers corners' ,'melrose', 'middle village' ,'midland beach' ,'midtown','midwood' ,
                'mill basin', 'mill island','morningside heights','morris heights' ,'morris park' ,'morrisania','mott haven' ,'mount hope' ,
                'murray hill' ,'neponsit' ,'new brighton' ,'new dorp' ,'new hyde park' ,'new lots' ,'new springville' ,'noho' ,'north corona',
                'norwood' ,'oakland gardens','oakwood' ,'ocean breeze' ,'ocean hill','ocean parkway','old astoria' ,'old town' ,'olinville' ,
                'ozone park' ,'park slope' ,'parkchester' ,'pelham bay' ,'pelham gardens' ,'pelham parkway' ,'peter cooper village' ,
                'pleasant plains' ,'plum beach' ,'polo grounds','pomonok houses', 'port ivory' ,'port morris','port richmond', 'princes bay' ,
                'prospect heights' ,'prospect lefferts gardens' ,'queens village' ,'queensboro hill','queensbridge' ,'randall manor' ,
                'randall\'s island', 'ravenswood' ,'red hook','rego park','remsen village' ,'richmond hill' ,'richmond valley', 'ridgewood',
                'riverdale' ,'rochdale village' ,'rockaway park','roosevelt island' ,'rose hill' ,'rosebank' ,'rosedale','rossville', 'rugby',
                'sea gate','sheepshead bay','shore acres' ,'silver beach' ,'silver lake' ,'soho' ,'soundview' ,'south beach', 'south jamaica', 
                'south ozone park', 'spanish harlem' ,'springfield gardens','spuyten duyvil' ,'st. albans' ,'st. george' ,'stapleton' ,
                'starrett city','steinway' ,'stuyvesant heights','stuyvesant town' ,'sunnyside' ,'sunset park', 'sutton place', 'throgs neck' ,
                'todt hill' ,'tompkinsville' ,'tottenville', 'travis','tribeca','tudor city' ,'tudor village','turtle bay' ,'two bridges',
                'unionport','university heights','upper east side' ,'upper west side','utopia','van cortlandt village','van nest','vinegar hill',
                'wakefield' ,'wards island','washington heights','waterside plaza' ,'weeksville','west brighton' ,'west farms','west village',
                'westchester square','westerleigh','whitestone' ,'willets point','williamsbridge', 'williamsburg','windsor terrace','wingate',
                'woodhaven','woodlawn','woodrow','woodside','yorkville']
                if v in l:
                    return True
                else:
                    False
            x = col.cast('string')
            return F.count(F.when(f1(F.lower(x)) == True, x))
        
        
        
        def is_city_agency(col):                
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'system|tribunal|agency|library|council|board|commission|president|services|office|department|nyc|administration|authority') == True, x))
        
        def is_school_level(col):              
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'elementary|kindergarten|middle|nursery|primary|secondary|senior|high|transfer|k-2|k-3|k-8') == True, x))
        
        def is_address(col):                    
            x = col.cast('string')
            return F.count(F.when(F.lower(x).rlike(r'\d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:ave|dr|rd|blvd|ln|st|av)\.?') == True, x))
        
        def is_building_classification(col):            
            x = col.cast('string')
            return F.count(F.when(x.rlike(r'^[A-Z][0-9A-Z][-][-\w]+$') == True, x))
        
        def is_area_of_study(col):                  
            @F.udf
            def f1(v):
                l = ['engineering', 'teaching', 'communications', 'animal ccience', 'science & math', 'law & government', 'architecture', 'business', 'culinary arts', 'performing arts', 'health profession', 'visual art & design', 'film/video', 'cosmetology', 'humanities & interdisciplinary', 'computer science & technology', 'project-based learning', 'hospitality, travel, & tourism', 'performing arts/visual art & design', 'environmental science', 'zoned']
                if v in l:
                    return True
                else:
                    False
            x = col.cast('string')
            return F.count(F.when(f1(F.lower(x)) == True, x))
        
        def is_MiddleInitial(col):                      
            x = col.cast('string')
            return F.count(F.when(x.rlike(r'^[A-Z][.]?$') == True, x))
        
        def is_car_make(col):                           
            @F.udf
            def f1(v):
                l = ['MERCEDES-BENZ S550', 'LEXUS ES 330', 'TOYOTA AVALON', 'HYUNDAI EQUUS', 'FORD FUSION', 'GMC YUKON DINALI', 'AUDI Q5', 'LINCOLN MKZ', 'HYUNDAI SONATA', 'MERCEDES-BENZ E350', 'FORD FREESTAR', 'MERCEDES-BENZ SPRINTER', 'CADILLAC ESCALADE', 'MERCURY MONTEREY', 'ACURA MDX', 'DODGE SPRINTER', 'CADILLAC XTS', 'MERCEDES-BENZ GL CLASS', 'GMC YUKON', 'LEXUS RX 350', 'NISSAN QUEST', 'HONDA ACCORD', 'CHEVROLET SUBURBAN', 'FORD EXPLORER', 'TOYOTA VENZA', 'GMC DENALI', 'NISSAN ROGUE', 'FORD', 'AUDI Q7', 'TOYOTA HIGHLANDER', 'LEXUS RX 400H', 'TOYOTA COROLLA', 'CHEVROLET CRUZE', 'CHEVROLET TRAILBLAZER', 'FORD WINDSTAR', 'BMW  7 SERIES', 'HONDA PILOT', 'CADILLAC ESCALADE ESV', 'VOLKSWAGEN ROUTAN', 'FORD FREESTYLE', 'LEXUS GX 460', 'CHRYSLER TOWN AND COUNTRY,TOYOTA HIGHLANDER', 'GMC YUKON XL', 'HONDA CR-V', 'LINCOLN NAVIGATOR', 'UNKNOWN', 'CHRYSLER 300', 'CHRYSLER TOWN AND COUNTRY', 'CHEVROLET MALIBU', 'DODGE CHARGER', 'FORD E350', 'CHEVROLET LUMINA', 'FORD TAURUS WAGON', 'JAGUAR XJ SERIES', 'CHRYSLER TOWN AND COUNTRY,LINCOLN TOWN CAR,', 'LEXUS LS 460', 'DODGE DURANGO', 'FORD FUSION SE', 'MERCEDES-BENZ E-CLASS', 'FORD ESCAPE', 'NISSAN NV200', 'BMW 5 SERIES', 'HONDA ODYSSEY', 'NISSAN PATHFINDER', 'LINCOLN MKT', 'TOYOTA RAV4', 'LINCOLN TOWN CAR', 'TOYOTA CAMRY', 'FORD EXPEDITION', 'BMW 7-SERIES', 'FORD CROWN VICTORIA', 'CHEVROLET EXPRESS,GMC YUKON XL', 'DODGE CARAVAN', 'TOYOTA PRIUS V', 'HONDA ODYSSEY,LINCOLN TOWN CAR,TOYOTA HIGHLANDER', 'NISSAN SENTRA', 'CHEVROLET EXPRESS LT 3500', 'MITSUBISHI MONTERO', 'GMC YUKON DENALI', 'JEEP GRAND CHEROKEE LAREDO', 'FORD FLEX', 'FORD ESCAPE,FORD FUSION,HONDA ODYSSEY', 'TOYOTA PRIUS', 'MERCEDES-BENZ S-CLASS', 'TOYOTA SIENNA', 'HYUNDAI VERACRUZ', 'MERCEDES-BENZ S350', 'NISSAN VERSA', 'FORD TAURUS', 'LEXUS ES 350', 'MERCEDES-BENZ R350', 'LINCOLN MKS', 'MERCURY GRAND MARQUIS', 'CHRYSLER PACIFICA', 'MERCEDES BENZ SPRINTER', 'FORD EXCURSION', 'FORD ESCAPE XLS 4WD', 'FORD C-MAX', 'CHEVROLET TAHOE', 'CHEVROLET UPLANDER', 'MERCEDES-BENZ', 'CADILLAC DTS', 'NISSAN ALTIMA', 'CHRYSLER SEBRING', 'FORD FIVE HUNDRED', 'CHEVROLET IMPALA', 'FORD EDGE', 'MAZDA MAZDA5']
                if v in l:
                    return True
                else:
                    False
            x = col.cast('string')
            return F.count(F.when(f1(x) == True, x))
        
        def is_location_type(col):                     
            @F.udf
            def f1(v):
                l = ['STORE UNCLASSIFIED','PARK/PLAYGROUND','SOCIAL CLUB/POLICY','BUS STOP','HOMELESS SHELTER','AIRPORT TERMINAL',
                     'TRANSIT - NYC SUBWAY','PHOTO/COPY','CLOTHING/BOUTIQUE','CANDY STORE','MARINA/PIER','BUS TERMINAL','COMMERCIAL BUILDING',
                     'PARKING LOT/GARAGE (PRIVATE)','TRAMWAY','GAS STATION','LOAN COMPANY','ATM','PUBLIC SCHOOL','BAR/NIGHT CLUB','RESIDENCE-HOUSE','BOOK/CARD','BEAUTY & NAIL SALON','FOOD SUPERMARKET','BUS (NYC TRANSIT)','HIGHWAY/PARKWAY','SHOE','STORAGE FACILITY',
                     'CHAIN STORE','PRIVATE/PAROCHIAL SCHOOL','ABANDONED BUILDING','SYNAGOGUE','CEMETERY','FACTORY/WAREHOUSE','TUNNEL','HOTEL/MOTEL',
                     'SMALL MERCHANT','MAILBOX OUTSIDE', 'TAXI (YELLOW LICENSED)','RESIDENCE - APT. HOUSE','CONSTRUCTION SITE','MAILBOX INSIDE',
                     'VIDEO STORE','PARKING LOT/GARAGE (PUBLIC)','CHECK CASHING BUSINESS','VARIETY STORE','STREET','LIQUOR STORE','BUS (OTHER)',
                     'PUBLIC BUILDING','JEWELRY','RESTAURANT/DINER','OPEN AREAS (OPEN LOTS)','HOSPITAL','DAYCARE FACILITY','TELECOMM. STORE',
                     'MOSQUE','DEPARTMENT STORE','BANK','TAXI/LIVERY (UNLICENSED)','DRY CLEANER/LAUNDRY','TAXI (LIVERY LICENSED)','TRANSIT FACILITY (OTHER)','FAST FOOD''GROCERY/BODEGA','BRIDGE','DRUG STORE','FERRY/FERRY TERMINAL','OTHER HOUSE OF WORSHIP','RESIDENCE - PUBLIC HOUSING','OTHER','GYM/FITNESS FACILITY','DOCTOR/DENTIST OFFICE','CHURCH']
                if v in l:
                    return True
                else:
                    False
            x = col.cast('string')
            return F.count(F.when(f1(x) == True, x))
        
        
        def is_color(col):                          
            @F.udf
            def f1(v):
                l = ['JADE', 'RED', 'RED APPLE PIN', 'WHITE', 'BLACK', '4 - COLOR LOGO', 'BLACK AND BLACK', 'GOLD APPLE PIN', 'GREEN', 'GRAY', 'HUNTER GREEN', 'ROYAL BLUE', 'SILVER', 'GOLD', 'MULTICOLOR', 'NAVY', 'BLUE PERIWINKLE', 'BROWN', 'BLACK AND GOLD', 'CLEAR', 'GREEN APPLE PIN', 'TURQUOISE', 'NAVY WITH ORANGE LOGO', 'BLUE', 'PINK', 'ORANGE', 'NAVY WITH WHITE LOGO']
                if v in l:
                    return True
                else:
                    False
            x = col.cast('string')
            return F.count(F.when(f1(x) == True, x))
        
        
        def check_s(df, c):            
            
            funs = [                       
                    is_zip_code,
                    is_phone_number,
                    is_school_name,
                    is_street_name,
                    is_lat_lon_cord,
                    is_college_name,
                    is_borough,
                    is_park_playground,
                    is_website,
                    is_business_name,
                    is_subject_in_school,
                    is_neighbourhood,
                    is_city_agency,
                    is_school_level,
                    is_building_classification,
                    is_area_of_study,
                    is_MiddleInitial,
                    is_car_make,
                    is_location_type,
                    is_color,
                    number_of_non_empty_cells
                   ]
            
            def exp():
                t = []
                for f in funs:  
                        t.append(f(F.col(c)).alias('{0}_{1}'.format(f.__name__, c))) 
                 
                return t
        
            ex = iter(exp())
            
            s = df.agg(*ex).toJSON().first()  
            
            r = json.loads(s)                
            
            main_labels = [                    
                    'is_zip_code',
                    'is_phone_number',
                    'is_school_name',
                    'is_street_name',
                    'is_lat_lon_cord',
                    'is_college_name',
                    'is_borough',
                    'is_park_playground',
                    'is_website',
                    'is_business_name',
                    'is_subject_in_school',
                    'is_neighbourhood',
                    'is_city_agency',
                    'is_school_level',
                    'is_building_classification',
                    'is_area_of_study',
                    'is_MiddleInitial',
                    'is_car_make',
                    'is_location_type',
                    'is_color',
                    'number_of_non_empty_cells'
                   ]
            
            l = []
            for f in funs:                                 
                if '{0}_{1}'.format(f.__name__, c) in r.keys() and f.__name__ in main_labels:
                    if r['{0}_{1}'.format(f.__name__, c)] > 0:
                        l.append('{0}_{1}'.format(f.__name__, c))
                
            return l
        
        
        def get_s(df, c, l):              
            
            funs = [
                    is_zip_code,               
                    is_phone_number,
                    is_school_name,
                    is_street_name,
                    is_lat_lon_cord,
                    is_college_name,
                    is_borough,
                    is_park_playground,
                    is_website,
                    is_business_name,
                    is_subject_in_school,
                    is_neighbourhood,
                    is_city_agency,
                    is_school_level,
                    is_building_classification,
                    is_area_of_study,
                    is_MiddleInitial,
                    is_car_make,
                    is_location_type,
                    is_color,
                    number_of_non_empty_cells
                   ]
            columns = df.columns
            
            def exp():
                t = []
                for f in funs:  
                    if '{0}_{1}'.format(f.__name__, c) in l:                        
                        t.append(f(F.col(c)).alias('{0}_{1}'.format(f.__name__, c)))  
                
                if len(t) == 0:
                    t.append(funs[0](F.col(c)).alias('{0}_{1}'.format(funs[0].__name__, c)))  
                
                return t
        
            ex = iter(exp())
            
            return df.agg(*ex).toJSON().first()  
        
        def get_l(s, column_name):
            
            
            
            r = json.loads(s)    
            
            funs = [
                    is_zip_code,             
                    is_phone_number,
                    is_school_name,
                    is_street_name,
                    is_lat_lon_cord,
                    is_college_name,
                    is_borough,
                    is_park_playground,
                    is_website,
                    is_business_name,
                    is_subject_in_school,
                    is_neighbourhood,
                    is_city_agency,
                    is_school_level,
                    is_building_classification,
                    is_area_of_study,
                    is_MiddleInitial,
                    is_car_make,
                    is_location_type,
                    is_color,
                    number_of_non_empty_cells
                   ]
            
            main_labels = [
                    'is_zip_code',            
                    'is_phone_number',
                    'is_school_name',
                    'is_street_name',
                    'is_lat_lon_cord',
                    'is_college_name',
                    'is_borough',
                    'is_park_playground',
                    'is_website',
                    'is_business_name',
                    'is_subject_in_school',
                    'is_neighbourhood',
                    'is_city_agency',
                    'is_school_level',
                    'is_building_classification',
                    'is_area_of_study',
                    'is_MiddleInitial',
                    'is_car_make',
                    'is_location_type',
                    'is_color',
                    'number_of_non_empty_cells'
                   ]
            
            
            
            l = []
        
 
                
            for f in funs:
                if '{0}_{1}'.format(f.__name__, c) in r.keys() and f.__name__ in main_labels and f.__name__ != 'number_of_non_empty_cells':
                    if r['{0}_{1}'.format(f.__name__, c)] > 0:
                        temp = {}
                        temp['semantic_type'] = f.__name__[f.__name__.index('_')+1:]
                        temp['count'] = r['{0}_{1}'.format(f.__name__, c)]          
                        l.append(temp)       
            
            c_list = set(column_name.lower().split('_'))  
            
            if {'last','name'} <= c_list or {'first','name'} <= c_list: 
                temp = {}
                temp['semantic_type'] = 'person_name'   
                temp['count'] = r['{0}_{1}'.format('number_of_non_empty_cells', c)] 
                l.append(temp)                         
                
            if {'vehicle','body','type'} <= c_list:    
                temp = {}
                temp['semantic_type'] = 'vehicle_body_type'   
                temp['count'] = r['{0}_{1}'.format('number_of_non_empty_cells', c)] 
                l.append(temp)                             
                
            if {'vehicle','type','code'} <= c_list:   
                temp = {}
                temp['semantic_type'] = 'vehicle_type_code' 
                temp['count'] = r['{0}_{1}'.format('number_of_non_empty_cells', c)]  
                l.append(temp)                               
            
            if len(l) == 0:                  
                temp = {}
                temp['semantic_type'] = 'other' 
                temp['label'] = column_name                
                temp['count'] = r['{0}_{1}'.format('number_of_non_empty_cells', c)]  
                l.append(temp)
       
            return l  
        
        
        if c:
            if n == 0:
                return check_s(df, c)   
            else:
                s = get_s(df, c, function_list) 
                l = get_l(s, column_name)
                return l                
        else:
            return []

        
    
    
    
    cmd = 'hdfs dfs -ls {}'.format(path)    
    files = subprocess.check_output(cmd, shell=True).decode("utf-8").strip().split('\n')
    files = list(files)
    files = files[1:]
    print('files success')

        
    with open('cluster1.txt', 'r') as f:    
        x = f.readlines()
        
    l = x[0].strip('[').strip('\n').strip(']').split(',')
    column_list = []
    for z in l:
        t = z[z.index('\''):]
        column_list.append(t.strip('\''))    
   
    i = 1
    for filename in column_list:       
        if i <= n:
            
            f, c, _, _ = filename.split('.')
            t = {}
            df = spark.read.format('csv').option("delimiter", "\t").option("header", "false").option("inferschema", "true").csv(str(path + '/' + filename))   #read the dataset in dataframe
            
            if df.count() > 1000:
                df2 = df.sample(False, 0.1, seed=0).limit(100)   
            else:
                df2 = df.sample(False, 0.9, seed=0).limit(100)
            k = stat1(df2, "_c0", 0, [], c)                        
            
            t['column_name'] = c
            t['semantic_types'] = stat1(df, "_c0", 1, k, c)      
            
            df.unpersist()
            with open('task2.json', 'a') as fp:            
                json.dump(t, fp)
                
            print("Completed dataset " + f + ' ' + str(time.time() - start_time)) 
            print("Completed {0} of {1}".format(i, n))
            start_time = time.time()
            i = i + 1
        else:
            break

    return 0

path = str(sys.argv[1])                 
n = int(sys.argv[2])                   

start_time = time.time()
semantic_profiling(path, start_time, n)     
print("--- %s seconds ---" % (time.time() - start_time))  

sc.stop()         
