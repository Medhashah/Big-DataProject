import json
import re
import os

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from matplotlib.colors import is_color_like
from collections import OrderedDict 

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()

######################## Utils ########################

person_first_name_collection = (spark.read.format('csv')
                                .options(inferschema='true')
                                .load('/user/ql1045/proj-in/person-first-names/*.txt')
                                .toDF('value', 'gender', 'count')
                                .select(F.upper(F.col('value')).alias('value'))
                                .distinct()
                                .cache())
person_last_name_collection = (spark.read.format('csv')
                               .options(inferschema='true', header='true')
                               .load('/user/ql1045/proj-in/person-last-names/*.csv')
                               .select(F.upper(F.col('name')).alias('value'))
                               .distinct()
                               .cache())
person_name_collection = person_first_name_collection.union(person_last_name_collection).distinct().cache()
def count_person_name(dataset):
    '''
    Source(first name) https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-national-level-data
    Source(last name)  https://www.census.gov/topics/population/genealogy/data.html
    '''
    explode = (dataset.withColumn('value', F.split(F.col('value'), r'\W+'))
                      .filter(F.size('value') <= 2)
                      .withColumn('value', F.explode('value')))
    return explode.join(person_name_collection, 'value').select(F.sum('count')).collect()[0][0] or 0
    # return explode.join(person_name_collection, 'value').groupBy('value').agg(F.sum('count')).show()


def count_business_name(dataset):   # >0.9 x3, >0.6 x1, >0.5 x3, 0.47 x1
    # name_list = ['CORPORATION', 'INCORPORATED', 'LIMITED', 'CORP.', 'CORP', 'INC.', 'INC', 'LTD.', 'LTD', 'L.L.C.', 'LLC', 'L.C.', 'LC', 'P.C.', 'PC', 'P.L.L.C.', 'PLLC', 'L.L.P.', 'LLP', 'DPC', 'AIA', 'CONSULTING', 'ENGINEERING', 'ARCHITECTURE', 'ARCHITECTS', 'PE', 'RA', 'WORKSHOP', 'CONSTRUCTION', 'MANAGEMENT']
    name_list = ['SERVICE', 'LLC', 'SVCE', 'LIMO', 'TRANSPORTATION', 'LIMOUSINE', 'SERVICES', 'CARS', 'INC.', 'INC', 'C/S', 'ELEGANTE', 'EXPRESS', 'CLASS', 'JUNO', 'LINE', 'GO', 'COACH', 'LYFT-TRI-CITY', 'LEE', 'CORP.', 'TOP', 'TRANSIT', 'SEDAN', 'MAGNA', 'TIME', 'III', 'GLOBAL', 'U.S.A.', 'MINUTEMEN', 'WORLDWIDE', 'CARLINK', '2', 'VIEW', 'GREENRIDE', 'YORK', 'GTS', 'LAPUMA', 'ACROPOLIS', 'CLASSIC', 'JRIDE', 'SVC', 'CLUB', 'ADON', 'DEMAND', 'NURIDE', 'LAND', 'CLIQCAR', 'TRANS', 'UBER', 'LYFT', 'VIA', 'LEE/TRISTAR', 'JUNIORS', 'CITYROAD', 'DEBORAHLIMO', 'INDRIVER', 'RESTAURANT', 'CAFE', 'PIZZA', 'BAR', 'GRILL', 'BAKERY', 'KITCHEN', 'HOUSE', 'SHOP', 'CHICKEN', 'CUISINE', 'PIZZERIA', "DUNKIN'", 'LOUNGE', 'DINER', 'DELI', 'SUBWAY', 'SUSHI', 'GARDEN', 'FOOD', 'COFFEE', 'STARBUCKS', "MCDONALD'S", 'KING', 'TAVERN', 'II', 'BISTRO', 'MARKET', 'PUB', 'BAGELS', 'ROBBINS', 'TEA', 'THAI', 'POPEYES', 'PLACE', 'RAMEN', 'ROOM', 'STEAKHOUSE', "DOMINO'S", 'BURGER', 'RISTORANTE', 'HOTEL', 'PALACE', 'CREAM', 'GOURMET', 'WOK', 'BBQ', 'HALL', 'INN', 'NYC', 'CATERING', 'BROOKLYN', 'JUICE', 'BAGEL', 'BUFFET', 'TAQUERIA', 'COMPANY', 'FUSION', 'CO', 'NY', 'KFC', 'CENTER', 'PASTA', 'BURGERS', 'TASTE', 'CAFETERIA', 'GROCERY', 'HUT', 'QUOTIDIEN', 'NOODLE', 'SPOT', 'BREAD', 'SHACK', 'CHECKERS', 'VILLAGE', 'TRATTORIA', "WENDY'S", 'PARK', 'SALAD', 'DRAGON', 'CHINA', 'TACO', 'EAST', 'EATERY', 'CATERERS', 'BOX', 'CO.', 'SHOPPE', 'FACTORY', 'CORNER', 'CHINESE', 'DUMPLING', "JOHN'S", 'WINGS', 'SUPERMARKET', 'CORP', 'MANGER', 'DONUTS', 'FIKA', 'BANK', 'BENE', 'STREET', 'AVE', 'FOODS', 'CANTINA', 'STEAK', 'ST', 'GENERAL', 'NOODLES', 'STORE', 'LLC.', 'ISLAND', 'YOGURT', 'AVE.', 'BROADWAY', 'BAGUETTE', 'GUYS', 'CORPORATION', 'CREPE', 'PRE-K', 'CAKES', 'MORE', '3)', 'WORKS', 'STATION', 'GROUP', 'CONSULTING', 'SOLUTIONS', 'CONSTRUCTION', 'ASSOCIATES', 'SYSTEMS', 'DESIGN', 'CONSULTANTS', 'PARTNERS', 'ARCHITECTS', 'SUPPLY', 'SUPPLIES', 'STUDIO', 'SECURITY', 'CONTRACTING', 'ENGINEERING', 'ENVIRONMENTAL', 'DEVELOPMENT', 'ENTERPRISES', 'MANAGEMENT', 'TECHNOLOGIES', 'COMMUNICATIONS', 'AGENCY', 'INDUSTRIES', 'MEDIA', 'ELECTRIC', 'PRODUCTS', 'ENGINEERS', 'TRAINING', 'ELECTRICAL', 'ARCHITECTURE', 'PC', 'ENERGY', 'CPA', 'BUILDERS', 'INTERNATIONAL', 'USA', 'ARTS', 'P.C.', 'PRINTING', 'PHOTOGRAPHY', 'INSTITUTE', 'ADVISORS', 'FIRM', 'FILMS', 'PRESS', 'SOCIAL', 'CONTROL', 'SIGNAGE', 'STAFFING', 'STEEL', 'SOLUTION', 'RECYCLING', 'SERIES', 'GLASS', 'MAINTENANCE', 'MARKETING', 'MINDS', 'KINDERDANCE', 'INK', 'INDUSTRIAL', 'PHARMACY', 'PLLC', 'GREEN', 'PR', 'GOOD', 'PRESERVATION', 'DIGITAL', 'PRINT', 'PRODUCTIONS', 'PROJECTS', 'PROMOTIONS', 'FLEET', 'STRATEGIES', 'EVENTS', 'ESTATE', 'RX', 'DIRECT', 'CLEAN', 'BAREBURGER', 'TRIBECA', 'HARLEM', 'LENWICH', 'TAVERNA', 'LANE', 'LADY', 'MILL', '28', 'FREDA', 'GREENE', "MAXWELL'S", 'MARGHERITA', 'BONE', 'DOG', 'ITALIANO', 'STAGECOACH', 'LION', 'STILL', 'BILBOQUET', 'BIANCA', 'CAPHE', 'SERAFINA', 'DELICATESSEN', 'DUMPLINGS', 'EAT', 'CUCINA', 'EATS', 'PEPE', 'ESQUINA', 'ESSENTIALS', 'CHELSEA', 'SALOON', "SARABETH'S", 'ONE', 'JONES', 'ARGENTINO', 'VERDE', 'WOLLENSKY', 'TACOS', 'VINO', 'ANDREA', 'BANTER', 'WEEKENDS', "O'GRADY'S", 'OFFICE', "OG'S", "WATSON'S", 'NOVITA', 'CAFÉ', 'SPICE', 'WALL', 'STAR', 'CITY', 'DELIGHT', 'HING', "CHOP'T", 'TORTILLAS', "LENNY'S", 'FALAFEL', 'COOKIES', 'CLEANERS', 'PIO', '1', 'BURRITO', 'LIQUORS', 'CENTRAL', 'MEXICANA', "SOPHIE'S", 'SICHUAN', 'SPIRITS', "JIMBO'S", 'TOASTIES', 'GINGER', 'EMPANADAS', 'CURRY', 'HEIGHTS', 'STOP', 'MEXICANO', 'LUNCHEONETTE', 'ASIAN', 'ROLL', 'MOONSTRUCK', 'KONG', 'FISH', 'E.', 'MEXICO', 'LEASHES', 'P.E.', 'PE', 'RA', "O'CONNOR", 'ARCHITECT', 'DPC', 'FORMICHELLA', 'P.C', 'LLP', 'ARCH.&ENG.', 'ROMAN', 'DP.C.', 'P.', 'PLLC.', 'DESIGNS', 'ENGINEER', 'R.A.', 'GAO', 'AS', 'LTD', 'AIA', 'PL', 'MOSTER', 'HUANG', 'J.K.DESIGN', 'ARCHITE', 'CROP', 'ADG', 'PLL', 'LOBUE', 'ODIGIE', 'CONCEPT', 'NB&C', 'TAN', 'S', 'SURVEYING', 'GUTERMAN', 'PC.', 'P', 'BUILD', 'RECREATION', 'GAMBA', 'CARMONA', 'PAPAY', 'VAA', 'ASSOC.', 'STUDIOS', 'VISIONACM', 'GENSLER', 'ENGINE', 'DOMANI', 'DESI', 'LL', 'PROFESSIONAL', 'STANTEC', 'ATLANTIC', 'ENG', 'DEMOLITION', 'D.P.C.', 'PLANNERS', 'NAN', 'SER', 'BOLLES', 'ECOPLUSDESIGN', 'ARUP', 'CON', 'WSP', 'LEO', 'CONSULT', 'REPUBLIC', 'AMATO', 'KAUFMAN', 'ZUCK']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if re.sub(r' |,', '$', str(x[0])).split('$')[-1].upper() in name_list else (x[0], 0)).values().sum()
    return count


def count_phone_number(dataset):
    phone_regex = re.compile('(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if phone_regex.match(str(x[0])) else (x[0], 0)).values().sum()
    return count


street_noun_regex = re.compile(r'^\W*(?:STREET|AVENUE|ST|PLACE|AVE|ROAD|COURT|LANE|DRIVE|PARK|BOULEVARD|RD|BLVD|PARKWAY|PL|TERRACE|EXIT|LOOP|EXPRESSWAY|PKWY|PLAZA|BRIDGE|EN|ENTRANCE|DR|ET|BROADWAY|FLOOR|added_by_us|TUNNEL|ROUTE|CIRCLE|WAY|SQUARE|XPWY|EXPY|CRCL|WALK|PKW|CONCOURSE|BOARDWALK|FREEWAY|CHANNEL)\w?\W*$')
door_number_regex = re.compile(r'\d+')


def is_address(data):
    if not data:
        return None
    split = [word for word in data.split(' ') if word]
    for street_noun_index in range(len(split)):
        if street_noun_regex.search(split[street_noun_index]):
            break
    else:
        return None
    for door_number_index in range(street_noun_index - 1):
        if door_number_regex.search(split[door_number_index]):
            return True
    return False


is_address_udf = F.udf(is_address, T.BooleanType())


def count_address(dataset):  # 0.86531622832036702147027053269088
    return dataset.filter(is_address_udf('value')).select(F.sum('count')).collect()[0][0] or 0


def count_street_name(dataset):  # 0.81533175864290315653854412712046
    return dataset.filter(~is_address_udf('value')).select(F.sum('count')).collect()[0][0] or 0


def count_city(dataset):    # most > 0.7, 0.6 x1, 0.5x1, 0.3x1
    # https://data.ny.gov/Government-Finance/New-York-State-Locality-Hierarchy-with-Websites/55k6-h6qq/data
    cities = ['albany', 'montgomery', 'cayuga', 'genesee', 'dutchess', 'broome', 'bronx', 'kings', 'erie', 'ontario', 'steuben', 'cortland', 'chautauqua', 'chemung', 'oswego', 'nassau', 'warren', 'fulton', 'columbia', 'tompkins', 'ulster', 'herkimer', 'niagara', 'new york', 'saratoga', 'orange', 'westchester', 'new york city', 'chenango', 'st lawrence', 'cattaraugus', 'madison', 'otsego', 'clinton', 'queens', 'rensselaer', 'monroe', 'oneida', 'schenectady', 'richmond', 'onondaga', 'jefferson', 'allegany', 'lewis', 'delaware', 'essex', 'franklin', 'greene', 'hamilton', 'wayne', 'livingston', 'orleans', 'putnam', 'rockland', 'schoharie', 'schuyler', 'seneca', 'suffolk', 'sullivan', 'tioga', 'washington', 'wyoming', 'yates', 'amsterdam', 'auburn', 'batavia', 'beacon', 'binghamton', 'brooklyn', 'buffalo', 'canandaigua', 'cohoes', 'corning', 'dunkirk', 'elmira', 'geneva', 'glen cove', 'glens falls', 'gloversville', 'hornell', 'hudson', 'ithaca', 'jamestown', 'johnstown', 'kingston', 'lackawanna', 'little falls', 'lockport', 'long beach', 'manhattan', 'mechanicville', 'middletown', 'mt vernon', 'newburgh', 'new rochelle', 'niagara falls', 'north tonawanda', 'norwich', 'ogdensburg', 'olean', 'oneonta', 'peekskill', 'plattsburgh', 'port jervis', 'poughkeepsie', 'rochester', 'rome', 'rye', 'salamanca', 'saratoga springs', 'sherrill', 'staten island', 'syracuse', 'tonawanda', 'troy', 'utica', 'watertown', 'watervliet', 'white plains', 'yonkers', 'berne', 'bethlehem', 'coeymans', 'colonie', 'green island', 'guilderland', 'knox', 'new scotland', 'rensselaerville', 'westerlo', 'alfred', 'allen', 'alma', 'almond', 'amity', 'andover', 'angelica', 'belfast', 'birdsall', 'bolivar', 'burns', 'caneadea', 'centerville', 'clarksville', 'cuba', 'friendship', 'granger', 'grove', 'hume', 'independence', 'new hudson', 'rushford', 'scio', 'ward', 'wellsville', 'west almond', 'willing', 'wirt', 'barker', 'colesville', 'conklin', 'dickinson', 'fenton', 'kirkwood', 'lisle', 'maine', 'nanticoke', 'sanford', 'triangle', 'union', 'vestal', 'windsor', 'ashford', 'carrollton', 'coldspring', 'conewango', 'dayton', 'east otto', 'ellicottville', 'farmersville', 'franklinville', 'freedom', 'great valley', 'hinsdale', 'humphrey', 'ischua', 'leon', 'little valley', 'lyndon', 'machias', 'mansfield', 'napoli', 'new albion', 'otto', 'perrysburg', 'persia', 'portville', 'randolph', 'red house', 'south valley', 'yorkshire', 'aurelius', 'brutus', 'cato', 'conquest', 'fleming', 'genoa', 'ira', 'ledyard', 'locke', 'mentz', 'montezuma', 'moravia', 'niles', 'owasco', 'scipio', 'sempronius', 'sennett', 'springport', 'sterling', 'summerhill', 'throop', 'venice', 'victory', 'arkwright', 'busti', 'carroll', 'charlotte', 'cherry creek', 'clymer', 'ellery', 'ellicott', 'ellington', 'french creek', 'gerry', 'hanover', 'harmony', 'kiantone', 'turin', 'mina', 'north harmony', 'poland', 'pomfret', 'portland', 'ripley', 'sheridan', 'sherman', 'stockton', 'villenova', 'westfield', 'ashland', 'baldwin', 'big flats', 'catlin', 'erin', 'horseheads', 'southport', 'van etten', 'veteran', 'afton', 'bainbridge', 'columbus', 'coventry', 'german', 'guilford', 'lincklaen', 'mc donough', 'new berlin', 'north norwich', 'otselic', 'oxford', 'pharsalia', 'pitcher', 'plymouth', 'preston', 'sherburne', 'smithville', 'smyrna', 'altona', 'ausable', 'beekmantown', 'black brook', 'champlain', 'chazy', 'dannemora', 'ellenburg', 'mooers', 'peru', 'saranac', 'schuyler falls', 'ancram', 'austerlitz', 'canaan', 'chatham', 'claverack', 'clermont', 'copake', 'gallatin', 'germantown', 'ghent', 'greenport', 'hillsdale', 'kinderhook', 'new lebanon', 'stockport', 'stuyvesant', 'taghkanic', 'cincinnatus', 'cortlandville', 'cuyler', 'freetown', 'harford', 'homer', 'lapeer', 'marathon', 'preble', 'scott', 'solon', 'taylor', 'truxton', 'virgil', 'willet', 'andes', 'bovina', 'colchester', 'davenport', 'delhi', 'deposit', 'hamden', 'hancock', 'harpersfield', 'kortright', 'masonville', 'meredith', 'roxbury', 'sidney', 'stamford', 'walton', 'amenia', 'beekman', 'dover', 'east fishkill', 'fishkill', 'hyde park', 'la grange', 'milan', 'northeast', 'pawling', 'pine plains', 'pleasant valley', 'red hook', 'stanford', 'wappinger', 'alden', 'amherst', 'aurora', 'boston', 'brant', 'cheektowaga', 'clarence', 'colden', 'collins', 'concord', 'eden', 'elma', 'evans', 'grand island', 'hamburg', 'holland', 'lancaster', 'marilla', 'newstead', 'north collins', 'orchard park', 'sardinia', 'wales', 'west seneca', 'chesterfield', 'crown point', 'elizabethtown', 'jay', 'keene', 'minerva', 'moriah', 'newcomb', 'north elba', 'north hudson', 'schroon', 'st. armand', 'ticonderoga', 'westport', 'willsboro', 'wilmington', 'bangor', 'bellmont', 'bombay', 'brandon', 'brighton', 'burke', 'chateaugay', 'constable', 'duane', 'fort covington', 'harrietstown', 'malone', 'moira', 'santa clara', 'tupper lake', 'waverly', 'westville', 'bleecker', 'broadalbin', 'caroga', 'ephratah', 'mayfield', 'northampton', 'oppenheim', 'perth', 'stratford', 'alabama', 'alexander', 'bergen', 'bethany', 'byron', 'darien', 'elba', 'le roy', 'oakfield', 'pavilion', 'pembroke', 'stafford', 'athens', 'cairo', 'catskill', 'coxsackie', 'durham', 'greenville', 'halcott', 'hunter', 'jewett', 'lexington', 'new baltimore', 'prattsville', 'windham', 'arietta', 'benson', 'hope', 'indian lake', 'inlet', 'lake pleasant', 'long lake', 'morehouse', 'wells', 'danube', 'fairfield', 'frankfort', 'german flatts', 'litchfield', 'manheim', 'newport', 'norway', 'ohio', 'russia', 'salisbury', 'stark', 'webb', 'winfield', 'adams', 'alexandria', 'antwerp', 'brownville', 'cape vincent', 'champion', 'clayton', 'ellisburg', 'henderson', 'hounsfield', 'le ray', 'lorraine', 'lyme', 'pamelia', 'philadelphia', 'rodman', 'rutland', 'theresa', 'wilna', 'worth', 'croghan', 'denmark', 'diana', 'greig', 'harrisburg', 'leyden', 'lowville', 'lyonsdale', 'sodus', 'martinsburg', 'montague', 'new bremen', 'osceola', 'pinckney', 'watson', 'west turin', 'avon', 'caledonia', 'conesus', 'geneseo', 'groveland', 'leicester', 'lima', 'livonia', 'mount morris', 'north dansville', 'nunda', 'ossian', 'portage', 'sparta', 'springwater', 'west sparta', 'york', 'brookfield', 'cazenovia', 'de ruyter', 'eaton', 'fenner', 'georgetown', 'lebanon', 'lenox', 'lincoln', 'nelson', 'smithfield', 'stockbridge', 'chili', 'clarkson', 'east rochester', 'gates', 'greece', 'hamlin', 'henrietta', 'irondequoit', 'mendon', 'ogden', 'parma', 'penfield', 'perinton', 'pittsford', 'riga', 'rush', 'sweden', 'webster', 'wheatland', 'canajoharie', 'remsen', 'charleston', 'florida', 'glen', 'minden', 'mohawk', 'palatine', 'root', 'st johnsville', 'hempstead', 'north hempstead', 'oyster bay', 'cambria', 'hartland', 'lewiston', 'newfane', 'pendleton', 'porter', 'royalton', 'somerset', 'wheatfield', 'wilson', 'annsville', 'augusta', 'ava', 'boonville', 'bridgewater', 'camden', 'deerfield', 'florence', 'floyd', 'forestport', 'kirkland', 'lee', 'marcy', 'marshall', 'new hartford', 'paris', 'sangerfield', 'trenton', 'vernon', 'verona', 'vienna', 'western', 'westmoreland', 'whitestown', 'camillus', 'cicero', 'clay', 'dewitt', 'elbridge', 'fabius', 'geddes', 'lafayette', 'lysander', 'manlius', 'marcellus', 'otisco', 'pompey', 'salina', 'skaneateles', 'spafford', 'tully', 'van buren', 'bristol', 'canadice', 'east bloomfield', 'farmington', 'gorham', 'hopewell', 'manchester', 'naples', 'phelps', 'south bristol', 'victor', 'west bloomfield', 'blooming grove', 'chester', 'cornwall', 'crawford', 'deerpark', 'goshen', 'hamptonburgh', 'highlands', 'minisink', 'mount hope', 'new windsor', 'tuxedo', 'wallkill', 'warwick', 'wawayanda', 'woodbury', 'albion', 'barre', 'carlton', 'clarendon', 'gaines', 'kendall', 'murray', 'ridgeway', 'shelby', 'amboy', 'boylston', 'constantia', 'granby', 'hannibal', 'hastings', 'mexico', 'minetto', 'new haven', 'orwell', 'palermo', 'parish', 'redfield', 'richland', 'sandy creek', 'schroeppel', 'scriba', 'volney', 'west monroe', 'williamstown', 'burlington', 'butternuts', 'cherry valley', 'decatur', 'edmeston', 'exeter', 'hartwick', 'laurens', 'maryland', 'middlefield', 'milford', 'morris', 'new lisbon', 'otego', 'pittsfield', 'plainfield', 'richfield', 'roseboom', 'springfield', 'unadilla', 'westford', 'worcester', 'carmel', 'kent', 'patterson', 'philipstown', 'putnam valley', 'southeast', 'berlin', 'brunswick', 'east greenbush', 'grafton', 'hoosick', 'north greenbush', 'petersburgh', 'pittstown', 'poestenkill', 'sand lake', 'schaghticoke', 'schodack', 'stephentown', 'clarkstown', 'haverstraw', 'orangetown', 'ramapo', 'stony point', 'ballston', 'charlton', 'clifton park', 'corinth', 'day', 'edinburg', 'galway', 'greenfield', 'hadley', 'halfmoon', 'malta', 'milton', 'moreau', 'northumberland', 'providence', 'stillwater', 'waterford', 'wilton', 'duanesburg', 'glenville', 'niskayuna', 'princetown', 'rotterdam', 'blenheim', 'carlisle', 'cobleskill', 'conesville', 'esperance', 'gilboa', 'middleburgh', 'richmondville', 'seward', 'sharon', 'summit', 'wright', 'catharine', 'cayuta', 'dix', 'hector', 'montour', 'reading', 'tyrone', 'covert', 'fayette', 'junius', 'lodi', 'ovid', 'romulus', 'seneca falls', 'tyre', 'varick', 'waterloo', 'addison', 'avoca', 'bath', 'bradford', 'cameron', 'campbell', 'canisteo', 'caton', 'cohocton', 'dansville', 'erwin', 'fremont', 'greenwood', 'hartsville', 'hornby', 'hornellsville', 'howard', 'jasper', 'lindley', 'prattsburgh', 'pulteney', 'rathbone', 'thurston', 'troupsburg', 'tuscarora', 'urbana', 'wayland', 'west union', 'wheeler', 'woodhull', 'brasher', 'canton', 'clare', 'clifton', 'colton', 'dekalb', 'de peyster', 'edwards', 'fine', 'fowler', 'gouverneur', 'hammond', 'hermon', 'hopkinton', 'lawrence', 'lisbon', 'louisville', 'macomb', 'madrid', 'massena', 'morristown', 'norfolk', 'oswegatchie', 'parishville', 'piercefield', 'pierrepont', 'pitcairn', 'potsdam', 'rossie', 'russell', 'stockholm', 'waddington', 'babylon', 'brookhaven', 'east hampton', 'huntington', 'islip', 'riverhead', 'shelter island', 'smithtown', 'southampton', 'southold', 'bethel', 'callicoon', 'cochecton', 'fallsburgh', 'forestburgh', 'highland', 'liberty', 'lumberland', 'mamakating', 'neversink', 'thompson', 'tusten', 'barton', 'berkshire', 'candor', 'newark valley', 'nichols', 'owego', 'richford', 'spencer', 'caroline', 'danby', 'dryden', 'enfield', 'groton', 'lansing', 'newfield', 'ulysses', 'denning', 'esopus', 'gardiner', 'hardenburgh', 'hurley', 'lloyd', 'marbletown', 'marlborough', 'new paltz', 'olive', 'plattekill', 'rosendale', 'saugerties', 'shandaken', 'shawangunk', 'wawarsing', 'woodstock', 'bolton', 'hague', 'horicon', 'johnsburg', 'lake george', 'lake luzerne', 'queensbury', 'stony creek', 'thurman', 'warrensburg', 'argyle', 'cambridge', 'dresden', 'easton', 'fort ann', 'fort edward', 'granville', 'greenwich', 'hampton', 'hartford', 'hebron', 'jackson', 'kingsbury', 'salem', 'white creek', 'whitehall', 'arcadia', 'butler', 'galen', 'huron', 'lyons', 'macedon', 'marion', 'palmyra', 'rose', 'savannah', 'walworth', 'williamson', 'wolcott', 'bedford', 'cortlandt', 'eastchester', 'greenburgh', 'harrison', 'lewisboro', 'mamaroneck', 'mount kisco', 'mount pleasant', 'perry', 'new castle', 'north castle', 'north salem', 'ossining', 'pelham', 'pound ridge', 'scarsdale', 'somers', 'yorktown', 'arcade', 'attica', 'bennington', 'castile', 'covington', 'eagle', 'gainesville', 'genesee falls', 'java', 'middlebury', 'orangeville', 'pike', 'sheldon', 'warsaw', 'wethersfield', 'barrington', 'benton', 'italy', 'jerusalem', 'middlesex', 'milo', 'potter', 'starkey', 'torrey', 'rhinebeck', 'union vale', 'ravena', 'menands', 'altamont', 'voorheesville', 'belmont', 'richburg', 'canaseraga', 'port dickinson', 'whitney point', 'endicott', 'johnson city', 'south dayton', 'gowanda', 'delevan', 'weedsport', 'meridian', 'port byron', 'union springs', 'fair haven', 'lakewood', 'sinclairville', 'mayville', 'bemus point', 'celoron', 'falconer', 'forestville', 'silver creek', 'panama', 'fredonia', 'brocton', 'cassadaga', 'wellsburg', 'elmira heights', 'millport', 'earlville', 'keeseville', 'rouses point', 'philmont', 'valatie', 'mc graw', 'fleischmanns', 'margaretville', 'hobart', 'millerton', 'wappingers falls', 'tivoli', 'millbrook', 'williamsville', 'east aurora', 'farnham', 'depew', 'sloan', 'springville', 'angola', 'blasdell', 'akron', 'kenmore', 'port henry', 'lake placid', 'saranac lake', 'brushton', 'northville', 'dolgeville', 'corfu', 'tannersville', 'speculator', 'middleville', 'ilion', 'cold brook', 'west winfield', 'alexandria bay', 'dexter', 'glen park', 'west carthage', 'mannsville', 'sackets harbor', 'black river', 'evans mills', 'chaumont', 'carthage', 'deferiet', 'herrings', 'castorland', 'copenhagen', 'harrisville', 'port leyden', 'lyons falls', 'constableville', 'morrisville', 'canastota', 'wampsville', 'munnsville', 'chittenango', 'honeoye falls', 'spencerport', 'hilton', 'fairport', 'churchville', 'brockport', 'scottsville', 'fort johnson', 'hagaman', 'ames', 'fort plain', 'fultonville', 'fonda', 'nelliston', 'palatine bridge', 'atlantic beach', 'bellerose', 'cedarhurst', 'east rockaway', 'floral park', 'freeport', 'garden city', 'hewlett bay park', 'hewlett harbor', 'hewlett neck', 'island park', 'lynbrook', 'malverne', 'mineola', 'new hyde park', 'rockville centre', 'south floral park', 'stewart manor', 'valley stream', 'woodsburgh', 'baxter estates', 'east hills', 'east williston', 'flower hill', 'great neck', 'great neck estates', 'great neck plaza', 'kensington', 'kings point', 'lake success', 'manor haven', 'munsey park', 'north hills', 'old westbury', 'plandome', 'plandome heights', 'plandome manor', 'port washington north', 'roslyn', 'roslyn estates', 'roslyn harbor', 'russell gardens', 'saddle rock', 'sands point', 'thomaston', 'westbury', 'williston park', 'bayville', 'brookville', 'centre island', 'cove neck', 'farmingdale', 'lattingtown', 'laurel hollow', 'massapequa park', 'matinecock', 'mill neck', 'muttontown', 'old brookville', 'oyster bay cove', 'sea cliff', 'upper brookville', 'middleport', 'youngstown', 'oriskany falls', 'waterville', 'new york mills', 'clayville', 'barneveld', 'holland patent', 'prospect', 'oneida castle', 'sylvan beach', 'oriskany', 'whitesboro', 'yorkville', 'north syracuse', 'east syracuse', 'jordan', 'solvay', 'baldwinsville', 'fayetteville', 'minoa', 'liverpool', 'bloomfield', 'rushville', 'clifton springs', 'shortsville', 'south blooming grove', 'washingtonville', 'cornwall-on-hudson', 'maybrook', 'highland falls', 'unionville', 'harriman', 'kiryas joel', 'walden', 'otisville', 'tuxedo park', 'greenwood lake', 'holley', 'medina', 'lyndonville', 'cleveland', 'central square', 'pulaski', 'lacona', 'phoenix', 'gilbertsville', 'cooperstown', 'richfield springs', 'cold spring', 'nelsonville', 'brewster', 'hoosick falls', 'east nassau', 'valley falls', 'castleton-on-hudson', 'nyack', 'spring valley', 'upper nyack', 'pomona', 'west haverstraw', 'grand view-on-hudson', 'piermont', 'south nyack', 'airmont', 'chestnut ridge', 'hillburn', 'kaser', 'montebello', 'new hempstead', 'new square', 'sloatsburg', 'suffern', 'wesley hills', 'ballston spa', 'round lake', 'south glens falls', 'schuylerville', 'delanson', 'scotia', 'sharon springs', 'odessa', 'montour falls', 'watkins glen', 'burdett', 'interlaken', 'savona', 'riverside', 'south corning', 'painted post', 'arkport', 'north hornell', 'hammondsport', 'rensselaer falls', 'richville', 'norwood', 'heuvelton', 'amityville', 'lindenhurst', 'belle terre', 'bellport', 'lake grove', 'mastic beach', 'old field', 'patchogue', 'poquott', 'port jefferson', 'shoreham', 'sag harbor', 'asharoken', 'huntington bay', 'lloyd harbor', 'northport', 'brightwaters', 'islandia', 'ocean beach', 'saltaire', 'dering harbor', 'head of the harbor', 'nissequogue', 'village of the branch', 'north haven', 'quogue', 'sagaponack', 'westhampton beach', 'west hampton dunes', 'jeffersonville', 'woodridge', 'bloomingburgh', 'wurtsboro', 'monticello', 'freeville', 'cayuga heights', 'trumansburg', 'ellenville', 'hudson falls', 'newark', 'clyde', 'sodus point', 'red creek', 'buchanan', 'croton-on-hudson', 'bronxville', 'tuckahoe', 'ardsley', 'dobbs ferry', 'elmsford', 'hastings-on-hudson', 'irvington', 'tarrytown', 'larchmont', 'briarcliff manor', 'pleasantville', 'sleepy hollow', 'pelham manor', 'port chester', 'rye brook', 'silver springs', 'penn yan', 'dundee']
    def check(city):
        result = [i for i in cities if i in city]
        if len(result) > 0:
            return True
        else:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(str(x[0]).lower()) else (x[0], 0)).values().sum()
    return count


def count_neighborhood(dataset):    # most > 0.9, 0.8 x2
    # https://en.wikipedia.org/wiki/Neighborhoods_in_New_York_City    
    neighborhoods = ['melrose', 'mott haven', 'port morris', 'hunts point', 'longwood', 'claremont', 'concourse village', 'crotona park', 'morrisania', 'concourse', 'highbridge', 'fordham', 'morris heights', 'mount hope', 'university heights', 'bathgate', 'belmont', 'east tremont', 'west farms', 'bedford park', 'norwood', 'university heights', 'fieldston', 'kingsbridge', 'kingsbridge heights', 'marble hill', 'riverdale', 'spuyten duyvil', 'van cortlandt village', 'bronx river', 'bruckner', 'castle hill', 'clason point', 'harding park', 'parkchester', 'soundview', 'unionport', 'city island', 'co-op city', 'locust point', 'pelham bay', 'silver beach', 'throgs neck', 'westchester square', 'allerton', 'bronxdale', 'indian village', 'laconia', 'morris park', 'pelham gardens', 'pelham parkway', 'van nest', 'baychester', 'edenwald', 'eastchester', 'fish bay', 'olinville', 'wakefield', 'williamsbridge', 'woodlawn', 'greenpoint', 'williamsburg', 'boerum hill', 'brooklyn heights', 'brooklyn navy yard', 'clinton hill', 'dumbo', 'fort greene', 'fulton ferry', 'fulton mall', 'vinegar hill', 'bedford-stuyvesant', 'ocean hill', 'stuyvesant heights', 'bushwick', 'city line', 'cypress hills', 'east new york', 'highland park', 'new lots', 'starrett city', 'carroll gardens', 'cobble hill', 'gowanus', 'park slope', 'red hook', 'greenwood heights', 'sunset park', 'windsor terrace', 'crown heights', 'prospect heights', 'weeksville', 'crown heights', 'prospect lefferts gardens', 'wingate', 'bay ridge', 'dyker heights', 'fort hamilton', 'bath beach', 'bensonhurst', 'gravesend', 'mapleton', 'borough park', 'kensington', 'midwood', 'ocean parkway', 'bensonhurst', 'brighton beach', 'coney island', 'gravesend', 'sea gate', 'flatbush', 'kensington', 'midwood', 'ocean parkway', 'east gravesend', 'gerritsen beach', 'homecrest', 'kings bay', 'kings highway', 'madison', 'manhattan beach', 'plum beach', 'sheepshead bay', 'brownsville', 'ocean hill', 'ditmas village', 'east flatbush', 'erasmus', 'farragut', 'remsen village', 'rugby', 'bergen beach', 'canarsie', 'flatlands', 'georgetown', 'marine park', 'mill basin', 'mill island', 'battery park city', 'financial district', 'tribeca', 'chinatown', 'greenwich village', 'little italy', 'lower east side', 'noho', 'soho', 'west village', 'alphabet city', 'chinatown', 'east village', 'lower east side', 'two bridges', 'chelsea', 'clinton', 'hudson yards', 'midtown', 'gramercy park', 'kips bay', 'rose hill', 'murray hill', 'peter cooper village', 'stuyvesant town', 'sutton place', 'tudor city', 'turtle bay', 'waterside plaza', 'lincoln square', 'manhattan valley', 'upper west side', 'lenox hill', 'roosevelt island', 'upper east side', 'yorkville', 'hamilton heights', 'manhattanville', 'morningside heights', 'harlem', 'polo grounds', 'east harlem', "randall's island", 'spanish harlem', 'wards island', 'inwood', 'washington heights', 'astoria', 'ditmars', 'garden bay', 'long island city', 'old astoria', 'queensbridge', 'ravenswood', 'steinway', 'woodside', 'hunters point', 'long island city', 'sunnyside', 'woodside', 'east elmhurst', 'jackson heights', 'north corona', 'corona', 'elmhurst', 'fresh pond', 'glendale', 'maspeth', 'middle village', 'liberty park', 'ridgewood', 'forest hills', 'rego park', 'bay terrace', 'beechhurst', 'college point', 'flushing', 'linden hill', 'malba', 'queensboro hill', 'whitestone', 'willets point', 'briarwood', 'cunningham heights', 'flushing south', 'fresh meadows', 'hilltop village', 'holliswood', 'jamaica estates', 'kew gardens hills', 'pomonok houses', 'utopia', 'kew gardens', 'ozone park', 'richmond hill', 'woodhaven', 'howard beach', 'lindenwood', 'richmond hill', 'south ozone park', 'tudor village', 'auburndale', 'bayside', 'douglaston', 'east flushing', 'hollis hills', 'little neck', 'oakland gardens', 'baisley park', 'jamaica', 'hollis', 'rochdale village', 'st. albans', 'south jamaica', 'springfield gardens', 'bellerose', 'brookville', 'cambria heights', 'floral park', 'glen oaks', 'laurelton', 'meadowmere', 'new hyde park', 'queens village', 'rosedale', 'arverne', 'bayswater', 'belle harbor', 'breezy point', 'edgemere', 'far rockaway', 'neponsit', 'rockaway park', 'arlington', 'castleton corners', 'clifton', 'concord', 'elm park', 'fort wadsworth', 'graniteville', 'grymes hill', 'livingston', 'mariners harbor', 'meiers corners', 'new brighton', 'port ivory', 'port richmond', 'randall manor', 'rosebank', 'st. george', 'shore acres', 'silver lake', 'stapleton', 'sunnyside', 'tompkinsville', 'west brighton', 'westerleigh', 'arrochar', 'bloomfield', 'bulls head', 'chelsea', 'dongan hills', 'egbertville', 'emerson hill', 'grant city', 'grasmere', 'midland beach', 'new dorp', 'new springville', 'oakwood', 'ocean breeze', 'old town', 'south beach', 'todt hill', 'travis', 'annadale', 'arden heights', 'bay terrace', 'charleston', 'eltingville', 'great kills', 'greenridge', 'huguenot', 'pleasant plains', "prince's bay", 'richmond valley', 'rossville', 'tottenville', 'woodrow']
    def check(neighborhood):
        result = [i for i in neighborhoods if i in neighborhood]
        if len(result) > 0:
            return True
        else:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(str(x[0]).lower()) else (x[0], 0)).values().sum()
    return count


def count_coordinates(dataset):
    coordinate_regex = re.compile('(\((-)?\d+(.+)?,( *)?(-)?\d+(.+)?\))')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if coordinate_regex.match(str(x[0])) else (x[0], 0)).values().sum()
    return count


def count_zip(dataset):
    zip_regex = re.compile('^[0-9]{5}([- /]?[0-9]{4})?$')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if zip_regex.match(str(x[0])) else (x[0], 0)).values().sum()
    return count


def count_borough(dataset):  # 0.92
    boro = ['K', 'M', 'Q', 'R', 'X']
    borough = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if (str(x[0]).upper() in boro or str(x[0]).upper() in borough) else (x[0], 0)).values().sum()
    return count


def count_school_name(dataset): # most >0.7, >0.5 x2 (school_name & parks_or_playgrounds)       0.24, 0.58, 0.97 for Park_Facility_Name contains schools
    school_name_list = ['J.H.S. ', 'JHS ', 'M.S. ', 'MS ', 'P.S. ', 'PS ', 'I.S. ', 'IS ', 'ACADEMY', 'SCHOOL', 'CENTER']
    name_prefix = ['P.S.', 'THE', 'I.S.', 'J.H.S.', 'HS', 'PS', 'IS', 'JHS', 'MS', 'ALTERNATIVE', 'PRE-K']
    name_suffix = ['TECHNOLOGY', 'ARTS', 'STUDIES', 'YABC', 'JUSTICE', 'CENTER', 'PRESCHOOL']
    def check(name):
        if 'SCHOOL' in name or 'ACADEMY' in name:
            return True
        try:
            name = name.split(' ')
            return (name[0] in name_prefix or name[-1] in name_suffix)
        except Exception as e:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(str(x[0]).upper()) else (x[0], 0)).values().sum()
    return count


def count_color(dataset):   # 0.7
    # https://www.ul.com/resources/color-codes-and-abbreviations-plastics-recognition
    color_list = ['AL', 'AM', 'AO', 'AT', 'BG', 'BK', 'BL', 'BN', 'BZ', 'CH', 'CL', 'CT', 'DK', 'GD', 'GN', 'GT', 'GY', 'IV', 'LT', 'NC', 'OL', 'OP', 'OR', 'PK', 'RD', 'SM', 'TL', 'TN', 'TP', 'VT', 'WT', 'YL']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if (is_color_like(x[0]) or x[0] in color_list) else (x[0], 0)).values().sum()
    return count


def count_car_make(dataset):    # most >0.94
    makes = ['FORD', 'TOYOT', 'HONDA', 'NISSA', 'CHEVR', 'FRUEH', 'ME/BE', 'BMW', 'DODGE', 'JEEP', 'HYUND', 'GMC', 'LEXUS', 'INTER', 'ACURA', 'CHRYS', 'VOLKS', 'INFIN', 'SUBAR', 'AUDI', 'ISUZU', 'KIA', 'MAZDA', 'NS/OT', 'MITSU', 'LINCO', 'CADIL', 'VOLVO', 'ROVER', 'MERCU', 'HIN', 'HINO', 'KENWO', 'WORKH', 'BUICK', 'PETER', 'MINI', 'MACK', 'PORSC', 'SMART', 'PONTI', 'SATUR', 'JAGUA', 'FIAT', 'UD', 'SUZUK', 'SAAB', 'MI/F', 'UTILI', 'WORK', 'SCION', 'VANHO', 'VESPA', 'OLDSM', 'STERL', 'UTIL', 'MASSA', 'HUMME', 'KAWAS', 'YAMAH', 'HARLE', 'TESLA', 'PLYMO', 'MCI', 'UPS', 'TRIUM', 'THOMA', 'BENTL', 'MASE', 'FONTA', 'PREVO', 'SPRI', 'FERRA', 'KW', 'NAVIS', 'IC', 'TRAIL', 'GEO', 'DUCAT', 'HYUN', 'GREAT', 'VPG', 'CHECK', 'STARC', 'KENTU', 'STAR', 'WO/C', 'MER', 'DORSE', 'FRIE', 'WHITE', 'LND', 'FR/LI', 'SMITH', 'CARGO', 'EASTO', 'BL/B', 'UNIFL', 'JENSE', 'HUMBE', 'EAST', 'ROLLS', 'FUSO', 'SOLEC', 'GRUMM', 'BL/BI', 'ANCIA', 'FRH', 'FR', 'LA', 'FRE', 'FRLN', 'UT/M', 'DIAMO', 'PET', 'GIDNY', 'UNIV', 'PIAGG', 'AUTOC', 'ALFAR', 'DAEWO', 'WO/H', 'TESL', 'VAN', 'AUSTI', 'BERTO', 'FRGHT', 'FR/L', 'FHTL', 'AMC', 'WABAS', 'SUNBE', 'LAMBO', 'MICKE', 'FRIEG', 'MG', 'FRT', 'TRL', 'ORION', 'KENW', 'CHER', 'PREV', 'FREG', 'STRIC', 'FRD', 'COLLI', 'WH', 'FRTL', 'LOTUS', 'TRUCK', 'F/L', 'DATSU', 'COACH', 'HIGHW', 'MAYBA', 'GNC', 'RO/R', 'FEDEX', 'TES', 'WABA', 'MI', 'L', 'OTHR', 'MAS', 'FRL', 'FRIGH', 'VA/H', 'STUDE', 'BRIDG', 'WAB', 'FRIG', 'FL', 'VANH', 'AS/M', 'RENAU', 'MORGA', 'PEUGE', 'FRGH', 'FRHT', 'CITRO', 'KYMCO', 'THEUR', 'TRAC', 'APRIL', 'G', 'BSA', 'STR', 'INTR', 'WO/CH', 'BLUE', 'FRLI', 'GD', 'LAYTO', 'GRUEH', 'ELDO', 'INTEL', 'KTM', 'CIMC', 'NE/FL', 'MOTOR', 'FRG', 'ALEXA', 'PETE', 'GENES', 'WANC', 'FISKE', 'KI/M', 'TLR', 'TR/TE', 'K', 'FT/LI', 'AJAX', 'BUS', 'INTIL', 'WO/HO', 'OSHKO', 'GRE', 'BLUEB', 'PB', 'MAGIR', 'FREGH', 'FERIG', 'REFG', 'M-B', 'ENFIE', 'SMA', 'ZENIT', 'FR/LN', 'GREIG', 'FRGT', 'TRLR', 'WKHS', 'MICK', 'AERO', 'WORKS', 'WRK', 'TITAN', 'UTI', 'WINNE', 'VA/HO', 'FR-L', 'MINIC', 'BENSO', 'DELV', 'INDIA', 'MOTOG', 'VICTO', 'DELOR', 'GILL', 'CUB', 'ME', 'UNCB', 'AM/T', 'WK/H', 'ANKA', 'DEMA', 'CASE', 'WE/ST', 'CA/AM', 'PRE', 'CHEET', 'KI/MO', 'UHAUL', 'CHY', 'INIFI', 'STRI', 'WORTH', 'HERCU', 'PTR', 'FRI', 'FREI', 'NEWFL', 'BEN', 'AL/R', 'WO', 'MINC', 'CORBE', 'RA/RO', 'UNIVE', 'TRA', 'MVONE', 'GRU', 'HYTR', 'PTRB', 'EZL', 'WINN', 'WORKV', 'GILLI', 'TEMSA', 'TEMS', 'SEMI', 'HUN', 'COPE', 'WES', 'H', 'WV', 'FREIT', 'SUZU', 'SETR', 'LADA', 'FREIGHTLINER', 'KENWORTH', 'PETERBILT', 'INTERNATIONAL', 'LIEBHERR', 'PETERBUILT', 'WESTERN', 'STERLING', 'INTL', 'MAC', 'FREIGHT', 'INT', 'FRIGHTLINER', 'KEN', 'GROVE', 'RAM', 'WINNEBAGO', 'PUTZMEISTER', 'MARK', 'FRTLR', 'PROSTAR', 'W', 'WS', 'KEA', 'GOLDHOFFER', 'FRIEGHTLINER', 'FREIGHTINER', 'TEREX', 'LINK', 'FREIGHLINER', 'PRO', 'WESTERNSTAR', 'FREINGLINER', 'FORT', 'STERN', 'SCOTT', 'FREIG', 'CHEVY', 'PETEBUILT', 'ARGOSY', 'LINKBELT', 'PREVOST', 'HADDAD', 'FGHT', 'INTERNATIONALL', 'SUP', 'FERIGHT', 'KENWRTH', 'FONTAINE', 'GREATDANE', 'PETBLT', 'FREIGHTLINERGHTLINER', 'BOUNDER', 'FLEETWOOD', 'CHEVROLET', 'PLAYMOR', 'FRTLNR', 'PETERBELT', 'FORDF550', 'PBILT', 'PETEBILT', 'ANGEL', 'TADANO', 'WSTAR', 'INTERSTATE', 'T-80', 'MONACO', 'INTERNATIONAL;', 'TOR', 'PETERBULIT', 'STLG', 'SCHWING', 'KENWROTH', 'KENWOORTH', 'SAFARI', 'VALVO', 'MARKLINE', 'WARA', 'FREIHTLINER', 'OSHKOSH', 'HOLIDAY', 'CAT', 'HOLLSTER', 'JUNTTAN', 'WEST', 'SPARTAN', 'GULF', 'IPROSTAR', 'PIERCE', 'FREIGHTTLINER', 'KMAG', 'FREIGHTLINE', 'PETERBILET', 'AUTOCAR', 'VOVLO', 'DOOSAN', 'PERTERBILT', 'DUTCHSTAR', 'CATERPILLAR', 'DOGE', 'KENTORTH', 'ITHACA', 'PETERBULT', 'FREIGH', 'LEIBHER', 'LINK-BELT', 'PENN', 'STER', 'UTILITY', 'KENWEORTH', 'TALBERT', '2004', 'WSTR', 'ENTYRE', 'INTTERNATIONAL', 'AIRSTREAM', 'INERNATIONAL', 'MACK/PUTZMEISTER', 'IN', 'ENT', 'DOTS', 'LIEBHER', 'GMK', 'INTERN', 'MONOKO', 'MANAC', 'FTL', 'PETERILT', 'FREGHTLINER', 'INTERNIATIONAL', 'FRLTNR', 'CHEV', 'WESTER', 'PETERBIILT', 'F-LINER', 'PV', 'FREIGHTLLINER', '2016', 'FLEET', 'RED', 'PETRBILT', 'FREIGHLINTER', 'FOREST', 'PRES-VAC', 'PROVOST', 'EAGLE', 'ROSENBAUER', 'W/S', '780', '9400I', 'WILSON', 'WHITE-GMC', 'WHGM', 'BUD', 'CHAPP', 'VOVO', 'F', 'CUSTOM', 'TRANSCRAFT', 'TRAILKING', 'TRACTOR', 'DOMINATOR', 'TALBOT', 'KEMWORTH', 'GULFSTREAM', 'KENTWORTH', 'MA', 'KENWORHT', 'KENWOTH', 'PBT', 'INTNL', 'MANIC', 'LIBEHERR', 'FRREIGHTLINER', 'KENSWORTH', 'KNWORTH', 'TRAILMOBLE', 'INTERNATIOAL', 'KN', 'FRIEHGLINER', 'INTERNATINAL', 'KS', 'DISCOVERY', 'DESTINATION', 'LEIBERR', 'TALENT', 'DORSEY', 'DIDGE', 'CIFA', 'VOLO', 'INTERNTIONAL', 'AG16439', 'KENTUCKY', 'INTRNATIONAL', 'WETERN', 'WESTRN', 'KENWORH', 'KENWORTTH', 'KEWORTH', 'WESERN', 'CHEVEROLET', 'WABASH', 'DUNNAN', 'INTERNATIONAL.', 'CUST', 'DUTS', 'DUST', 'FTRL', 'FREGITHLINER', 'P886719', 'PATERBUILT', 'FRWEIGHTLINER', 'FREIGHTLIENR', 'FREIGHTLINERNR', 'PETERBUIT', 'PETERBUILE', 'PETERBILTT', 'PETERBILTIL', 'FREIGHTLINGER', 'FREIGHTLINRE', 'FRTK', 'FREIGHTLNIER', 'PETEERBILT', 'P758014', 'FREEIGHTLINER', 'DUTCH', 'NUTALL', 'FRIEHGTLINER', 'INTEERNATIONAL', 'EMPTY', 'LKENWORTH', 'HYSTER', 'MAKC', 'MARATHON', 'PWTERBUILT', 'FERIGHTLINER', 'MARKINE', 'GM', 'PTERBUILT', 'PTERBILT', 'NACK', 'FREIGHTLINERS', 'TOYOTA', 'OLDS', 'NISSAN', 'PONTIA', 'UNKNOW', 'UNK', 'DATSUN', 'MERCUR', 'CADDY', 'CHEVRO', 'PONT', 'LINCOL', 'NAN', 'PLYMOU', 'CHRYSL', 'VW', 'OLDSMO', 'MITSUB', 'SUBARU', 'HYUNDA', 'PLYM', 'TOY', 'MERC', 'CADI', 'CADILL', 'BIN', 'RENAUL', 'VOLKSW', 'LINC', 'PLY', 'NV', 'INFINI', 'NVA', 'MERCED', 'SUZUKI', 'HYUNDI', 'CADDI', 'SATURN', 'UNKN', 'PLYMTH', 'CHEV.', 'UK', 'PONTIC', 'UNKNWN', 'CAD', 'YAMAHA', 'DATSON', 'CADILA', 'UNKOWN', 'CHRY', 'PONT.', 'LINCON', 'BOAT', 'NISS', 'KAWASA', 'CHEVVY', 'CADDIL', 'OLDS.', 'DATS', 'JAGUAR', 'PONITA', 'CHEVOL', 'UNKWN', 'MADZA', 'NISSIA', 'PLYMOT', 'MERCRY', 'CHEVER', 'PONTAC', 'V.W.', 'YUGO', 'PEUGEO', 'CHEVEY', 'PLYM.', 'UNKNON', 'UKNOWN', 'OLSMBL', 'PONTY', 'SUBURU', 'DAT', 'BURNT', 'OLD', 'ACCURA', 'CADY', 'MAZADA', 'HYNDAI', 'NISSON', 'CHEVY.', 'BENZ', 'UNK.', 'PORSCH', '?', 'CHEVE', 'MERC.', 'MERCY', 'V/W', 'UKN', 'PONTAI', 'V', 'TRAILE', 'MITS', 'LINCLN', 'STERLI', 'CADLAC', 'RENALT', 'MERCUY', 'HUNDAI', 'CHRYST', 'CHEY', 'CADDIE', 'UN', 'PLYMON', 'SHELL', 'FRUEHA', 'CHRYS.', 'POINT', 'PEUGOT', 'TOTOTA', 'CHEVOR', 'TOYTOA', 'CHRYLS', 'TOYOTO', 'TOYTA', 'N/V', 'G.M.C.', 'BAYLIN', 'PLYMUT', 'MAXIMA', 'TRIUMP', 'KAWASK', 'CLOTHI', 'TOYOYA', 'VOLK', 'LANDRO', 'NONE', 'SEARAY', 'INFINT', 'CHRYL', 'V.W', 'CHRSYL', 'MEBE', 'B.M.W.', 'VOLKWA', 'MERKUR', 'COLT', 'IZUZU', 'LINC.', 'CHYSLE', 'PONTI.', 'SABARU', 'LINCL', 'UNKNO', 'RENULT', 'OLDSMB', 'VOXWGN', 'SUB', 'TOYO', 'STRICK', 'DAEWOO', 'NEON', 'TOYATA', 'CHRYLE', 'ALFA', 'U/N', 'TOYATO', 'LINCLO', 'CAMARO', 'CONTAI', 'CUSHMA', 'RENAUT', 'PLYMT', 'CHRYLR', 'A', 'OLDMOB', 'OPEL', 'UTILIT', 'CHEROK', 'CRYSLE', 'MITSHU', 'RANGE', 'PLY.', 'MITSUI', 'KENWOR', 'MECURY', 'CHYRSL', '0LDS', 'GRUMAN', 'B', 'SEADOO', 'MITZ', 'CAPRI', 'PORCHE', 'CHRSY', 'CLARK', 'MITSIB', 'PETERB', 'CHEVI', 'GRUMMA', 'A.M.C.', 'CADILC', 'VILTEX', 'CHEVEL', 'CHRSLE', 'CONT', 'M.BENZ', 'PLMOUT', 'TRAILR', 'SUBAUR', 'CB', 'TRAILM', 'NISAN', 'IVECO', 'N/S', 'BOX', 'TOTOYA', 'CHEUY', 'DUMPST', 'U/K', 'CHEVYQ', 'RENKEN', 'SUBARA', 'SUBURA', 'HYUDAI', 'VOLKWG', 'JETTA', 'M/BENZ', 'MITSB', 'KRYSLA', 'GREATD', 'N\\A', 'TOYTOT', 'MITISU', 'CHRSLY', 'SABB', 'NAV', 'BX', 'G.M.C', 'TL', 'CORVET', 'OURNEI', 'CONTIN', 'HYUAND', 'CUTLAS', 'GINDY', 'M', 'STL', 'NS', 'WOR', 'VN/H', 'FT', 'RTS', 'WK', 'JINO', 'GOV', 'KRYS', 'WH/GM', 'SIMCA', 'THO', 'COLL', 'VAHO', 'BRTO', 'SCIO', 'BERI', 'ST/A', 'STE', 'KENMO', 'NO/BU', 'RA', 'TOYA', 'NAVI', 'INTIN', 'GIL', 'FLIN', 'FISK', 'VESP', 'LODC', 'ALMAC', 'WKH', 'TRITO', 'SPECT', 'SMRT', 'TRAN', 'FRIEH', 'FOD', 'ORIO', 'EAS', 'LA/RD', 'FT/LN', 'RYDER', 'PRATT', 'GSCR', 'OTHER', 'HIND', 'MIC', 'WORH', 'FEDE', 'METZ', 'HEIL', 'W/H', 'MAYB', 'FI', 'WHOR', 'FR/LT', 'MCLA', 'GW', 'VA', 'THEU', 'MCY', 'DORS', 'METRO', 'K/W', 'GARDE', 'P/B', 'SKODA', 'FLORE', 'FTLNR', 'VOIKS', 'WOKH', 'ZIM', 'HINOD', 'MAERS', 'MAXON', 'OLDMO', 'TEX', 'R', 'AM/TR', 'AMTR', 'ADD', 'WRKHR', 'WKHRS', 'KENI', 'PEDIC', 'ST/Q', 'HINI', 'AMTRA', 'MO/V', 'FREGT', 'FT/L', 'TNTE', 'SYM', 'GRD', 'TOMOS', 'MITSUBISHI', 'NISSAN-UD', 'WHITE-GMC-VOLVO', 'MERCEDES', 'SCANIA', 'BERING', 'PACKARD', 'FRUEHAUF', 'CRANE', 'SPECTOR', 'NAVISTAR', 'HACKNEY', 'LINCOLN', 'CADILLAC', 'MERCEDES-BENZ', 'CHRYSLER', 'MERCURY', 'HYUNDAI', 'UNKNOWN', 'VOLKSWAGEN', 'SUV', 'SEDAN', 'CAMRY', 'ALTIMA', 'CAR', 'MUSTAN', 'MOTORC', 'IMPALA', 'LAND', 'ASTRO', 'TAURUS', 'GRAND', 'TAXI', 'CIVIC', 'SUDAN', 'WAGON', 'RV', 'GALANT', 'HONDAI', 'SCOOTE', 'CAB', 'ACCORD', 'NOT', 'MINIVA', 'BIKE', 'CROWN', 'TOWN', 'CARAVA', 'BMV', 'CRYSTL', 'CADI.', 'DOGDE', 'BLAZER', 'MC', 'CADALL', 'LIMO', 'MERCAD', 'MOPED', 'PICKUP', 'HARLEY', 'TRACTO', 'QUEST', 'LICOLN', 'SABLE', 'SENTRA', 'MERCE', 'PATHFI', 'CAMPER', 'LIN', 'MALIBU', 'UNSURE', 'EXPLOR', 'PASSAT', 'HUMMER', 'VOLSWA', 'TOW', 'BRONCO', 'COUGAR', 'HUNDA', 'COOPER', 'TRAILO', 'HANDA', 'AMERIC', 'RANGER', 'VOLKS.', 'BICYCL', 'TR', '2', 'MAX', 'MULTIP', 'YUKON', 'SADAN', 'ESCALA', 'SUBURB', 'CAMARY', 'VARIOU', 'CAMERY', 'VOLTSW', 'MIT', 'TAHOE', 'CYCLE', 'VOYAGE', 'DURANG', 'BLACK', 'PT', 'CADALA', 'COVERE', 'MANY', 'MB', 'MECEDE', 'ANON', 'VOLTS', 'TOWNCA', 'SMALL', 'MERCES', 'ECLIPS', 'MAXIMU', 'ULTIMA', 'THUNDE', 'ALL', 'MISUBI', 'CAMERO', 'PACIFI', 'UNKNW', 'KEEP', 'COROLL', 'NO', 'LEX', 'MITZUB', 'JAQUAR', 'MISTIB', '4', 'CADALI', 'SONATA', 'LEXIS', 'NIU', 'REVEL', 'TSM', 'ACUR', 'LEXU', 'SETRA', 'ISUZ', 'INFI', 'SUBA', 'C/C', 'CANAM', 'VOLV', 'GCR', 'GV', 'DCLI', 'MO/GU', 'R/R', 'POLAR', 'VNHL', 'TA/TA', 'USPS', 'YAMAS', 'DUC', 'CUSTO', 'VOLZ', 'MAZD', 'MO/VE', 'IZ', 'LIEBH', 'ST/CR', 'KAITO', 'NRS', 'NIV', 'L/R', 'LAM', 'SMAR', 'BUIC', 'LX', 'COL', 'ME/', 'TELSA', 'RAN', 'ZEN', 'SLING', 'ZENTI', 'CHANG', 'MT', 'ST/QU', 'LAR', 'INTE', 'DAYCO', 'ROGER', 'ZHNG', 'STOUG', 'DUNBA', 'GO', 'MCB', 'AC', 'ARUCA', 'MICRO', 'WELLS', 'GEN', 'BAODI', 'BUELL', 'HOD', 'GLBEN', 'BUGAT', 'STREA', 'SMR', 'PTRBL', 'INF', 'MCLAR', 'GMC.', 'TRIM', 'JO/DE', 'IS', 'GENUI', 'HYOSU', 'FOUNT', 'KAUFM', 'PGO', 'MS', 'PSD', 'DOG', 'SET', 'HUD', 'CONV', 'MI/BI', 'TAOTA', 'GENE', 'UTL', 'KAWA', 'MOTO', 'GENIS', 'TRUM', 'PREVE', 'V-W', 'GSB', 'FLXZ', 'WINNI', 'NATIO', 'JINDO', 'LA/RO', 'MEBENZ', 'PIAGGI', 'MERBEN', 'ME/BEN', '_FORD', 'DUCATI', 'MBENZ', 'CHYSTL', 'NISAAN', 'MERSED', 'MER/BE', 'PORSHE', '_HONDA', 'MISTU', 'ME/BZ', 'MISTUB', 'AMTRAN', 'FRIEGH', 'STELLA', 'CADILI', 'BENTLE', 'FOED', 'JAG', 'NISSN', 'TORPED', 'LARO', 'CHECY', 'CHERVO', 'M/B', 'ME/BEZ', 'SUSUKI', 'TAOTAO', 'MERCDS', 'INTERC', 'FLEETW', 'MERCDE', 'TAILG', 'MITZU', 'MELBE', 'BLUEBI', 'ME-BE', 'CHYRS', 'VULCAN', 'CHERV', '_BMW', 'WINNEB', 'ME\\BE', 'UTILIM', 'VENTO', 'CHRSLR', 'MURCUR', 'HAULMA', 'APRILI', 'STARCR', 'PEOPLE', 'SPRINT', '_AUDI', 'HOND', 'HYNDUN', 'FRIGHT', 'MITUSB', 'KARAVA', 'HONDAS', 'HINDA', 'IZUSU', 'MITT', 'MC5415', 'DERBI', 'APRILL', 'MASERA', 'TALIG', 'MERCBE', 'CHYRST', 'ROLLSR', 'LROVER', 'NOMAD', 'IFINIT', 'PIAGIO', "INT'L", 'MERCER']
    def check(car):
        try:
            make = car.split(' ')[0]
        except Exception as e:
            make = car
        return make in makes
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(str(x[0]).upper()) else (x[0], 0)).values().sum()
    return count


def count_city_agency(dataset): # most >0.8, AGENCY_NAME 0.23, 0.67, Agency_ID 0.0
    # https://a856-cityrecord.nyc.gov/Home/AgencyAcronyms
    agencies = ['ACS', "ADMINISTRATION FOR CHILDREN'S SERVICES", 'BIC', 'BUSINESS INTEGRITY COMMISSION', 'BNYDC', 'BROOKLYN NAVY YARD DEVELOPMENT CORP', 'BOC', 'BOARD OF CORRECTION', 'BPL', 'BROOKLYN PUBLIC LIBRARY', 'BSA', 'BOARD OF STANDARDS AND APPEALS', 'BXDA', 'BRONX DISTRICT ATTORNEY', 'CCRB', 'CIVILIAN COMPLAINT REVIEW BOARD', 'CFB', 'CAMPAIGN FINANCE BOARD', 'COIB', 'CONFLICTS OF INTEREST BOARD', 'CPC', 'CITY PLANNING COMMISSION', 'CUNY', 'CITY UNIVERSITY OF NEW YORK', 'DANY', 'NEW YORK COUNTY DISTRICT ATTORNEY', 'DCA', 'DEPARTMENT OF CONSUMER AFFAIRS', 'DCAS', 'DEPARTMENT OF CITYWIDE ADMINISTRATIVE SERVICES', 'DCLA', 'DEPARTMENT OF CULTURAL AFFAIRS', 'DCP', 'DEPARTMENT OF CITY PLANNING', 'DDC', 'DEPARTMENT OF DESIGN AND CONSTRUCTION', 'DEP', 'DEPARTMENT OF ENVIRONMENTAL PROTECTION', 'DFTA', 'DEPARTMENT FOR THE AGING', 'DHS', 'DEPARTMENT OF HOMELESS SERVICES', 'DOB', 'DEPARTMENT OF BUILDINGS', 'DOC', 'DEPARTMENT OF CORRECTION', 'DOF', 'DEPARTMENT OF FINANCE', 'DOHMH', 'DEPARTMENT OF HEALTH AND MENTAL HYGIENE', 'DOI', 'DEPARTMENT OF INVESTIGATION', 'DOITT', 'DEPARTMENT OF INFORMATION TECHNOLOGY AND TELECOMMUNICATIONS', 'DOP', 'DEPARTMENT OF PROBATION', 'DORIS', 'DEPARTMENT OF RECORDS AND INFORMATION SERVICES', 'DOT', 'DEPARTMENT OF TRANSPORTATION', 'DPR', 'DEPARTMENT OF PARKS AND RECREATION', 'DSNY', 'DEPARTMENT OF SANITATION', 'DYCD', 'DEPARTMENT OF YOUTH AND COMMUNITY DEVELOPMENT', 'ECB', 'ENVIRONMENTAL CONTROL BOARD', 'EDC', 'ECONOMIC DEVELOPMENT CORPORATION', 'EEPC', 'EQUAL EMPLOYMENT PRACTICES COMMISSION', 'FCRC', 'FRANCHISE AND CONCESSION REVIEW COMMITTEE', 'FDNY', 'FIRE DEPARTMENT OF NEW YORK', 'FISA', 'FINANCIAL INFORMATION SERVICES AGENCY', 'HHC', 'HEAL AND HOSPITALS CORPORATION', 'HPD', 'DEPARTMENT OF HOUSING PRESERVATION AND DEVELOPMENT', 'HRA', 'HUMAN RESOURCES ADMINISTRATION', 'IBO', 'INDEPENDENT BUDGET OFFICE', 'KCDA', 'KINGS COUNTY DISTRICT ATTORNEY', 'LPC', 'LANDMARKS PRESERVATION COMMISSION', 'MOCJ', 'MAYOR’S OFFICE OF CRIMINAL JUSTICE', 'MOCS', "MAYOR'S OFFICE OF CONTRACT SERVICES", 'NYCDOE', 'DEPARTMENT OF EDUCATION', 'NYCEM', 'EMERGENCY MANAGEMENT', 'NYCERS', 'NEW YORK CITY EMPLOYEES RETIREMENT SYSTEM', 'NYCHA', 'NEW YORK CITY HOUSING AUTHORITY', 'NYCLD', 'LAW DEPARTMENT', 'NYCTAT', 'NEW YORK CITY TAX APPEALS TRIBUNAL', 'NYPD', 'NEW YORK CITY POLICE DEPARTMENT', 'NYPL', 'NEW YORK PUBLIC LIBRARY', 'OATH', 'OFFICE OF ADMINISTRATIVE TRIALS AND HEARINGS', 'OCME', 'OFFICE OF CHIEF MEDICAL EXAMINER', 'OMB', 'OFFICE OF MANAGEMENT & BUDGET', 'OSNP', 'OFFICE OF THE SPECIAL NARCOTICS PROSECUTOR', 'PPB', 'PROCUREMENT POLICY BOARD', 'QCDA', 'QUEENS DISTRICT ATTORNEY', 'QPL', 'QUEENS BOROUGH PUBLIC LIBRARY', 'RCDA', 'RICHMOND COUNTY DISTRICT ATTORNEY', 'RGB', 'RENT GUIDELINES BOARD', 'SBS', 'SMALL BUSINESS SERVICES', 'SCA', 'SCHOOL CONSTRUCTION AUTHORITY', 'TBTA', 'TRIBOROUGH BRIDGE AND TUNNEL AUTHORITY', 'TLC', 'TAXI AND LIMOUSINE COMMISSION', 'TRS', "TEACHERS' RETIREMENT SYSTEM"]
    def check(agency):
        if agency in agencies:
            return True
        abbr = re.search(r"\(([A-Za-z0-9_]+)\)", str(agency))
        if abbr:
            if abbr.group(1) in agencies:
                return True
        return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(str(x[0]).upper()) else (x[0], 0)).values().sum()
    return count


def count_areas_of_study(dataset):  # all >0.9
    # uq7m-95z8.interest1
    area_study = ['ANIMAL SCIENCE', 'ARCHITECTURE', 'BUSINESS', 'COMMUNICATIONS', 'COMPUTER SCIENCE & TECHNOLOGY', 'COMPUTER SCIENCE, MATH & TECHNOLOGY', 'COSMETOLOGY', 'CULINARY ARTS', 'ENGINEERING', 'ENVIRONMENTAL SCIENCE', 'FILM/VIDEO', 'HEALTH PROFESSIONS', 'HOSPITALITY, TRAVEL, & TOURISM', 'HUMANITIES & INTERDISCIPLINARY', 'JROTC', 'LAW & GOVERNMENT', 'PERFORMING ARTS', 'PERFORMING ARTS/VISUAL ART & DESIGN', 'PROJECT-BASED LEARNING', 'SCIENCE & MATH', 'TEACHING', 'VISUAL ART & DESIGN', 'ZONED']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if str(x[0]).upper() in area_study else (x[0], 0)).values().sum()
    return count


def count_subjects_in_school(dataset):  # all >0.9
    subjects = ['ENGLISH', 'MATH', 'SCIENCE', 'SOCIAL STUDIES']
    core_course = ['ALGEBRA', 'ASSUMED TEAM TEACHING', 'CHEMISTRY', 'EARTH SCIENCE', 'ECONOMICS', 'ENGLISH 10', 'ENGLISH 11', 'ENGLISH 12', 'ENGLISH 9', 'GEOMETRY', 'GLOBAL HISTORY 10', 'GLOBAL HISTORY 9', 'LIVING ENVIRONMENT', 'MATCHED SECTIONS', 'MATH A', 'MATH B', 'OTHER', 'PHYSICS', 'US GOVERNMENT', 'US GOVERNMENT & ECONOMICS', 'US HISTORY']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if (str(x[0]).upper() in subjects or str(x[0]).upper() in core_course) else (x[0], 0)).values().sum()
    return count


def count_school_levels(dataset):
    school_levels = ['ELEMENTARY SCHOOL', 'TRANSFER SCHOOL', 'HIGH SCHOOL TRANSFER', 'TRANSFER HIGH SCHOOL', 'MIDDLE SCHOOL',
                     'K-2 SCHOOL', 'K-3 SCHOOL', 'K-8 SCHOOL', 'YABC', 'D75']
    def check(level):
        result = [i for i in school_levels if level in i]
        if len(result) > 0:
            return True
        else:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(str(x[0]).upper()) else (x[0], 0)).values().sum()
    return count


def count_university_names(dataset):
    university_names = ['university', 'college']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if str(x[0]).split(" ")[-1].lower() in university_names else (x[0], 0)).values().sum()
    return count

def count_websites(dataset):
    web_regex = re.compile('(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if web_regex.search(str(x[0])) else (x[0], 0)).values().sum()
    return count


def count_building_classification(dataset):
    # https://www1.nyc.gov/assets/finance/jump/hlpbldgcode.html
    buildings = ['A0', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'B1', 'B2', 'B3', 'B9', 'C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'D0', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'E1', 'E3', 'E4', 'E5', 'E7', 'E9', 'F1', 'F2', 'F4', 'F5', 'F8', 'F9', 'G0', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8', 'H9', 'HB', 'HH', 'HR', 'HS', 'I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I9', 'J1', 'J2', 'J3', 'J4', 'J5', 'J6', 'J7', 'J8', 'J9', 'K1', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8', 'K9', 'L1', 'L2', 'L3', 'L8', 'L9', 'M1', 'M2', 'M3', 'M4', 'M9', 'N1', 'N2', 'N3', 'N4', 'N9', 'O1', 'O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'Q0', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'RA', 'RB', 'RC', 'RD', 'RG', 'RH', 'RI', 'RK', 'RM', 'RR', 'RS', 'RW', 'RX', 'RZ', 'S0', 'S1', 'S2', 'S3', 'S4', 'S5', 'S9', 'T1', 'T2', 'T9', 'U0', 'U1', 'U2', 'U3', 'U4', 'U5', 'U6', 'U7', 'U8', 'U9', 'V0', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7', 'Y8', 'Y9', 'Z0', 'Z1', 'Z2', 'Z3', 'Z4', 'Z5', 'Z6', 'Z7', 'Z8', 'Z9']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if str(x[0]).split('-')[0] in buildings else (x[0], 0)).values().sum()
    return count


def count_vehicle_type(dataset):
    # https://data.ny.gov/Transportation/Vehicle-Makes-and-Body-Types-Most-Popular-in-New-Y/3pxy-wy2i
    types = ['2dsd', '4dsd', 'ambu', 'atv', 'boat', 'bus', 'cmix', 'conv', 'cust', 'dcom', 'delv', 'dump', 'emvr', 'fire', 'flat', 'fpm', 'h/in', 'h/tr', 'h/wh', 'hrse', 'lim', 'loco', 'lsv', 'lsvt', 'ltrl', 'mcc', 'mcy', 'mfh', 'mopd', 'p/sh', 'pick', 'pole', 'r/rd', 'rbm', 'rd/s', 'refg', 'rplc', 's/sp', 'sedn', 'semi', 'sn/p', 'snow', 'stak', 'subn', 'swt', 't/cr', 'tank', 'taxi', 'tow', 'tr/c', 'tr/e', 'trac', 'trav', 'trlr', 'util', 'van', 'w/dr', 'w/sr']
    # Top 50 most type codes
    type_codes = ['PASSENGER VEHICLE', 'SPORT UTILITY / STATION WAGON', 'SEDAN', 'STATION WAGON/SPORT UTILITY VEHICLE', 'TAXI', 'PICK-UP TRUCK', 'VAN', 'OTHER', 'BUS', 'UNKNOWN', 'SMALL COM VEH(4 TIRES)', 'LARGE COM VEH(6 OR MORE TIRES)', 'LIVERY VEHICLE', 'MOTORCYCLE', 'BOX TRUCK', 'BICYCLE', 'BIKE', 'TRACTOR TRUCK DIESEL', 'AMBULANCE', 'TK', 'BU', 'CONVERTIBLE', 'DUMP', 'DS', 'FIRE TRUCK', '4 DR SEDAN', 'PK', 'VN', 'GARBAGE OR REFUSE', 'FLAT BED', 'CARRY ALL', 'TRACTOR TRUCK GASOLINE', 'CONV', 'TOW TRUCK / WRECKER', 'DP', 'AMBUL', 'AM', 'SCOOTER', 'FB', 'TANKER', 'CHASSIS CAB', 'MOPED', 'GG', 'MOTORSCOOTER', 'LL', 'TR', 'CONCRETE MIXER', 'REFRIGERATED VAN', 'TRAIL', 'TT']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if (str(x[0]).lower() in types or str(x[0]).upper() in type_codes) else (x[0], 0)).values().sum()
    return count


def count_type_of_location(dataset):
    type_location = ['ABANDONED BUILDING', 'AIRPORT TERMINAL', 'ATM', 'BANK', 'BAR/NIGHT CLUB', 'BEAUTY & NAIL SALON', 'BOOK/CARD', 'BRIDGE', 'BUS (NYC TRANSIT)', 'BUS (OTHER)', \
                    'BUS STOP', 'BUS TERMINAL', 'CANDY STORE', 'CEMETERY', 'CHAIN STORE', 'CHECK CASHING BUSINESS', 'CHURCH', 'CLOTHING/BOUTIQUE', \
                    'COMMERCIAL BUILDING', 'CONSTRUCTION SITE', 'DAYCARE FACILITY', 'DEPARTMENT STORE', 'DOCTOR/DENTIST OFFICE', 'DRUG STORE', 'DRY CLEANER/LAUNDRY', 'FACTORY/WAREHOUSE', \
                    'FAST FOOD', 'FERRY/FERRY TERMINAL', 'FOOD SUPERMARKET', 'GAS STATION', 'GROCERY/BODEGA', 'GYM/FITNESS FACILITY', \
                    'HIGHWAY/PARKWAY', 'HOMELESS SHELTER', 'HOSPITAL', 'HOTEL/MOTEL', 'JEWELRY', 'LIQUOR STORE', 'LOAN COMPANY', \
                    'MAILBOX INSIDE', 'MAILBOX OUTSIDE', 'MARINA/PIER', 'MOSQUE', 'OPEN AREAS (OPEN LOTS)', 'OTHER', 'OTHER HOUSE OF WORSHIP', 'PARK/PLAYGROUND', 'PARKING LOT/GARAGE (PRIVATE)', 'PARKING LOT/GARAGE (PUBLIC)', \
                    'PHOTO/COPY', 'PRIVATE/PAROCHIAL SCHOOL', 'PUBLIC BUILDING', 'PUBLIC SCHOOL', 'RESIDENCE - APT. HOUSE', \
                    'RESIDENCE - PUBLIC HOUSING', 'RESIDENCE-HOUSE', 'RESTAURANT/DINER', 'SHOE', 'SMALL MERCHANT', 'SOCIAL CLUB/POLICY', 'STORAGE FACILITY', 'STORE UNCLASSIFIED', 'STREET', 'SYNAGOGUE', 
                    'TAXI (LIVERY LICENSED)', 'TAXI (YELLOW LICENSED)', 'TAXI/LIVERY (UNLICENSED)', 'TELECOMM. STORE', 'TRAMWAY', 'TRANSIT - NYC SUBWAY', 'TRANSIT FACILITY (OTHER)', 'TUNNEL', 'VARIETY STORE', 'VIDEO STORE']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if str(x[0]).upper() in type_location else (x[0], 0)).values().sum()
    return count


def count_parks_or_playground(dataset):    # 0.87 , 0.67, 0.46, 0.32       0.01 for landmark      0.39 x2 for School_Name contains parks
    words = ['park', 'playground', 'green', 'plots', 'square', 'plaza']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if str(x[0]).split(" ")[-1].lower() in words else (x[0], 0)).values().sum()
    return count


######################## Main ########################

semantic_types = [
    ['phone_number', count_phone_number],
    ['lat_lon_cord', count_coordinates],
    ['zip_code', count_zip],
    ['website', count_websites],
    ['borough', count_borough],
    ['building_classification', count_building_classification],
    ['school_level', count_school_levels],
    ['location_type', count_type_of_location],
    ['neighborhood', count_neighborhood],
    ['area_of_study', count_areas_of_study],
    ['subject_in_school', count_subjects_in_school],
    ['address', count_address],
    ['street_name', count_street_name],
    ['city', count_city],
    ['color', count_color],
    ['car_make', count_car_make],
    ['school_name', count_school_name],
    ['city_agency', count_city_agency],
    ['college_name', count_university_names],
    ['vehicle_type', count_vehicle_type],
    ['park_playground', count_parks_or_playground],
    ['person_name', count_person_name],
    ['business_name', count_business_name],
]

def profile_semantic(dataset):
    res = dataset.select(F.sum('count')).collect()[0][0]
    invalid_words = ['UNSPECIFIED', 'UNKNOWN', 'UNKNOW', '-', 'NA', 'N/A', '__________']
    dataset = dataset.filter(~dataset.value.isin(invalid_words))
    total = dataset.select(F.sum('count')).collect()[0][0]
    confirm_threshold = 0.8 * total
    minimum_threshold = 0.2 * total
    ret = []

    for semantic_type, is_semantic_type in semantic_types:
        count, label = is_semantic_type(dataset), None
        if count > minimum_threshold:
            ret.append({'semantic_type': semantic_type, 'count': count})
            res -= count
        if count > confirm_threshold:
            break
    if res > 0:
        ret.append({'semantic_type': 'other', 'count': res})
    return ret


# 1. list the working subset
cluster = json.loads(spark.read.text('/user/ql1045/proj-in/cluster2.txt').collect()[0][0].replace("'", '"'))

# 2. for each working dataset
for filename in cluster:
    # filename = cluster[0]
    [dataset_name, column_name] = filename.split('.')[0:2]
    print(u'>> entering {}.{}'.format(dataset_name, column_name))
    try:
        with open('./task2/{}.{}.json'.format(dataset_name, column_name)) as f:
            json.load(f)
            print(u'>> skipping {}.{}'.format(dataset_name, column_name))
            continue
    except:
        pass

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))

    # 2.2 load the corresponding dataset profile
    try:
        with open('task1.{}.json'.format(dataset_name), 'r') as f:
            dataset_profile = json.load(f)

        # 2.3 load the corresponing column profile
        for entry in dataset_profile['columns']:
            if entry['column_name'] == column_name:
                column_profile = entry
                break
        else:
            raise ValueError
    except:
        pass

    # 2.4 create column semantic profile
    output = {
        'column_name': column_name,
        'semantic_types': profile_semantic(dataset)
    }

    # 2.5 dump updated dataset profile as json
    try:
        os.mkdir('./task2/')
    except Exception as e:
        pass

    with open('./task2/{}.{}.json'.format(dataset_name, column_name), 'w') as f:
        json.dump(output, f, indent=2)
