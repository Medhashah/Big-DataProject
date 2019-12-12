# -*- coding: utf-8 -*-
import sys

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import requests
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import json
import pandas as pd
import random
import numpy as np
from pandas.io.json import json_normalize
import re
import FileInputManager as fm

neiborhood_names = 0
boroughs = 0
business_data = 0
schoolLevels = 0
streets = 0
parks = 0
buildingTypes = 0
first_name = 0
last_name = 0
colleges = 0
cars = 0
fields = 0


def initialize():
    global threshold
    threshold = 0.8

    global labels  # DO NOT CHANGE THE ORDER OF LABELS
    labels = np.asarray(
        ["Business Name", 'School Levels', 'Street Name', 'Park/Playground', 'City agency', 'Building Classification', \
         'Neighborhood', 'Borough', 'Car Make', 'Areas of study', 'Websites', 'Color', 'Type of location',
         'Subjects in school', 'College/University names', 'Phone number', 'Address', 'City', 'LAT/LON coordinates' \
         'Zip code', 'School Name', "Person Name", 'Vehicle Type', ])

    global label_proportion
    label_proportion = {}

    global neiborhood_names
    global boroughs
    response = requests.get("https://data.cityofnewyork.us/resource/xyye-rtrs.json")
    neiborhood_data = response.json()
    temp_borough_set = set()
    neiborhood_names = []
    for item in neiborhood_data:
        neiborhood_names.append(item["name"])
        temp_borough_set.add(item["borough"])
    boroughs = list(temp_borough_set)

    global business_data
    response = requests.get("https://data.ny.gov/resource/n9v6-gdp6.json")
    business_data = response.json()

    global schoolLevels
    schoolLevels = np.asarray(
        ['K-12', 'K-11', 'K-10', 'K-9', 'K-8', 'K-7', 'K-6', 'K-5', 'K-4', 'K-3', 'K-2', 'K-1', 'Elementry',
         'Middle school', 'high school', 'college', 'grade'])

    global streets
    streets = np.asarray(['avenue', 'street', 'st', 'east', 'west', 'north', 'south', 'ave'])

    global parks
    parks = np.asarray(['park', 'playground', 'field'])

    global first_name
    first_name = pd.read_csv("/user/yy3090/csv/fname.csv")

    global last_name
    last_name = pd.read_csv("/user/yy3090/csv/lname.csv")

    global colleges
    f1 = ['NAME']
    colleges = pd.read_csv("/user/yy3090/csv/college.csv", usecols=f1)

    global cars
    f2 = ['Unnamed: 0']
    cars = pd.read_csv("/user/yy3090/csv/cars.csv", usecols=f2)

    global fields
    f3 = ['Arts']
    fields = pd.read_csv("/user/yy3090/csv/study.csv", usecols=f3)

    global buildingTypes
    buildingTypes = np.asarray(['A0	CAPE COD', 'A1	TWO STORIES - DETACHED SM OR MID',
                                'A2	ONE STORY - PERMANENT LIVING QUARTER', 'A3	LARGE SUBURBAN RESIDENCE',
                                'A4	CITY RESIDENCE ONE FAMILY', 'A5	ONE FAMILY ATTACHED OR SEMI-DETACHED',
                                'A6	SUMMER COTTAGE', 'A7	MANSION TYPE OR TOWN HOUSE',
                                'A8	BUNGALOW COLONY - COOPERATIVELY OWNED LAND', 'A9	MISCELLANEOUS ONE FAMILY',
                                'B1	TWO FAMILY BRICK', 'B2	TWO FAMILY FRAME',
                                'B3	TWO FAMILY CONVERTED FROM ONE FAMILY',
                                'B9	MISCELLANEOUS TWO FAMILY', 'C0	THREE FAMILIES',
                                'C1	OVER SIX FAMILIES WITHOUT STORES',
                                'C2	FIVE TO SIX FAMILIES', 'C3	FOUR FAMILIES', 'C4	OLD LAW TENEMENT',
                                'C5	CONVERTED DWELLINGS OR ROOMING HOUSE', 'C6	WALK-UP COOPERATIVE',
                                'C7	WALK-UP APT. OVER SIX FAMILIES WITH STORES',
                                'C8	WALK-UP CO-OP; CONVERSION FROM LOFT/WAREHOUSE', 'C9	GARDEN APARTMENTS',
                                'CM	MOBILE HOMES/TRAILER PARKS',
                                'D0	ELEVATOR CO-OP; CONVERSION FROM LOFT/WAREHOUSE',
                                'D1	ELEVATOR APT; SEMI-FIREPROOF WITHOUT STORES',
                                'D2	ELEVATOR APT; ARTISTS IN RESIDENCE',
                                'D3	ELEVATOR APT; FIREPROOF WITHOUT STORES', 'D4	ELEVATOR COOPERATIVE',
                                'D5	ELEVATOR APT; CONVERTED', 'D6	ELEVATOR APT; FIREPROOF WITH STORES',
                                'D7	ELEVATOR APT; SEMI-FIREPROOF WITH STORES', 'D8	ELEVATOR APT; LUXURY TYPE',
                                'D9	ELEVATOR APT; MISCELLANEOUS', 'E1	FIREPROOF WAREHOUSE',
                                'E2	CONTRACTORS WAREHOUSE',
                                'E3	SEMI-FIREPROOF WAREHOUSE', 'E4	METAL FRAME WAREHOUSE',
                                'E7	SELF-STORAGE WAREHOUSES',
                                'E9	MISCELLANEOUS WAREHOUSE', 'F1	FACTORY; HEAVY MANUFACTURING - FIREPROOF',
                                'F2	FACTORY; SPECIAL CONSTRUCTION - FIREPROOF',
                                'F4	FACTORY; INDUSTRIAL SEMI-FIREPROOF',
                                'F5	FACTORY; LIGHT MANUFACTURING', 'F8	FACTORY; TANK FARM',
                                'F9	FACTORY; INDUSTRIAL-MISCELLANEOUS', 'G0	GARAGE; RESIDENTIAL TAX CLASS 1',
                                'G1	ALL PARKING GARAGES', 'G2	AUTO BODY/COLLISION OR AUTO REPAIR',
                                'G3	GAS STATION WITH RETAIL STORE', 'G4	GAS STATION WITH SERVICE/AUTO REPAIR',
                                'G5	GAS STATION ONLY WITH/WITHOUT SMALL KIOSK', 'G6	LICENSED PARKING LOT',
                                'G7	UNLICENSED PARKING LOT', 'G8	CAR SALES/RENTAL WITH SHOWROOM',
                                'G9	MISCELLANEOUS GARAGE OR GAS STATION',
                                'GU	CAR SALES OR RENTAL LOTS WITHOUT SHOWROOM',
                                'GW	CAR WASH OR LUBRITORIUM FACILITY', 'G9	MISCELLANEOUS GARAGE',
                                'HB	BOUTIQUE: 10-100 ROOMS, W/LUXURY FACILITIES, THEMED, STYLISH, W/FULL SVC ACCOMMODATIONS',
                                'HH	HOSTELS- BED RENTALS IN DORMITORY-LIKE SETTINGS W/SHARED ROOMS & BATHROOMS',
                                'HR	SRO- 1 OR 2 PEOPLE HOUSED IN INDIVIDUAL ROOMS IN MULTIPLE DWELLING AFFORDABLE HOUSING',
                                'HS	EXTENDED STAY/SUITE: AMENITIES SIMILAR TO APT; TYPICALLY CHARGE WEEKLY RATES & LESS EXPENSIVE THAN FULL-SERVICE HOTEL',
                                'H1	LUXURY HOTEL', 'H2	FULL SERVICE HOTEL',
                                'H3	LIMITED SERVICE; MANY AFFILIATED WITH NATIONAL CHAIN', 'H4	MOTEL',
                                'H5	HOTEL; PRIVATE CLUB, LUXURY TYPE', 'H6	APARTMENT HOTEL',
                                'H7	APARTMENT HOTEL - COOPERATIVELY OWNED', 'H8	DORMITORY',
                                'H9	MISCELLANEOUS HOTEL',
                                'I1	HOSPITAL, SANITARIUM, MENTAL INSTITUTION', 'I2	INFIRMARY', 'I3	DISPENSARY',
                                'I4	HOSPITAL; STAFF FACILITY', 'I5	HEALTH CENTER, CHILD CENTER, CLINIC',
                                'I6	NURSING HOME',
                                'I7	ADULT CARE FACILITY', 'I9	MISCELLANEOUS HOSPITAL, HEALTH CARE FACILITY',
                                'J1	THEATRE; ART TYPE LESS THAN 400 SEATS',
                                'J2	THEATRE; ART TYPE MORE THAN 400 SEATS',
                                'J3	MOTION PICTURE THEATRE WITH BALCONY', 'J4	LEGITIMATE THEATRE, SOLE USE',
                                'J5	THEATRE IN MIXED-USE BUILDING', 'J6	TELEVISION STUDIO',
                                'J7	OFF BROADWAY TYPE THEATRE',
                                'J8	MULTIPLEX PICTURE THEATRE', 'J9	MISCELLANEOUS THEATRE',
                                'K1	ONE STORY RETAIL BUILDING',
                                'K2	MULTI-STORY RETAIL BUILDING (2 OR MORE)', 'K3	MULTI-STORY DEPARTMENT STORE',
                                'K4	PREDOMINANT RETAIL WITH OTHER USES', 'K5	STAND-ALONE FOOD ESTABLISHMENT',
                                'K6	SHOPPING CENTER WITH OR WITHOUT PARKING',
                                'K7	BANKING FACILITIES WITH OR WITHOUT PARKING',
                                'K8	BIG BOX RETAIL: NOT AFFIXED & STANDING ON OWN LOT W/PARKING, E.G. COSTCO & BJ',
                                'K9	MISCELLANEOUS STORE BUILDING', 'L1	LOFT; OVER 8 STORIES (MID MANH. TYPE)',
                                'L2	LOFT; FIREPROOF AND STORAGE TYPE WITHOUT STORES', 'L3	LOFT; SEMI-FIREPROOF',
                                'L8	LOFT; WITH RETAIL STORES OTHER THAN TYPE ONE', 'L9	MISCELLANEOUS LOFT',
                                'M1	CHURCH, SYNAGOGUE, CHAPEL', 'M2	MISSION HOUSE (NON-RESIDENTIAL)',
                                'M3	PARSONAGE, RECTORY', 'M4	CONVENT', 'M9	MISCELLANEOUS RELIGIOUS FACILITY',
                                'N1	ASYLUM', 'N2	HOME FOR INDIGENT CHILDREN, AGED, HOMELESS', 'N3	ORPHANAGE',
                                'N4	DETENTION HOUSE FOR WAYWARD GIRLS', 'N9	MISCELLANEOUS ASYLUM, HOME',
                                'O1	OFFICE ONLY - 1 STORY', 'O2	OFFICE ONLY 2 - 6 STORIES',
                                'O3	OFFICE ONLY 7 - 19 STORIES',
                                'O4	OFFICE ONLY WITH OR WITHOUT COMM - 20 STORIES OR MORE',
                                'O5	OFFICE WITH COMM - 1 TO 6 STORIES', 'O6	OFFICE WITH COMM 7 - 19 STORIES',
                                'O7	PROFESSIONAL BUILDINGS/STAND ALONE FUNERAL HOMES',
                                'O8	OFFICE WITH APARTMENTS ONLY (NO COMM)',
                                'O9	MISCELLANEOUS AND OLD STYLE BANK BLDGS.',
                                'P1	CONCERT HALL', 'P2	LODGE ROOM', 'P3	YWCA, YMCA, YWHA, YMHA, PAL',
                                'P4	BEACH CLUB',
                                'P5	COMMUNITY CENTER', 'P6	AMUSEMENT PLACE, BATH HOUSE, BOAT HOUSE',
                                'P7	MUSEUM',
                                'P8	LIBRARY', 'P9	MISCELLANEOUS INDOOR PUBLIC ASSEMBLY',
                                'Q1	PARKS/RECREATION FACILTY',
                                'Q2	PLAYGROUND', 'Q3	OUTDOOR POOL', 'Q4	BEACH', 'Q5	GOLF COURSE',
                                'Q6	STADIUM, RACE TRACK, BASEBALL FIELD', 'Q7	TENNIS COURT',
                                'Q8	MARINA, YACHT CLUB',
                                'Q9	MISCELLANEOUS OUTDOOR RECREATIONAL FACILITY',
                                'RA	CULTURAL, MEDICAL, EDUCATIONAL, ETC.',
                                'RB	OFFICE SPACE', 'RG	INDOOR PARKING', 'RH	HOTEL/BOATEL',
                                'RK	RETAIL SPACE',
                                'RP	OUTDOOR PARKING', 'RR	CONDOMINIUM RENTALS',
                                'RS	NON-BUSINESS STORAGE SPACE',
                                'RT	TERRACES/GARDENS/CABANAS', 'RW	WAREHOUSE/FACTORY/INDUSTRIAL',
                                'R0	SPECIAL CONDOMINIUM BILLING LOT',
                                'R1	CONDO; RESIDENTIAL UNIT IN 2-10 UNIT BLDG.',
                                'R2	CONDO; RESIDENTIAL UNIT IN WALK-UP BLDG.',
                                'R3	CONDO; RESIDENTIAL UNIT IN 1-3 STORY BLDG.',
                                'R4	CONDO; RESIDENTIAL UNIT IN ELEVATOR BLDG.', 'R5	MISCELLANEOUS COMMERCIAL',
                                'R6	CONDO; RESID.UNIT OF 1-3 UNIT BLDG-ORIG CLASS 1',
                                'R7	CONDO; COMML.UNIT OF 1-3 UNIT BLDG-ORIG CLASS 1',
                                'R8	CONDO; COMML.UNIT OF 2-10 UNIT BLDG.', 'R9	CO-OP WITHIN A CONDOMINIUM',
                                'RR	CONDO RENTALS', 'S0	PRIMARILY 1 FAMILY WITH 2 STORES OR OFFICES',
                                'S1	PRIMARILY 1 FAMILY WITH 1 STORE OR OFFICE',
                                'S2	PRIMARILY 2 FAMILY WITH 1 STORE OR OFFICE',
                                'S3	PRIMARILY 3 FAMILY WITH 1 STORE OR OFFICE',
                                'S4	PRIMARILY 4 FAMILY WITH 1 STORE OROFFICE',
                                'S5	PRIMARILY 5-6 FAMILY WITH 1 STORE OR OFFICE',
                                'S9	SINGLE OR MULTIPLE DWELLING WITH STORES OR OFFICES',
                                'T1	AIRPORT, AIRFIELD, TERMINAL',
                                'T2	PIER, DOCK, BULKHEAD', 'T9	MISCELLANEOUS TRANSPORTATION FACILITY',
                                'U0	UTILITY COMPANY LAND AND BUILDING', 'U1	BRIDGE, TUNNEL, HIGHWAY',
                                'U2	GAS OR ELECTRIC UTILITY', 'U3	CEILING RAILROAD', 'U4	TELEPHONE UTILITY',
                                'U5	COMMUNICATION FACILITY OTHER THAN TELEPHONE',
                                'U6	RAILROAD - PRIVATE OWNERSHIP',
                                'U7	TRANSPORTATION - PUBLIC OWNERSHIP', 'U8	REVOCABLE CONSENT',
                                'U9	MISCELLANEOUS UTILITY PROPERTY', 'V0	ZONED RESIDENTIAL; NOT MANHATTAN',
                                'V1	ZONED COMMERCIAL OR MANHATTAN RESIDENTIAL',
                                'V2	ZONED COMMERCIAL ADJACENT TO CLASS 1 DWELLING: NOT MANHATTAN',
                                'V3	ZONED PRIMARILY RESIDENTIAL; NOT MANHATTAN', 'V4	POLICE OR FIRE DEPARTMENT',
                                'V5	SCHOOL SITE OR YARD', 'V6	LIBRARY, HOSPITAL OR MUSEUM',
                                'V7	PORT AUTHORITY OF NEW YORK AND NEW JERSEY',
                                'V8	NEW YORK STATE OR US GOVERNMENT',
                                'V9	MISCELLANEOUS VACANT LAND', 'W1	PUBLIC ELEMENTARY, JUNIOR OR SENIOR HIGH',
                                'W2	PAROCHIAL SCHOOL, YESHIVA', 'W3	SCHOOL OR ACADEMY', 'W4	TRAINING SCHOOL',
                                'W5	CITY UNIVERSITY', 'W6	OTHER COLLEGE AND UNIVERSITY',
                                'W7	THEOLOGICAL SEMINARY',
                                'W8	OTHER PRIVATE SCHOOL', 'W9	MISCELLANEOUS EDUCATIONAL FACILITY',
                                'Y1	FIRE DEPARTMENT',
                                'Y2	POLICE DEPARTMENT', 'Y3	PRISON, JAIL, HOUSE OF DETENTION',
                                'Y4	MILITARY AND NAVAL INSTALLATION', 'Y5	DEPARTMENT OF REAL ESTATE',
                                'Y6	DEPARTMENT OF SANITATION', 'Y7	DEPARTMENT OF PORTS AND TERMINALS',
                                'Y8	DEPARTMENT OF PUBLIC WORKS', 'Y9	DEPARTMENT OF ENVIRONMENTAL PROTECTION',
                                'Z0	TENNIS COURT, POOL, SHED, ETC.', 'Z1	COURT HOUSE',
                                'Z2	PUBLIC PARKING AREA',
                                'Z3	POST OFFICE', 'Z4	FOREIGN GOVERNMENT', 'Z5	UNITED NATIONS',
                                'Z7	EASEMENT',
                                'Z8	CEMETERY', 'Z9	OTHER MISCELLANEOUS'])

    global agencies
    agencies = np.asarray(
        ["Actuary, NYC Office of the (NYCOA)", "Administrative Justice Coordinator, NYC Office of (AJC)",
         "Administrative Tax Appeals, Office of", "Administrative Trials and Hearings, Office of (OATH)",
         "Aging, Department for the (DFTA)", "Appointments, Mayor's Office of (MOA)", "Brooklyn Public Library (BPL)",
         "Buildings, Department of (DOB)", "Business Integrity Commission (BIC)", "Campaign Finance Board (CFB)",
         "Center for Innovation through Data Intelligence (CIDI)", "Charter Revision Commission",
         "Chief Medical Examiner, NYC Office of (OCME)", "Children's Services, Administration for (ACS)",
         "City Clerk, Office of the (CLERK)", "City Council, New York", "City Planning, Department of (DCP)",
         "City University of New York (CUNY)", "Citywide Administrative Services, Department of (DCAS)",
         "Citywide Event Coordination and Management, Office of (CECM)", "Civic Engagement Commission (CEC)",
         "Civil Service Commission (CSC)", "Civilian Complaint Review Board (CCRB)", "Climate Policy & Programs",
         "Commission on Gender Equity (CGE)", "Commission to Combat Police Corruption (CCPC)",
         "Community Affairs Unit (CAU)", "Community Boards (CB)", "Comptroller (COMP)",
         "Conflicts of Interest Board (COIB)", "Consumer Affairs, Department of (DCA)",
         "Consumer and Worker Protection, Department of (DCWP)", "Contract Services, Mayor's Office of (MOCS)",
         "Correction, Board of (BOC)", "Correction, Department of (DOC)", "Criminal Justice, Mayor's Office of",
         "Cultural Affairs, Department of (DCLA)", "Data Analytics, Mayor's Office of (MODA)",
         "Design and Construction, Department of (DDC)", "District Attorney - Bronx County",
         "District Attorney - Kings County (Brooklyn)", "District Attorney - New York County (Manhattan)",
         "District Attorney - Queens County", "District Attorney - Richmond County (Staten Island)",
         "Education, Department of (DOE)", "Elections, Board of (BOE)", "Emergency Management, NYC",
         "Environmental Coordination, Mayor’s Office of (MOEC)", "Environmental Protection, Department of (DEP)",
         "Equal Employment Practices Commission (EEPC)", "Finance, Department of (DOF)",
         "Fire Department, New York City (FDNY)", "Fiscal Year 2005 Securitization Corporation",
         "Food Policy Director, Office of the", "GreeNYC (GNYC)", "Health and Mental Hygiene, Department of (DOHMH)",
         "Homeless Services, Department of (DHS)", "Housing Authority, New York City (NYCHA)",
         "Housing Preservation and Development, Department of (HPD)", "Housing Recovery Operations (HRO)",
         "Hudson Yards Infrastructure Corporation", "Human Resources Administration (HRA)",
         "Human Rights, City Commission on (CCHR)", "Immigrant Affairs, Mayor's Office of (MOIA)",
         "Independent Budget Office, NYC (IBO)", "Information Privacy, Mayor's Office of (MOIP)",
         "Information Technology and Telecommunications, Department of (DOITT)",
         "Inspector General NYPD, Office of the", "Intergovernmental Affairs, Mayor's Office of (MOIGA)",
         "Investigation, Department of (DOI)", "Judiciary, Mayor's Advisory Committee on the (MACJ)",
         "Labor Relations, NYC Office of (OLR)", "Landmarks Preservation Commission (LPC)", "Law Department (LAW)",
         "Library, Brooklyn Public (BPL)", "Library, New York Public (NYPL)", "Library, Queens Public (QL)",
         "Loft Board (LOFT)", "Management and Budget, Office of (OMB)", "Mayor's Committee on City Marshals (MCCM)",
         "Mayor's Fund to Advance NYC (Mayor's Fund)", "Mayor's Office (OM)", "Mayor's Office for Economic Opportunity",
         "Mayor's Office for International Affairs (IA)", "Mayor's Office for People with Disabilities (MOPD)",
         "Mayor's Office of Environmental Remediation (OER)",
         "Mayor's Office of Special Projects & Community Events (MOSPCE)",
         "Mayor's Office of the Chief Technology Officer",
         "Media and Entertainment, Mayor's Office of (MOME)", "Media, NYC", "NYC & Company (NYCGO)",
         "NYC Children's Cabinet", "NYC Cyber Command", "NYC Economic Development Corporation (NYCEDC)",
         "NYC Employees' Retirement System (NYCERS)", "NYC Health + Hospitals", "NYC Service (SERVICE)",
         "New York City Transitional Finance Authority (TFA)",
         "New York Public Library (NYPL)", "Office of Recovery & Resiliency", "Office of ThriveNYC",
         "Office of the Census for NYC", "Operations, Mayor's Office of (OPS)",
         "Parks and Recreation, Department of (DPR)", "Payroll Administration, Office of (OPA)",
         "Police Department (NYPD)", "Police Pension Fund (PPF)", "Probation, Department of (DOP)",
         "Procurement Policy Board (PPB)", "Property Tax Reform, Advisory Commission on",
         "Public Administrator - Bronx County (BCPA)", "Public Administrator - Kings County (KCPA)",
         "Public Administrator - New York County (NYCountyPA)", "Public Administrator - Queens County (QPA)",
         "Public Administrator - Richmond County (RCPA)", "Public Advocate (PUB ADV)", "Public Design Commission",
         "Queens Public Library (QPL)", "Records and Information Services, Department of (DORIS)",
         "Rent Guidelines Board (RGB)", "Sales Tax Asset Receivable Corporation (STAR)",
         "Sanitation, Department of (DSNY)", "School Construction Authority (SCA)", "Small Business Services (SBS)",
         "Social Services, Department of (DSS)",
         "Special Commissioner of Investigation for the New York City School District",
         "Special Narcotics Prosecutor, NYC Office of the (SNP)",
         "Standards and Appeals, Board of (BSA)", "Sustainability, Mayor's Office Of", "TSASC, Inc.",
         "Tax Appeals Tribunal, New York City (TAT)", "Tax Commission, New York City (TC)",
         "Taxi and Limousine Commission (TLC)", "Teachers' Retirement System of the City of New York",
         "Transportation, Department of (DOT)", "Veterans' Services, Department of (DVS)", "Water Board (NYWB)",
         "Water Finance Authority, NYC Municipal (NYW)", "Workforce Development, Mayor's Office of",
         "Youth and Community Development, Department of (DYCD)"])

    global subjects
    subjects = np.asarray(
        ['Academic Studies in Education', 'Accounting', 'Analytical Science', 'Animal Science', 'Applied Mathematics',
         'Applied Statistics', 'Aquatic Sciences', 'Art History', 'Arts & Humanities', 'Astronomy & Planetary Science',
         'Biochemistry', 'Biology', 'Building & Planning', 'Business & Commercial Law', 'Business & Management',
         'Business Studies', 'Cardiography', 'Chemistry', 'Childhood & Youth Studies',
         'Children, Young People & Families', 'Chinese', 'Cinematics & Photography', 'Civil Engineering',
         'Classical Studies', 'Climatology', 'Communication Technologies', 'Computer Science', 'Computing & IT',
         'Contract Law', 'Counselling', 'Creative Writing', 'Criminal Law', 'Criminology', 'Databases', 'Design',
         'Development', 'Early Years', 'Earth Sciences', 'Ecology', 'Economics', 'Education, Childhood & Youth',
         'Electronic Engineering', 'Engineering', 'Engineering Design', 'English Language', 'English Literature',
         'Environment & Development', 'Environmental Management', 'Environmental Sciences', 'Environmental Studies',
         'Finance', 'Forensic Science', 'French', 'General Computing', 'General Design', 'General Education',
         'General Engineering', 'General Law', 'General Nursing', 'General Social Science', 'General Technology',
         'Genetics', 'Geography', 'Geology', 'German', 'Health & Social Care', 'Health & Wellbeing', 'Health Sciences',
         'Healthcare Management', 'Healthcare Practice', 'Heritage', 'History', 'Human Anatomy',
         'Human Computer Interaction', 'Humanities', 'Industrial Engineering', 'Information Systems',
         'Innovation, Enterprise & Creativity', 'International Business Studies', 'International Law', 'Italian',
         'Jurisprudence', 'Languages', 'Law', 'Leadership & Management', 'Linguistics', 'Management',
         'Management of Education', 'Managing Technology', 'Marketing', 'Materials Science', 'Mathematical Methods',
         'Mathematical Modelling', 'Mathematics', 'Mathematics & Statistics', 'Medical Sciences',
         'Mental Health Nursing', 'Microbiology', 'Music', 'Natural Sciences', 'Networking', 'Neuroscience',
         'Nursing & Healthcare Practice', 'Organisational Administration', 'Other European Languages', 'Pathology',
         'Philosophy', 'Physics', 'Politics', 'Primary Education', 'Probability', 'Product design',
         'Production Manufacturing', 'Professional Science', 'Programming', 'Project Management', 'Psychology',
         'Psychology & Counselling', 'Public Law', 'Pure Mathematics', 'Religious Studies',
         'Research & Study Skills in Education', 'Retail Management', 'Science', 'Science & Society',
         'Secondary Education', 'Social Care', 'Social Sciences', 'Social Work', 'Sociology', 'Software Engineering',
         'Spanish', 'Sport & Fitness', 'Statistics', 'Strategic Management', 'Teacher Training', 'Technology', 'Torts',
         'Translation studies'])

    global carBrand
    carBrand = np.asarray(
        ['Mercedes-Benz', 'Audi', 'Volkswagen', 'BMW', 'Opel', 'Porsche', 'Fiat', 'Lancia', 'AlfaRomeo', 'Lamborghini',
         'Maserati', 'Ferrari', 'Citroen', 'Renault', 'Bugatti', 'Alpine', 'Peugeot', 'McLaren', 'AstonMartin',
         'Vauxhall', 'Bentley', 'Rolls-Royce', 'LandRover', 'Mini', 'Chrysler', 'Dodge', 'Jeep', 'Chevrolet', 'Buick',
         'GMC', 'Cadillac', 'Lincoln', 'Ford', 'Honda', 'Toyota', 'Suzuki', 'Lexus', 'Infiniti', 'Mazda', 'Mitsubishi',
         'Nissan', 'Hyundai', 'Kia', 'Daewoo', 'Geely', 'Chery', 'Hongqi', 'Brilliance', 'BYD'])

    global color
    color = np.asarray(
        ['White', 'Yellow', 'Blue', 'Red', 'Green', 'Black', 'Brown', 'Azure', 'Ivory', 'Teal', 'Silver', 'Purple',
         'Navy blue', 'Pea green', 'Gray', 'Orange', 'Maroon', 'Charcoal', 'Aquamarine', 'Coral', 'Fuchsia', 'Wheat',
         'Lime', 'Crimson', 'Khaki', 'Hot pink', 'Magenta', 'Olden', 'Plum', 'Olive', 'Cyan'])

    global schoolName
    schoolName = np.asarray(['institute', 'school', 'university', 'college', 'campus'])

    global poi
    poi = np.asarray(
        ['accounting', 'airport', 'amusement_park', 'aquarium', 'art_gallery', 'atm', 'bakery', 'bank', 'bar',
         'beauty_salon', 'bicycle_store', 'book_store', 'bowling_alley', 'bus_station', 'cafe', 'campground',
         'car_dealer', 'car_rental', 'car_repair', 'car_wash', 'casino', 'cemetery', 'church', 'city_hall',
         'clothing_store', 'convenience_store', 'courthouse', 'dentist', 'department_store', 'doctor', 'drugstore',
         'electrician', 'electronics_store', 'embassy', 'fire_station', 'florist', 'funeral_home', 'furniture_store',
         'gas_station', 'grocery_or_supermarket', 'gym', 'hair_care', 'hardware_store', 'hindu_temple',
         'home_goods_store', 'hospital', 'insurance_agency', 'jewelry_store', 'laundry', 'lawyer', 'library',
         'light_rail_station', 'liquor_store', 'local_government_office', 'locksmith', 'lodging', 'meal_delivery',
         'meal_takeaway', 'mosque', 'movie_rental', 'movie_theater', 'moving_company', 'museum', 'night_club',
         'painter', 'park', 'parking', 'pet_store', 'pharmacy', 'physiotherapist', 'plumber', 'police', 'post_office',
         'primary_school', 'real_estate_agency', 'restaurant', 'roofing_contractor', 'rv_park', 'school',
         'secondary_school', 'shoe_store', 'shopping_mall', 'spa', 'stadium', 'storage', 'store', 'subway_station',
         'supermarket', 'synagogue', 'taxi_stand', 'tourist_attraction', 'train_station', 'transit_station',
         'travel_agency', 'university', 'veterinary_care', 'zoo'])

    global schoolSubject
    schoolSubject = np.asarray(
        ['Math', 'Art', 'Algebra', 'Geometry', 'Science', 'English', 'Music', 'History', 'Science', 'Geography',
         'Information technology', 'Biology', 'Drama', 'Swimming', 'Physical education', 'Physics', 'Chemistry'])

    global _vowels
    _vowels = 'AEIOU'


## Main Function
output__dumbo_path = ''

def profile_colum(_sc, sqlContext, colName, table_name):
    results = []
    colName = fm.Process_column_name_for_dataframe(colName)
    query = "select distinct %s from %s" % (colName, table_name)
    temp = sqlContext.sql(query)
    if temp.count() > 100000:
        return results.append({"skip":table_name})
    temp_col_metadata = {
        "file_name": table_name.replace("_","-"),
        "column_name": colName,
        "semantic_types": semanticCheck(temp)
    }
    results.append(temp_col_metadata)
    return results


def semanticCheck(col):
    # DO NOT CHANGE THE ORDER OF FUNCTION CALLS
    result = []
    parsecolumn(col)
    colSize = col.count()
    for key in label_proportion:
        if label_proportion[key] != 0:
            semantic = {
                "semantic_type": key,
                "count": int(colSize * label_proportion[key])
            }
            result.append(semantic)

    if (len(result) == 0):
        semantic = {
            "semantic_type": 'Other',
            "count": colSize
        }
        result.append(semantic)

    return result


def replace_at(text, position, fromlist, tolist):
    for f, t in zip(fromlist, tolist):
        if text[position:].startswith(f):
            return ''.join([text[:position],
                            t,
                            text[position + len(f):]])
    return text


def replace_end(text, fromlist, tolist):
    for f, t in zip(fromlist, tolist):
        if text.endswith(f):
            return text[:-len(f)] + t
    return text

def nysiis(name):
    if len(name)<1:
        return ''
    name = re.sub(r'\W', '', name).upper()
    name = replace_at(name, 0, ['MAC', 'KN', 'K', 'PH', 'PF', 'SCH'],
                      ['MCC', 'N', 'C', 'FF', 'FF', 'SSS'])
    name = replace_end(name, ['EE', 'IE', 'DT', 'RT', 'RD', 'NT', 'ND'],
                       ['Y', 'Y', 'D', 'D', 'D', 'D', 'D'])
    key, key1 = name[0], ''
    i = 1
    while i < len(name):
        # print(i, name, key1, key)
        n_1, n = name[i - 1], name[i]
        n1_ = name[i + 1] if i + 1 < len(name) else ''
        name = replace_at(name, i, ['EV'] + list(_vowels), ['AF'] + ['A'] * 5)
        name = replace_at(name, i, 'QZM', 'GSN')
        name = replace_at(name, i, ['KN', 'K'], ['N', 'C'])
        name = replace_at(name, i, ['SCH', 'PH'], ['SSS', 'FF'])
        if n == 'H' and (n_1 not in _vowels or n1_ not in _vowels):
            name = ''.join([name[:i], n_1, name[i + 1:]])
        if n == 'W' and n_1 in _vowels:
            name = ''.join([name[:i], 'A', name[i + 1:]])
        if key and key[-1] != name[i]:
            key += name[i]
        i += 1
    key = replace_end(key, ['S', 'AY', 'A'], ['', 'Y', ''])
    return key1 + key


def generalCheck(col, list, label):
    cnt=0
    sampleSize = len(col)

    for i in range(sampleSize):
        ele = col[i]
        flag = False
        for s in list:
            ele = ele.lower()
            s = s.lower()
            s = nysiis(s)
            ele = nysiis(ele)
            if fuzz.ratio(ele, s) > 50:
                flag = True
                break
        if flag:
            cnt += 1
    prob = cnt / sampleSize
    if prob < threshold:
        prob = 0
    label_proportion[label] = prob
    return prob > threshold


def generalCheck2(col, list, label):
    cnt=0
    sampleSize = len(col)

    for i in range(sampleSize):
        ele = col[i]
        flag = False
        for s in list:
            if fuzz.partial_ratio(ele.lower(), s.lower()) > 70:
                flag = True
                break
        if flag:
            cnt += 1
    prob = cnt / sampleSize
    if prob < threshold:
        prob = 0
    label_proportion[label] = prob
    return prob > threshold


def checkNeiborhoods(column, label):
    return generalCheck(column, neiborhood_names, label)

def checkBoroughs(column, label):
    return generalCheck(column, boroughs, label)

def checkWebsites(column, label):
    if type(column) != str:
        return False
    exp = "^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$"
    result = re.search(exp, column)
    if result:
        label_proportion[label] = 1
    else:
        label_proportion[label] = 0
    return result


def checkBusinessName(column, label):
    businessNames = [item['current_entity_name'] for item in business_data]
    return generalCheck(column, businessNames, label)


def checkSchoolLevel(column, label):
    return generalCheck(column, schoolLevels, label)


def checkStreetName(column, label):
    ## If this is not enough, then we can use real street data
    return generalCheck2(column, streets, label)


def checkParkandPlayground(column, label):
    return generalCheck2(column, parks, label)


def checkCityAgencies(column, label):
    return generalCheck(column, agencies, label)


def checkBuildingType(column, label):
    return generalCheck(column, buildingTypes, label)


def proportion(lis, label):
    if sum(lis) >= .8 * len(lis):
        label_proportion[label] = sum(lis) / len(lis)


def parsecolumn(column):
    size = column.count()
    sample = clamp(size)
    elem = []
    if size < 0:
        elem = column.toPandas().values.flatten().tolist()
    else:
        elem = column.toPandas().sample(sample).values.flatten().tolist()
    if len(elem) == 0:
        return

    checkBusinessName(elem, "Business Name")
    checkSchoolLevel(elem, 'School Levels')
    checkStreetName(elem, 'Street Name')
    checkParkandPlayground(elem, 'Park/Playground')
    checkCityAgencies(elem, 'City agency')
    checkBuildingType(elem, 'Building Classification')
    checkNeiborhoods(elem, 'Neighborhood')
    checkBoroughs(elem, 'Borough')
    checkCarMake(elem, 'Car Make')
    checkAreasOfStudy(elem, 'Areas of study')
    checkWebsites(elem, 'Websites')
    checkColor(elem, 'Color')
    checkTypeOfLocation(elem, 'Type of location')
    checkSchoolSubject(elem, 'Subjects in school')
    namecheck(elem, 'Name')
    phonecheck(elem, 'Phone Number')
    zipcodeCheck(elem, 'Zipcode')
    collegeCheck(elem, 'College')
    FieldCheck(elem, 'Study')
    CarType(elem, 'Vehicle Type')
    latlon(elem, 'Langitude/Longitude')


def checkCarMake(column, label):
    return generalCheck2(column, carBrand, label)


def checkColor(column, label):
    return generalCheck(column, color, label)


def checkSchoolName(column, label):
    return generalCheck2(column, schoolName, label)


def checkTypeOfLocation(column, label):
    return generalCheck2(column, poi, label)


def checkSchoolSubject(column, label):
    return generalCheck2(column, schoolSubject, label)


def checkAreasOfStudy(column, label):
    return generalCheck(column, subjects, label)


def namecheck(col, label):
    first_name_df = first_name.dropna()
    last_name_df = last_name.dropna()
    lis = []
    for item in col:
        name = nysiis(item)
        count = 0
        for index, row in first_name_df.iterrows():
            rat = fuzz.ratio(name, row['ny'])
            if rat > 99:
                return lis.append(True)
            else:
                if count < 10000:
                    for index, row in last_name_df.iterrows():
                        rat1 = fuzz.ratio(name, row['ny'])
                        if rat1 > 90:
                            return lis.append(True)
                        else:
                            count += 1
                            continue
        return lis.append(False)
    proportion(lis, label)


def phonecheck(col, label):
    lis = []
    for item in col:
        pattern = '^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$'
        r = re.match(pattern, item)
        if r:
            lis.append(True)
        else:
            lis.append(False)
    proportion(lis, label)


def zipcodeCheck(col, label):
    lis = []
    for item in col:
        if len(item) == 5:
            lis.append(True)
        else:
            lis.append(False)
    proportion(lis, label)


def collegeCheck(col, label):
    lis = []
    for item in col:
        val = str(item)
        max = 0
        for ind in colleges.index:
            temp = fuzz.ratio(val, colleges['NAME'][ind])
            if temp > max:
                max = temp
        if max > 50:
            lis.append(True)
        else:
            lis.append(False)
    proportion(lis, label)


def FieldCheck(col, label):
    lis = []
    f = fields.dropna()
    for item in col:
        val = str(item)
        max = 0
        for ind in f.index:
            temp = fuzz.ratio(val, f['Arts'][ind])
            if temp > max:
                max = temp
        if max > 50:
            lis.append(True)
        else:
            lis.append(False)
    proportion(lis, label)


def CarType(col, label):
    lis = []
    c = cars.dropna()
    for item in col:
        val = str(item)
        max = 0
        for ind in c.index:
            temp = fuzz.ratio(val, c['Unnamed: 0'][ind])
            if temp > max:
                max = temp
        if max > 50:
            lis.append(True)
        else:
            lis.append(False)
    proportion(lis, label)


def latlon(col, label):
    lis = []
    pattern = '^[-]?(([0-8]?[0-9])\.(\d+))|(90(\.0+)?),[-]?((((1[0-7][0-9])|([0-9]?[0-9]))\.(\d+))|180(\.0+)?)$'
    for item in col:
        test = str(item)
        result = re.match(pattern, test)
        if result:
            lis.append(True)
        else:
            lis.append(False)
    proportion(lis, label)


def clamp(num, limit=1000):
    if num > limit:
        return limit
    return num


if __name__ == '__main__':
    config = pyspark.SparkConf().setAll(
        [('spark.executor.memory', '8g'), ('spark.executor.cores', '5'), ('spark.cores.max', '5'),
         ('spark.driver.memory', '8g')])
    sc = SparkContext(conf=config)
    sc.addFile("FileInputManager.py")
    sc.addFile("task2_coinflippers.py")
    sc.addFile("task1_coinflippers.py")

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    fm.iterate_files_from_file(sc, spark, sqlContext,  "/user/yy3090/input/task2_filename.txt", output__dumbo_path)
    sc.stop()
