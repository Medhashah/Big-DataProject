import sys
import pyspark
import string
import os
import json

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import *

if __name__ == "__main__":

	    sc = SparkContext()

	    spark = SparkSession \
		       .builder \
		       .appName("profiling") \
		       .config("spark.some.config.option", "some-value") \
		       .getOrCreate()

	    sqlContext = SQLContext(sparkContext = spark.sparkContext, sparkSession = spark)

	    # get command-line arguments
	    inFile = sys.argv[1]


	    print ("Executing data profiling with input from " + inFile)

#================== read file =====================#
	    text_file = sc.textFile(inFile)
        #spliting against '\t' return list of list[row]
        list_row  = text_file.map(lambda x: [x.split("\t")[0]])
        perct = {}
        df = list_row.toDF() #convert to dataframe
        df_count = df.count() #count rows in dataframe
        col_name = df.columns[0] # retrive column name

#================== semantic types =====================#
    	# city
        df_city = df.where( col(col_name).like("%ROO%") | col(col_name).like("%BEACH%") | col(col_name).like("%CITY%") | col(col_name).like("%DALE%") | \
                            col(col_name).like("%EAST%") | col(col_name).like("%HIL%") | col(col_name).like("%LAND%") | col(col_name).like("%LAKE%") | \
                            col(col_name).like("%NOR%") | col(col_name).like("%TON%") | col(col_name).like("%PARK%") | col(col_name).like("%PORT%") | \
                            col(col_name).like("%NEW%") | col(col_name).like("%SAN%") | col(col_name).like("%SOUTH%") | col(col_name).like("%ROCK%") | \
                            col(col_name).like("%TOWN%") | col(col_name).like("%WEST%") | col(col_name).like("%VILL%") | col(col_name).like("%WOOD%") | \
                            col(col_name).like("%YORK%") | col(col_name).like("%MAN%") | col(col_name).like("%LONG%") | col(col_name).like("%POINT%") | \
                            col(col_name).like("%JAMAICA%") | col(col_name).like("%OZONE%") | col(col_name).like("%JACKSON%") | col(col_name).like("%GARDEN%") | \
                            col(col_name).like("% NY %") | col(col_name).like("% NY%"))


        city_count = df_city.count()
        city_perct = (city_count/df_count)*100


        # street_name
        df_street = df.where( col(col_name).like("%STREET%") | col(col_name).like("%AVE%") | col(col_name).like("%PLACE%") | col(col_name).like("%ROAD%") | \
                              col(col_name).like("%COURT%") | col(col_name).like("%DRIV%") | col(col_name).like("%PARK%") | col(col_name).like("%LANE%") | \
                              col(col_name).like("%TERRACE%") | col(col_name).like("%WEST%") | col(col_name).like("%SOUTH%") | col(col_name).like("%EAST%") | \
                              col(col_name).like("%NORTH%"))

        street_count = df_street.count()
        street_perct = (street_count /df_count)*100


        # website
        df_website = df.where( col(col_name).like("%ORG%") | col(col_name).like("%PORTAL%") | col(col_name).like("%GOV%") | col(col_name).like("%COM%") | \
                               col(col_name).like("%WWW%") | col(col_name).like("%NET%"))

        website_count = df_website.count()
        website_perct = (website_count/df_count)*100


        #building_classification
        df_building = df.where( col(col_name).like("%WALK%") | col(col_name).like("%ELEVATOR%") | col(col_name).like("%CONDOPS%"))

        building_count = df_building.count()
        building_perct = (building_count/df_count)*100


        #borough
        df_borough = df.where( col(col_name).like("M") | col(col_name).like("K") | col(col_name).like("Q") | col(col_name).like("R") | \
                               col(col_name).like("X") | col(col_name).like("%MANHAT%") | col(col_name).like("%QUEEN%") | col(col_name).like("%BROO%") | \
                               col(col_name).like("%QUEEN%") | col(col_name).like("%BRONX%") | col(col_name).like("%RICH%") | \
                               col(col_name).like("%STATEN%"))

        borough_count = df_borough.count()
        borough_perct = (borough_count / df_count) * 100


        #person_name
        df_person = df.where( col(col_name).like("%H_M%") | col(col_name).like("%R_N%") | col(col_name).like("%A_B%") | col(col_name).like("A_D%") | \
                              col(col_name).like("%A_E%") | col(col_name).like("%A_A%") | col(col_name).like("%A_N%") | col(col_name).like("%A_O%") | \
                              col(col_name).like("%A_U%") | col(col_name).like("%A_T%") | col(col_name).like("%ADM%") | col(col_name).like("%A_L%") | \
                              col(col_name).like("%A_R%") | col(col_name).like("%A_L%") | col(col_name).like("%A_I%") | col(col_name).like("%A_J%") | \
                              col(col_name).like("%A_V%") | col(col_name).like("%A_J%") | col(col_name).like("%A_K%") | col(col_name).like("%A_I%") | \
                              col(col_name).like("%A_N%") | col(col_name).like("%A_L%") | col(col_name).like("%A_O%") | col(col_name).like("%A_P%") | \
                              col(col_name).like("%A_S%") | col(col_name).like("%A_U%") | col(col_name).like("%A_Y%") | col(col_name).like("%A_Z%") | \
                              col(col_name).like("%B_A%") | col(col_name).like("%C_A%") | col(col_name).like("%D_A%") | col(col_name).like("%E_A%") | \
                              col(col_name).like("%B_D%") | col(col_name).like("%C_B%") | col(col_name).like("%D_E%") | col(col_name).like("%E_E%") | \
                              col(col_name).like("%B_E%") | col(col_name).like("%C_D%") | col(col_name).like("%D_I%") | col(col_name).like("%E_L%") | \
                              col(col_name).like("%B_G%") | col(col_name).like("%C_F%") | col(col_name).like("%D_O%") | col(col_name).like("%E_R%") | \
                              col(col_name).like("%B_H%") | col(col_name).like("%C_G%") | col(col_name).like("%D_U%") | col(col_name).like("%E_O%") | \
                              col(col_name).like("%B_I%") | col(col_name).like("%C_I%") | col(col_name).like("%D_L%") | col(col_name).like("%E_Y%") | \
                              col(col_name).like("%B_U%") | col(col_name).like("%C_U%") | col(col_name).like("%D_M%") | col(col_name).like("%E_M%") | \
                              col(col_name).like("%B_L%") | col(col_name).like("%C_M%") | col(col_name).like("%D_D%") | col(col_name).like("%E_N%") | \
                              col(col_name).like("%B_M%") | col(col_name).like("%C_N%") | col(col_name).like("%D_N%") | col(col_name).like("%E_Z%") | \
                              col(col_name).like("%B_N%") | col(col_name).like("%C_P%") | col(col_name).like("%D_Y%") | col(col_name).like("%L_E%"))

        person_count = df_person.count()
        person_perct = (person_count/ df_count) * 100


        #business_name
        df_business = df.where( col(col_name).like("%RESTAURANT%") | col(col_name).like("%PIZZ%") | col(col_name).like("%CAFE%") | col(col_name).like("%DELI%") | \
                                col(col_name).like("%GRILL%") | col(col_name).like("%CHINESE%") | col(col_name).like("%KITCHEN%") | col(col_name).like("%THAI%") | \
                                col(col_name).like("%SUSHI%") | col(col_name).like("%CUISINE%") | col(col_name).like("%BAR%") | col(col_name).like("%SHOP%") | \
                                col(col_name).like("%DINER%") | col(col_name).like("%FOOD%") | col(col_name).like("%HOUSE%") | col(col_name).like("%CHICKEN%") | \
                                col(col_name).like("%CHINA%") | col(col_name).like("%COFFEE%") | col(col_name).like("%BURGER%") | col(col_name).like("%MARKET%") | \
                                col(col_name).like("%GOURMET%") | col(col_name).like("%MEXICAN%") | col(col_name).like("%INDIAN%") | col(col_name).like("%BAGEL%") |\
                                col(col_name).like("%KING%") | col(col_name).like("%EXPRESS%") | col(col_name).like("%GARDEN%") | \
                                col(col_name).like("%NEW%"))

        business_count = df_business.count()
        business_perct = (business_count/df_count) * 100

        # phone_number
        df_phone_number = df.where(col(col_name).rlike("^[0-9]{3}-[0-9]{3}-[0-9]{4}$"))

        phone_number_count = df_phone_number.count()
        phone_number_perct = (phone_number_count/df_count)*100


        #address
        df_address = df.where( col(col_name).like("%STREET%") | col(col_name).like("%AVE%") | col(col_name).like("%PLACE%") | col(col_name).like("%ROAD%") | \
                               col(col_name).like("%COURT%") | col(col_name).like("%DRIV%") | col(col_name).like("%PARK%") | col(col_name).like("%LOTS%") | \
                               col(col_name).like("%RAND%") | col(col_name).like("%WEST%") | col(col_name).like("%SOUTH%") | col(col_name).like("%EAST%") | \
                               col(col_name).like("%ROO%") | col(col_name).like("%BEACH%") | col(col_name).like("%BOULEVARD%") | col(col_name).like("%BROADWAY%") | \
                               col(col_name).like("%CONEY%") | col(col_name).like("%HIL%") | col(col_name).like("%LAND%") | col(col_name).like("%JAMAICA%") | \
                               col(col_name).like("%NOR%") | col(col_name).like("%TON%") | col(col_name).like("%PARK%") | col(col_name).like("%MYRTLE%") | \
                               col(col_name).like("%NEW%") | col(col_name).like("%CHURCH%") | col(col_name).like("%ROCK%") | col(col_name).like("%FULTON%") | \
                               col(col_name).like("%METROP%") | col(col_name).like("%TREMONT%") | col(col_name).like("%WOOD%") | col(col_name).like("%FLATBUSH%") | \
                               col(col_name).like("%WHITE%") | col(col_name).like("%PLAINS%") | col(col_name).like("%LIBERTY%") | col(col_name).like("%BLVD%") | \
                               col(col_name).like("%MAN%") | col(col_name).like("%OCEAN%") | col(col_name).like("%AMSTERDAM%"))

        address_count = df_address.count()
        address_perct = (address_count/df_count) * 100


        #neighborhood
        df_neighborhood = df.where( col(col_name).like("%ARROCHAR%") | col(col_name).like("%DONGAN%") | col(col_name).like("%GRANT%") | col(col_name).like("%GRYMES%") | \
                                    col(col_name).like("%SPRING%") | col(col_name).like("%ROSEBANK%") | col(col_name).like("%SILVER%") | col(col_name).like("%SUNNY%") | \
                                    col(col_name).like("%TOMPKINS%"))

        neighbor_count = df_neighborhood.count()
        neighbor_perct = (neighbor_count/df_count) * 100


        #lat_lon_cord
        df_cord = df.where( col(col_name).like("(__.%") | col(col_name).like("__.%"))

        cord_count = df_cord.count()
        cord_perct = (cord_count/df_count) * 100


        #zip_code
        df_zip = df.where( col(col_name).rlike("^[0-9]{2}$"))

        zip_count = df_zip.count()
        zip_perct = (zip_count/df_count) * 100


        #school_name
        df_school = df.where( col(col_name).like("%PUBLIC%") | col(col_name).like("%SCHOOL%") | col(col_name).like("%CHILDREN%") | col(col_name).like("%ACADEMY%") | \
                              col(col_name).like("%COLLEGE%") | col(col_name).like("%EDU%") | col(col_name).like("%TECH%") | col(col_name).like("%MID%") | \
                              col(col_name).like("%PS%"))

        school_count = df_school.count()
        school_perct = (school_count / df_count) * 100


        #color
        df_color = df.where( col(col_name).like("BLK%") | col(col_name).like("BLU%") | col(col_name).like("RED%") | col(col_name).like("%RD%") | \
                             col(col_name).like("%LUE%") | col(col_name).like("%BAI%") | col(col_name).like("%BEI%") | col(col_name).like("%BRO%") | \
                             col(col_name).like("%GRE%") | col(col_name).like("%BL%") | col(col_name).like("%WT%") |col(col_name).like("%WH%") | \
                             col(col_name).like("%YL%") | col(col_name).like("%YEL%") |col(col_name).like("%TN%") |col(col_name).like("%ROWN%") | \
                             col(col_name).like("%SLV%") |col(col_name).like("%RED%") |col(col_name).like("%SIL%") |col(col_name).like("%GRN%") | \
                             col(col_name).like("%MAC%") | col(col_name).like("%MOR%") | col(col_name).like("%BEI%") | col(col_name).like("%NAV%") | \
                             col(col_name).like("%NVY%") | col(col_name).like("%BL%") | col(col_name).like("%WT%") |col(col_name).like("%WH%") | \
                             col(col_name).like("%YL%") | col(col_name).like("%YEL%") |col(col_name).like("%TN%") |col(col_name).like("%ROWN%") | \
                             col(col_name).like("%SLV%") |col(col_name).like("%RED%") |col(col_name).like("%SIL%") |col(col_name).like("%GRN%") | \
                             col(col_name).like("LT%"))

        color_count = df_color.count()
        color_perct = (color_count / df_count)*100


        #car_make
        df_car = df.where( col(col_name).like("VOL%") | col(col_name).like("%AC_%") | col(col_name).like("%AE_%") | col(col_name).like("%AD_%") | \
                           col(col_name).like("%AI_%") | col(col_name).like("%AL_%") | col(col_name).like("%AO_%") | col(col_name).like("%AM_%") | \
                           col(col_name).like("%AP_%") | col(col_name).like("%AN_%") |col(col_name).like("%AR_%") | col(col_name).like("%AS_%")  | \
                           col(col_name).like("%AT_%") | col(col_name).like("%AU_%") | col(col_name).like("%AZ_%") | col(col_name).like("%AV_%") | \
                           col(col_name).like("%CA_%") | col(col_name).like("%CH_%") | col(col_name).like("%CE_%") | col(col_name).like("%CD_%") | \
                           col(col_name).like("%CI_%") | col(col_name).like("%CL_%") | col(col_name).like("%CO_%") | col(col_name).like("%CM_%") | \
                           col(col_name).like("%CP_%") | col(col_name).like("%CN_%") |col(col_name).like("%CR_%") | col(col_name).like("%CS_%")  | \
                           col(col_name).like("%CT_%") | col(col_name).like("%CU_%") | col(col_name).like("%CZ_%") | col(col_name).like("%CV_%") | \
                           col(col_name).like("%BA_%") | col(col_name).like("%BC_%") | col(col_name).like("%BE_%") | col(col_name).like("%BD_%") | \
                           col(col_name).like("%BI_%") | col(col_name).like("%BL_%") | col(col_name).like("%BO_%") | col(col_name).like("%BM_%") | \
                           col(col_name).like("%BP_%") | col(col_name).like("%BN_%") |col(col_name).like("%BR_%") | col(col_name).like("%BS_%")  | \
                           col(col_name).like("%BT_%") | col(col_name).like("%BU_%") | col(col_name).like("%BZ_%") | col(col_name).like("%BV_%") | \
                           col(col_name).like("%PA_%") | col(col_name).like("%PC_%") | col(col_name).like("%PE_%") | col(col_name).like("%PD_%") | \
                           col(col_name).like("%PI_%") | col(col_name).like("%PL_%") | col(col_name).like("%PO_%") | col(col_name).like("%PM_%") | \
                           col(col_name).like("%PP_%") | col(col_name).like("%PN_%") |col(col_name).like("%PR_%") | col(col_name).like("%PS_%")  | \
                           col(col_name).like("%PT_%") | col(col_name).like("%PU_%") | col(col_name).like("%PZ_%") | col(col_name).like("%PV_%"))

        car_make_count = df_car.count()
        car_make_perct = (car_make_count/df_count)*100


        #city_agency
        df_agency = df.where( col(col_name).like("311") | col(col_name).like("ACS") | col(col_name).like("BIC") | col(col_name).like("BOE") | col(col_name).like("BPL") | \
                              col(col_name).like("CC__") | col(col_name).like("DHS") | col(col_name).like("CUNY") | col(col_name).like("DCA%") | col(col_name).like("NYP%") | \
                              col(col_name).like("DCLA") | col(col_name).like("DCP") | col(col_name).like("DDC") | col(col_name).like("DEP") | col(col_name).like("DFTA") | \
                              col(col_name).like("DO_") | col(col_name).like("DO___") | col(col_name).like("DPR") | col(col_name).like("DSNY") | col(col_name).like("DVS") | \
                              col(col_name).like("DYCD") | col(col_name).like("EDC") | col(col_name).like("FDNY") | col(col_name).like("HPD") | col(col_name).like("HRA") | \
                              col(col_name).like("LAW") | col(col_name).like("LPC") | col(col_name).like("NYC__") | col(col_name).like("OATH") | col(col_name).like("OCME") | \
                              col(col_name).like("QPL") | col(col_name).like("SBS") | col(col_name).like("SCA") | col(col_name).like("TLC"))

        city_agency_count = df_agency.count()
        city_agency_perct = (city_agency_count/df_count)*100


        # area_of_study
        df_study = df.where( col(col_name).like("%SCIENCE%") | col(col_name).like("%DESIGN%") | col(col_name).like("%ARTS%") | col( col_name).like("%PERFORMING%") | \
                             col(col_name).like("%TEACH%") | col(col_name).like("%COMMUNI%") | col(col_name).like("%ARCHITECT%") | col(col_name).like("%TECHNO%") | \
                             col(col_name).like("%HOSPITALITY%") | col(col_name).like("%HUMANIT%") | col(col_name).like("%MATH%") | col(col_name).like("%HEALTH%") | \
                             col(col_name).like("%ENVIRON%") | col(col_name).like("%INTEREST%") | col(col_name).like("%GOVERNMENT%") | col(col_name).like("%COMPUTER%") | \
                             col(col_name).like("%BUSINESS%") | col(col_name).like("%CULINARY%") | col(col_name).like("%LAW%"))

        area_of_study_count = df_study.count()
        area_of_study_perct = (area_of_study_count/df_count)*100


        #subject_in_school
        df_subject = df.where( col(col_name).like("%ALGEBRA%") | col(col_name).like("%CHEM%") | col(col_name).like("%BIO%") | col(col_name).like("%MATH%") | \
                               col(col_name).like("%ECO%") | col(col_name).like("%ENG%") | col(col_name).like("%GEO%") | col(col_name).like("%GLO%") | \
                               col(col_name).like("%ENV%") | col(col_name).like("%PHY%") | col(col_name).like("%GOV%") | col(col_name).like("%HIST%"))

        school_subject_count = df_subject.count()
        school_subject_perct = (school_subject_count/df_count)*100


        #school_level
        df_school_level = df.where( col(col_name).like("%SCHOOL%") | col(col_name).like("%ELEMENTARY%") | col(col_name).like("%HIGH%") | col(col_name).like("KINDER%") | \
                                    col(col_name).like("%TRANSFER%"))

        school_level_count = df_school_level.count()
        school_level_perct = (school_level_count/df_count)*100


        #vehicle_type
        df_vehicle_type = df.where( col(col_name).like("4__") | col(col_name).like("2_") | col(col_name).like("2__") | col(col_name).like("2___") | \
                                    col(col_name).like("4___") | col(col_name).like("CM__") | col(col_name).like("CM_") | col(col_name).like("CN_") | \
                                    col(col_name).like("C_") | col(col_name).like("D_") | col(col_name).like("DE_") | col(col_name).like("DE__") | \
                                    col(col_name).like("E_") | col(col_name).like("EX__") | col(col_name).like("FR_") | col(col_name).like("FR__") | \
                                    col(col_name).like("CN__") | col(col_name).like("CN_") | col(col_name).like("CM_") | col(col_name).like("CM__") | \
                                    col(col_name).like("CX_") | col(col_name).like("CX__") | col(col_name).like("D_") | col(col_name).like("DEL_") | \
                                    col(col_name).like("DE_") | col(col_name).like("DU__") | col(col_name).like("DU_") | col(col_name).like("IN__") | \
                                    col(col_name).like("RE_") | col(col_name).like("RE__") | col(col_name).like("FR_") | col(col_name).like("FR__") | \
                                    col(col_name).like("M_") | col(col_name).like("MO_") | col(col_name).like("MO__") | col(col_name).like("ON_") | \
                                    col(col_name).like("O_") | col(col_name).like("OL__") | col(col_name).like("ON_") | col(col_name).like("ON__") | \
                                    col(col_name).like("SU_") | col(col_name).like("SU__") | col(col_name).like("ST__") | col(col_name).like("SE__") | \
                                    col(col_name).like("T_") | col(col_name).like("TL_") | col(col_name).like("TK__") | col(col_name).like("TR__") | \
                                    col(col_name).like("TR_") | col(col_name).like("UT__") | col(col_name).like("UT_") | col(col_name).like("IN__") | \
                                    col(col_name).like("SN_") | col(col_name).like("SN__") | col(col_name).like("SA_") | col(col_name).like("SE__"))

        vehicle_type_count = df_vehicle_type.count()
        vehicle_type_perct = (vehicle_type_count/df_count)*100


        # park_playground
        df_playground = df.where( col(col_name).like("%SCHOOL%") | col(col_name).like("%PARK%") | col(col_name).like("%PLAYGROUND%") | col(col_name).like("%HIGH%") | \
                                  col(col_name).like("%CENTER%") | col(col_name).like("%POOL%") | col(col_name).like("%RECREATION%") | col(col_name).like("%CENTRAL%") | \
                                  col(col_name).like("%BEACH%") | col(col_name).like("%FIELD%") | col(col_name).like("%PROSPECT%") | col(col_name).like("%ACADEMY%") | \
                                  col(col_name).like("%COMMUNITY%"))

        park_type_count = df_playground.count()
        park_type_perct = (park_type_count/df_count)*100


        #other

        perct = {
                    "AGENCY": city_agency_perct,
                    "CAR MAKE": car_make_perct,
                    "BUSINESS NAME":business_perct,
                    "BUILDING CLASSIFICATION" :building_perct,
                    "BOROUGH": borough_perct,
                    "PERSON NAME" :person_perct,
                    "ADDRESS":address_perct,
                    "CITY":city_perct,
                    "COLOR":color_perct,
                    "LAT/LON":cord_perct,
                    "NEIGHBOURHOOD":neighbor_perct,
                    "SCHOOL_LEVEL": school_level_perct ,
                    "SCHOOL_NAME": school_perct ,
                    "SCHOOL SUBJECT": school_subject_perct,
                    "STREET":street_perct,
                    "VEHICLE TYPE":vehicle_type_perct,
                    "WEBSITE": website_perct,
                    "ZIPCODE":zip_perct
                }



        max_percent = sorted(perct.values(),reverse=True)[0]

        matched_label = ""

        for label, value in perct.items():
            if value == max_percent:
                matched_label = label


# ================== Saving as JSON file =====================

        {
            "column_name" : inFile,
            "semantic_types": [
                {
                        "semantic_type": matched_label,
                }
            ]
        }