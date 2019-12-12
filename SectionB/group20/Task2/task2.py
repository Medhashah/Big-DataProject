import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import soundex
import json
from pyspark.sql.functions import lower, col
import re
import nltk
import os
from nltk.tag import StanfordNERTagger

stanford_ner_tagger = StanfordNERTagger('stanford-ner-2018-10-16/' + 'classifiers/english.muc.7class.distsim.crf.ser.gz', 'stanford-ner-2018-10-16/' + 'stanford-ner-3.9.2.jar')

# spark context
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession \
        .builder \
        .appName("task2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

all_filenames = ['5694-9szk.Business_Website_or_Other_URL.txt.gz', 'uwyv-629c.StreetName.txt.gz', 'faiq-9dfq.Vehicle_Color.txt.gz', 'qcdj-rwhu.BUSINESS_NAME2.txt.gz', '6ypq-ih9a.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'pvqr-7yc4.Vehicle_Color.txt.gz', 'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'uq7m-95z8.interest6.txt.gz', '5ziv-wcy4.WEBSITE.txt.gz', 'ydkf-mpxb.CrossStreetName.txt.gz', 'w9ak-ipjd.Applicant_Last_Name.txt.gz', 'jz4z-kudi.Respondent_Address__City_.txt.gz', 'rbx6-tga4.Owner_Street_Address.txt.gz', 'sqmu-2ixd.Agency_Name.txt.gz', 'aiww-p3af.Incident_Zip.txt.gz', 'mmvm-mvi3.Org_Name.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz', 'uh2w-zjsn.Borough.txt.gz', 'tqtj-sjs8.FromStreetName.txt.gz', 'mqdy-gu73.Color.txt.gz', '7jkp-5w5g.Agency.txt.gz', 's3zn-tf7c.QUEENS_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'sqcr-6mww.School_Name.txt.gz', 'vrn4-2abs.SCHOOL_LEVEL_.txt.gz', '2sps-j9st.PERSON_LAST_NAME.txt.gz', '2bmr-jdsv.DBA.txt.gz', '4d7f-74pe.Address.txt.gz', 'ji82-xba5.address.txt.gz', 'hy4q-igkk.School_Name.txt.gz', 's9d3-x4fz.EMPCITY.txt.gz', '5uac-w243.PREM_TYP_DESC.txt.gz', '64gx-bycn.EMPCITY.txt.gz', 'e9xc-u3ds.CANDMI.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz', 'p937-wjvj.HOUSE_NUMBER.txt.gz', 'dj4e-3xrn.SCHOOL_LEVEL_.txt.gz', 'qu8g-sxqf.MI.txt.gz', 'mdcw-n682.Middle_Initial.txt.gz', 'pq5i-thsu.DVC_MAKE.txt.gz', 'ub9e-s7ai.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '52dp-yji6.Owner_First_Name.txt.gz', 'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz', 'vx8i-nprf.MI.txt.gz', 'k3cd-yu9d.Location_1.txt.gz', 'p6h4-mpyy.PRINCIPAL_PHONE_NUMBER.txt.gz', 'sybh-s59s.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'kz72-dump.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '7btz-mnc8.Provider_Last_Name.txt.gz', 'ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz', 'mjux-q9d4.SCHOOL_LEVEL_.txt.gz', 'hjvj-jfc9.Borough.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz', 'easq-ubfe.CITY.txt.gz', 'sv2w-rv3k.BORO.txt.gz', 'qu8g-sxqf.First_Name.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_First_Name.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz', 'pgtq-ht5f.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', '52dp-yji6.Owner_Last_Name.txt.gz', 's3k6-pzi2.interest4.txt.gz', '4y63-yw9e.SCHOOL_NAME.txt.gz', 'gez6-674h.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'a9md-ynri.MI.txt.gz', 'u553-m549.Independent_Website.txt.gz', 'uzcy-9puk.Street_Name.txt.gz', 'dg92-zbpx.VendorAddress.txt.gz', 'jcih-dj9q.QUEENS_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', '735p-zed8.CANDMI.txt.gz', 'vg63-xw6u.CITY.txt.gz', 'aiww-p3af.Cross_Street_1.txt.gz', 'sa5w-dn2t.Agency.txt.gz', 'cspg-yi7g.ADDRESS.txt.gz', 'crbs-vur7.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'erm2-nwe9.City.txt.gz', 'nyis-y4yr.Owner_s__Phone__.txt.gz', 'tukx-dsca.Address_1.txt.gz', '9b9u-8989.DBA.txt.gz', 'e4p3-6ecr.Agency_Name.txt.gz', '5mw2-hzqx.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz', 'w6yt-hctp.COMPARABLE_RENTAL_1__Building_Classification.txt.gz', 'k3cd-yu9d.CANDMI.txt.gz', 'ii2w-6fne.Borough.txt.gz', 'w7w3-xahh.Location.txt.gz', 'erm2-nwe9.Park_Facility_Name.txt.gz', '5nz7-hh6t.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '8wbx-tsch.Website.txt.gz', 'xne4-4v8f.SCHOOL_LEVEL_.txt.gz', 'vw9i-7mzq.neighborhood.txt.gz', 'yayv-apxh.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.Park_Facility_Name.txt.gz', 'jz4z-kudi.Violation_Location__City_.txt.gz', 'kiv2-tbus.Vehicle_Body_Type.txt.gz', 'fzv4-jan3.SCHOOL_LEVEL_.txt.gz', 'w7w3-xahh.Address_ZIP.txt.gz', 'i9pf-sj7c.INTEREST.txt.gz', 'ci93-uc8s.ZIP.txt.gz', 'jtus-srrj.School_Name.txt.gz', 'a5td-mswe.Vehicle_Color.txt.gz', '29bw-z7pj.Location_1.txt.gz', 'vw9i-7mzq.interest4.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', '3rfa-3xsf.Incident_Zip.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz', 'kj4p-ruqc.StreetName.txt.gz', '4pt5-3vv4.Location.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz', 'pqg4-dm6b.Address1.txt.gz', 'cqc8-am9x.Borough.txt.gz', '6rrm-vxj9.parkname.txt.gz', 'tg4x-b46p.ZipCode_s_.txt.gz', 'jzt2-2f7h.School_Name.txt.gz', 'ci93-uc8s.Website.txt.gz', 'm59i-mqex.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'uzcy-9puk.School_Phone_Number.txt.gz', 'ci93-uc8s.Vendor_DBA.txt.gz', 'cyfw-hfqk.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', '3rfa-3xsf.Intersection_Street_2.txt.gz', 'uqxv-h2se.neighborhood.txt.gz', 'w9ak-ipjd.Owner_s_Street_Name.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz', '4twk-9yq2.CrossStreet2.txt.gz', 'fbaw-uq4e.CITY.txt.gz', 'mdcw-n682.First_Name.txt.gz', 'w7w3-xahh.Address_City.txt.gz', 'i4ni-6qin.PRINCIPAL_PHONE_NUMBER.txt.gz', 'imfa-v5pv.School_Name.txt.gz', 'sxx4-xhzg.Park_Site_Name.txt.gz', 'vw9i-7mzq.interest1.txt.gz', 'sqcr-6mww.Cross_Street_1.txt.gz', '6anw-twe4.FirstName.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'uzcy-9puk.Park_Facility_Name.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', 'c284-tqph.Vehicle_Color.txt.gz', 'm56g-jpua.COMPARABLE_RENTAL___1___Building_Classification.txt.gz', 'tsak-vtv3.Upcoming_Project_Name.txt.gz', 'tg3t-nh4h.BusinessName.txt.gz', 'cgz5-877h.SCHOOL_LEVEL_.txt.gz', 'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz', 'us4j-b5zt.Agency.txt.gz', 'vr8p-8shw.DVT_MAKE.txt.gz', '3qfc-4tta.BRONX_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', 'bawj-6bgn.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'ci93-uc8s.fax.txt.gz', 'ffnc-f3aa.SCHOOL_LEVEL_.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz', 'rbx6-tga4.Owner_Street_Address.txt.gz', 's3k6-pzi2.interest5.txt.gz', '2sps-j9st.PERSON_FIRST_NAME.txt.gz', 'ji82-xba5.street.txt.gz', 'f7qh-bcr5.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', '3rfa-3xsf.Street_Name.txt.gz', 'n84m-kx4j.VEHICLE_MAKE.txt.gz', 'hy4q-igkk.Location.txt.gz', 'sxmw-f24h.Cross_Street_2.txt.gz', 'yahh-6yjc.School_Type.txt.gz', '72ss-25qh.Agency_ID.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', 'm56g-jpua.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', '3rfa-3xsf.School_Name.txt.gz', 'ic3t-wcy2.Applicant_s_First_Name.txt.gz', 'vw9i-7mzq.interest3.txt.gz', 'i6b5-j7bu.TOSTREETNAME.txt.gz', 'i5ef-jxv3.Agency.txt.gz', '7crd-d9xh.website.txt.gz', 'mdcw-n682.Last_Name.txt.gz', 'ge8j-uqbf.interest.txt.gz', 'q2ni-ztsb.Street_Address_1.txt.gz', '8k4x-9mp5.Last_Name__only_2014_15_.txt.gz', 'wks3-66bn.School_Name.txt.gz', '43nn-pn8j.DBA.txt.gz', 'qgea-i56i.PREM_TYP_DESC.txt.gz', 'bdjm-n7q4.CrossStreet2.txt.gz', 'nhms-9u6g.Name__Last__First_.txt.gz', 'bdjm-n7q4.Location.txt.gz', 'x3kb-2vbv.School_Name.txt.gz', 'uzcy-9puk.Location.txt.gz', '6anw-twe4.LastName.txt.gz', 'tyfh-9h2y.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', '3rfa-3xsf.Cross_Street_2.txt.gz', 'bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz', '9jgj-bmct.Incident_Address_Street_Name.txt.gz', 'pdpg-nn8i.BORO.txt.gz', 'w9ak-ipjd.Owner_s_Business_Name.txt.gz', 'rb2h-bgai.Website.txt.gz', 'jt7v-77mi.Vehicle_Make.txt.gz', 'as69-ew8f.TruckMake.txt.gz', 'mrxb-9w9v.BOROUGH___COMMUNITY.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz', 'dm9a-ab7w.AUTH_REP_LAST_NAME.txt.gz', '9z9b-6hvk.Borough.txt.gz', 'wv4q-e75v.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'kwmq-dbub.CANDMI.txt.gz', 'dvzp-h4k9.COMPARABLE_RENTAL_____1_____Building_Classification.txt.gz', '6ypq-ih9a.BOROUGH.txt.gz', 'p2d7-vcsb.ACCOUNT_CITY.txt.gz', '2v9c-2k7f.DBA.txt.gz', 'erm2-nwe9.Landmark.txt.gz', 'dm9a-ab7w.APPLICANT_FIRST_NAME.txt.gz', '72ss-25qh.Borough.txt.gz', 'qpm9-j523.org_neighborhood.txt.gz', '6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', 'nfkx-wd79.Address_1.txt.gz', 'jzdn-258f.Agency.txt.gz', 'kiv2-tbus.Vehicle_Color.txt.gz', 'w9ak-ipjd.Filing_Representative_First_Name.txt.gz', 'irhv-jqz7.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'm3fi-rt3k.Street_Address_1_.txt.gz', 'ipu4-2q9a.Owner_s_House_City.txt.gz', 'qpm9-j523.org_website.txt.gz', 'qgea-i56i.Lat_Lon.txt.gz', 'jvce-szsb.Website.txt.gz', 'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', 'kiyv-ks3f.phone.txt.gz', 'qe6k-pu9t.Agency.txt.gz', '5e7x-8jy6.School_Name.txt.gz', 'xne4-4v8f.SCHOOL.txt.gz', '7btz-mnc8.Provider_First_Name.txt.gz', 'uq7m-95z8.interest1.txt.gz', 'n5mv-nfpy.Location1.txt.gz', '8i43-kna8.CORE_SUBJECT.txt.gz', 'eccv-9dzr.Telephone_Number.txt.gz', '4n2j-ut8i.SCHOOL_LEVEL_.txt.gz', 'dm9a-ab7w.STREET_NAME.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', '2bnn-yakx.Vehicle_Color.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'jt7v-77mi.Vehicle_Color.txt.gz', 'bty7-2jhb.Owner_s_House_Zip_Code.txt.gz', 'cvh6-nmyi.SCHOOL_LEVEL_.txt.gz', '7yds-6i8e.CORE_SUBJECT__MS_CORE_and_9_12_ONLY_.txt.gz', 'ajxm-kzmj.NeighborhoodName.txt.gz', '3aka-ggej.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '6bgk-3dad.RESPONDENT_ZIP.txt.gz', 'fbaw-uq4e.Location_1.txt.gz', 'jxyc-rxiv.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'n2s5-fumm.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'bjuu-44hx.DVV_MAKE.txt.gz', 'uzcy-9puk.Street_Name.txt.gz', 's3k6-pzi2.interest1.txt.gz', 'wg9x-4ke6.Principal_phone_number.txt.gz', 'vhah-kvpj.Borough.txt.gz', 'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz', '3rfa-3xsf.Street_Name.txt.gz', 'urzf-q2g5.Phone_Number.txt.gz', 'him9-7gri.Agency.txt.gz', '3rfa-3xsf.Cross_Street_2.txt.gz', 'mu46-p9is.CallerZipCode.txt.gz', 'a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'ytjm-yias.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'sxmw-f24h.Park_Facility_Name.txt.gz', 'vuae-w6cg.Agency.txt.gz', 'qusa-igsv.BORO.txt.gz', '5tdj-xqd5.Borough.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', 't8hj-ruu2.Business_Phone_Number.txt.gz', 'ajgi-hpq9.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'jhjm-vsp8.Agency.txt.gz', '4nft-bihw.Property_Address.txt.gz', '6je4-4x7e.SCHOOL_LEVEL_.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz', 'dpm2-m9mq.owner_zip.txt.gz', 'gk83-aa6y.SCHOOL_NAME.txt.gz', 't8hj-ruu2.First_Name.txt.gz', 'as69-ew8f.StartCity.txt.gz', 'i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz', 'myei-c3fa.Neighborhood_1.txt.gz', 'upwt-zvh3.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.School_Phone_Number.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz', 'weg5-33pj.SCHOOL_LEVEL_.txt.gz', 'rmv8-86p4.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz']


# iterate on each filename


#all_filenames = ['jz4z-kudi.Violation_Location__City_.txt.gz', '2sps-j9st.PERSON_FIRST_NAME.txt.gz', 'w7w3-xahh.Address_City.txt.gz', '7crd-d9xh.website.txt.gz', 'erm2-nwe9.City.txt.gz']

# city preprocessing
us_cities_df = spark.read.load("/user/ts3813/uscities.csv",format="csv", delimiter=",", inferSchema="true", header="true")
nyc_cities_df = us_cities_df.filter(us_cities_df.state_id=='NY').select('city')

lower_udf = udf(lambda x: x.lower() if x else '', StringType())

udf_1 = udf(lambda x: 1, IntegerType())
nyc_cities_df = nyc_cities_df.withColumn('city', lower_udf('city'))
nyc_cities_df = nyc_cities_df.withColumn('city_bool', udf_1('city'))
wiki_colors_df = spark.read.load("/user/ts3813/wikipedia_color_names.csv",format="csv", delimiter=",", inferSchema="true", header="true")     
w_name = wiki_colors_df.select("Name")
w_name_lower = w_name.withColumn('name_lower', lower_udf('Name'))
w_name_lower = w_name_lower.withColumn('w_name_soundex', soundex('name_lower'))
w_soundex_vals = [w_soundex[0] for w_soundex in w_name_lower.select('w_name_soundex').collect()]

phoneNumRegex = re.compile(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')

school_levels= ['Preschool','pre-kindergarten','elementary school','middle school','senior school','junior high school','high school','middle school','K-8','K-1','K-2','K-3','K-4','K-5','K-6','K-7']
school_levels_df = spark.createDataFrame(school_levels, StringType())
school_levels_df = school_levels_df.withColumn('levels', lower_udf('value'))
school_levels_df = school_levels_df.withColumn('school_level_bool', udf_1('levels'))


boroughs=['the bronx','staten island','manhattan','brooklyn','queens','bronx', 'K','B','R','Q','M']
boroughs_df=spark.createDataFrame(boroughs,StringType())
boroughs_df = boroughs_df.withColumn('levels', lower_udf('value'))
boroughs_df = boroughs_df.withColumn('boroughs_bool', udf_1('levels'))

area_of_study=['engineering', 'teaching', 'communications', 'animal ccience', 'science & math', 'law & government', 'architecture', 'business', 'culinary arts', 'performing arts', 'health profession', 'visual art & design', 'film/video', 'cosmetology', 'humanities & interdisciplinary', 'computer science & technology', 'project-based learning', 'hospitality, travel, & tourism', 'performing arts/visual art & design', 'environmental science', 'zoned']
area_of_study_df=spark.createDataFrame(area_of_study,StringType())
area_of_study_df = area_of_study_df.withColumn('levels', lower_udf('value'))
area_of_study_df = area_of_study_df.withColumn('study_bool', udf_1('levels'))


subject_in_school=['algebra','art','biology', 'calculus','chemistry','cinema','composition','craft','drawing','economics','economy','english','arts','geography','geometry','gym','history', 'humanities','language','literature','math','mathematics','music','painting','physical development','physics','science','social science','social studies','statistics']
subject_in_school_df=spark.createDataFrame(subject_in_school,StringType())
subject_in_school_df = subject_in_school_df.withColumn('levels', lower_udf('value'))
subject_in_school_df = subject_in_school_df.withColumn('school_bool', udf_1('levels'))

university_df=spark.read.load("/user/ts3813/universities.csv",format="csv", delimiter=",", inferSchema="true", header="true")
university_df=university_df.select('Name')
university_df = university_df.withColumn('levels', lower_udf('Name'))
university_df = university_df.withColumn('university_bool', udf_1('levels'))

car_makes_df = spark.read.load("/user/ts3813/car_makes_csv.csv",format="csv", delimiter=",", inferSchema="true", header="false")
car_makes_df=car_makes_df.select('_c0')
car_makes_df = car_makes_df.withColumn('levels', lower_udf('_c0'))
car_makes_df = car_makes_df.withColumn('car_make_bool', udf_1('levels'))

parks_playgrounds_df=spark.read.load("/user/ts3813/OpenData_ParksProperties.csv",format="csv", delimiter=",", inferSchema="true", header="true")
parks_playgrounds_df=parks_playgrounds_df.select('Name311')
parks_playgrounds_df = parks_playgrounds_df.withColumn('levels', lower_udf('Name311'))
parks_playgrounds_df = parks_playgrounds_df.withColumn('parks_bool', udf_1('levels'))

loc_df = sc.textFile("/user/ts3813/Type_of_Location.txt")
loc_df=spark.createDataFrame(loc_df, StringType())
loc_df = loc_df.withColumn('levels', lower_udf('value'))
loc_df = loc_df.withColumn('loc_bool', udf_1('levels'))

agency_df = spark.read.load("/user/ts3813/us_agency.csv",format="csv", delimiter=",", inferSchema="true", header="true")
agency_df = agency_df.withColumn('levels', lower_udf('AGENCY NAME'))
agency_df = agency_df.withColumn('agency_bool', udf_1('levels'))

school_df = spark.read.load("/user/ts3813/schoolnames.csv",format="csv", delimiter=",", inferSchema="true", header="true")
school_df = school_df.withColumn('levels', lower_udf('LOCATION_NAME'))
school_df = school_df.withColumn('school_bool', udf_1('levels'))

building_classification_df = spark.read.load("/user/nap493/Building_Classification_list.csv", format="csv", inferSchema="true")
building_classification_df = building_classification_df.withColumn('levels', lower_udf('_c0'))
building_classification_df = building_classification_df.withColumn('building_classification_bool', udf_1('levels'))

neighbourhood_df = spark.read.load("/user/nap493/neighbourhood_list.csv", format="csv", inferSchema="true")
neighbourhood_df = neighbourhood_df.withColumn('levels', lower_udf('_c0'))
neighbourhood_df = neighbourhood_df.withColumn('neighbourhood_bool', udf_1('levels'))

vehicle_type_df = spark.read.load("/user/nap493/vehicle_type_list.csv", format="csv", inferSchema="true")
vehicle_type_df = vehicle_type_df.withColumn('levels', lower_udf('_c0'))
vehicle_type_df = vehicle_type_df.withColumn('vehicle_type_bool', udf_1('levels'))

def PersonStats(rdd):
	total_data = stanford_ner_tagger.tag(rdd.map(lambda x: re.sub('\s','',x)).take(500))
	#print(total_data)
	count_persons =len(list(filter(lambda x: x[1]=='PERSON', total_data)))
	count_persons= rdd.count()*(count_persons/500)
	return {'semantic_type': 'person_name', 'count':count_persons}


def OrganisationStats(rdd):
	total_data = stanford_ner_tagger.tag(rdd.map(lambda x: re.sub('\s','',x)).take(500))
	#print(total_data)
	count_business =len(list(filter(lambda x: x[1]=='ORGANISATION', total_data)))
	count_business= rdd.count()*(count_business/500)
	return {'semantic_type': 'business_name', 'count':count_business}



def WebsiteStats(rdd):
	websiteRegex = re.compile(r'^((ftp|http|https):\/\/)?(www.)?(?!.*(ftp|http|https|www.))[a-zA-Z0-9_-]+(\.[a-zA-Z]+)+((\/)[\w#]+)*(\/\w+\?[a-zA-Z0-9_]+=\w+(&[a-zA-Z0-9_]+=\w+)*)?$')
	result = rdd.map(lambda x:	True	if	websiteRegex.match(x.lower())!=None	else	False).filter(lambda	x:	x==True)
	total_count=rdd.count()
	website_count= result.count()
	return {'semantic_type': 'website', 'count': website_count}

def CityStats(rdd):
	output_df = spark.createDataFrame(rdd, StringType())
	output_df = output_df.withColumn('value', lower_udf('value'))
	total_count = output_df.rdd.count()
	output_df = output_df.join(nyc_cities_df, output_df['value'] == nyc_cities_df['city'], how='left')
	result_count = output_df.rdd.filter(lambda x: x[2]==1).count()
	return {'semantic_type': 'city', 'count': result_count}

def PhoneStats(rdd):
	phoneNumRegex = re.compile(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
	result = rdd.map(lambda x:      True    if      phoneNumRegex.match(x.lower())!=None     else    False).filter(lambda       x:      x==True)
	total_count=rdd.count()
	phone_count= result.count()
	return {'semantic_type': 'phone_number', 'count': phone_count}

def LatLongStats(rdd):
	latLongRegex = re.compile(r'(^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$)')
	rdd = rdd.map(lambda f: f.replace("(","").replace(")",""))
	result = rdd.map(lambda x:      True    if      latLongRegex.match(x.lower())!=None     else    False).filter(lambda       x:      x==True)
	total_count=rdd.count()
	latlong_count= result.count()
	return {'semantic_type': 'lat_lon_cord', 'count': latlong_count}

def ZipCode(rdd):
	ZipCodeRegex = re.compile(r'(^[0-9]{5}(?:-[0-9]{4})?$)')
	result=rdd.map(lambda x: True	if zipCodeRegex.match(x)!=None	else	False).filter(lambda	x:	x==True)
	total_count=rdd.count()
	zipcode_count=result.count()
	return {'semantic_type':'zip_code','count':zipcode_count}

def soundex_lookup(x):
    for soundex_val in w_soundex_vals:
        if x==soundex_val:
            return True
    return False

def ColorStats(rdd):
	total_count = rdd.count()

	analyse_color_df = spark.createDataFrame(rdd, StringType())
	a_name=analyse_color_df.select("value")
	soundex_udf = udf(lambda x: soundex_lookup(x) , StringType())
	analyse_color_df = analyse_color_df.withColumn('color_lower', lower_udf('value')).withColumn('color_soundex', soundex('color_lower'))
	analyse_color_df = analyse_color_df.withColumn('isColor', soundex_udf('color_soundex'))
	color_count = analyse_color_df.rdd.filter(lambda x: x[3]=='true').count()
	return {'semantic_type': 'color', 'count': color_count}

def SchoolLevelStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(school_levels_df, output_df['value'] == school_levels_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'school_level', 'count': result_count}
	
def BoroughStats(rdd):
        output_df=spark.createDataFrame(rdd, StringType())
        output_df=output_df.withColumn('value',lower_udf('value'))
        total_count=output_df.rdd.count()
        output_df=output_df.join(boroughs_df, output_df['value'] == boroughs_df['levels'], how='left')
        result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
        return {'semantic_type': 'borough', 'count': result_count}

		 
def UniversitiesStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(university_df, output_df['value'] == university_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'college_name', 'count': result_count}

def BuildingClassificationStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))	
	total_count=output_df.rdd.count()
	output_df=output_df.join(building_classification_df, output_df['value'] == building_classification_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[2]==1).count()
	return {'semantic_type': 'building_classification', 'count': result_count}

def CarMakesStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(car_makes_df, output_df['value'] == car_makes_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'car_make', 'count': result_count}


def ParksStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(parks_playgrounds_df, output_df['value'] == parks_playgrounds_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'park_playground', 'count': result_count}


def TypeoflocationStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(loc_df, output_df['value'] == loc_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'location_type', 'count': result_count}

def AgencyStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(agency_df, output_df['value'] == agency_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'city_agency', 'count': result_count}


def SchoolStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(school_df, output_df['value'] == school_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'school_name', 'count': result_count}

def StreetStats(rdd):
	streetRegex = re.compile(r'(avenue|lane|road|boulevard|drive|street|ave|dr|rd|blvd|ln|st)')
	result = rdd.map(lambda x:      True    if      streetRegex.match(x.lower())!=None     else    False).filter(lambda       x:      x==True)
	total_count=rdd.count()
	street_count= result.count()
	return {'semantic_type': 'street_name', 'count': street_count}

def AreaOfStudiesStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(area_of_study_df, output_df['value'] == area_of_study_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'area_of_study', 'count': result_count}

def SubjectInSchoolStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))
	total_count=output_df.rdd.count()
	output_df=output_df.join(subject_in_school_df, output_df['value'] == subject_in_school_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[3]==1).count()
	return {'semantic_type': 'subject_in_school', 'count': result_count}


def VehicleTypeStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))	
	total_count=output_df.rdd.count()
	output_df=output_df.join(vehicle_type_df, output_df['value'] == vehicle_type_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[2]==1).count()
	return {'semantic_type': 'vehicle_type', 'count': result_count}

def NeighbourhoodStats(rdd):
	output_df=spark.createDataFrame(rdd, StringType())
	output_df=output_df.withColumn('value',lower_udf('value'))	
	total_count=output_df.rdd.count()
	output_df=output_df.join(neighbourhood_df, output_df['value'] == neighbourhood_df['levels'], how='left')
	result_count=output_df.rdd.filter(lambda x: x[2]==1).count()
	return {'semantic_type': 'neighbourhood', 'count': result_count}


for filename in all_filenames:
	print('Processing file: {}'.format(filename))
	final_json = [{'predicted_types': ['person_name', 'business_name', 'phone_number', 'address', 'street_name', 'city', 'neighborhood', 'lat_lon_cord', 'zip_code', 'borough', 'school_name', 
'color', 'car_make', 'city_agency', 'area_of_study', 'subject_in_school', 'school_level', 'college_name', 'website', 'building_classification', 'vehicle_type', 'location_type', 'park_playground', 'other']}]
	json_file_data = {}
	
	input_rdd = sc.textFile(os.path.join('/user/hm74/NYCColumns',filename))
	input_rdd = input_rdd.map(lambda x: x.split('\t')[0])
	
	json_file_data["column_name"] = filename.split('.')[1]
	json_file_data['semantic_types'] = []
		
	#Phone stats
	phone_stats = PhoneStats(input_rdd)
	if len(phone_stats)>0:
		json_file_data['semantic_types'].append(phone_stats)


	
	# person stats
	person_stats = PersonStats(input_rdd)
	if len(person_stats)>0:
		json_file_data['semantic_types'].append(person_stats)

	# website stats
	website_stats = WebsiteStats(input_rdd)
	if len(website_stats)>0:
		json_file_data['semantic_types'].append(website_stats)

	# city stats
	city_stats = CityStats(input_rdd)
	if len(city_stats)>0:
		json_file_data['semantic_types'].append(city_stats)

	# phone number
	phone_stats = PhoneStats(input_rdd)
	if len(phone_stats)>0:
		json_file_data['semantic_types'].append(phone_stats)

	# latlong
	latlong_stats = LatLongStats(input_rdd)
	if len(latlong_stats)>0:
		json_file_data['semantic_types'].append(latlong_stats)

	# color
	if 'color' in filename.lower():
		color_stats = ColorStats(input_rdd)
		if len(color_stats)>0:
			json_file_data['semantic_types'].append(color_stats)
	

	school_level_stats = SchoolLevelStats(input_rdd)
	if len(school_level_stats)>0:
		json_file_data['semantic_types'].append(school_level_stats)
		
	#Boroughs
	borough_stats = BoroughStats(input_rdd)
	if len(borough_stats)>0:
		json_file_data['semantic_types'].append(borough_stats) 
		
	#Univeristy
	university_stats = UniversitiesStats(input_rdd)
	if len(university_stats)>0:
		json_file_data['semantic_types'].append(university_stats)

	#Car Makes
	car_makes_stats = CarMakesStats(input_rdd)
	if len(car_makes_stats)>0:
		json_file_data['semantic_types'].append(car_makes_stats)

	#Park Stats
	park_stats = ParksStats(input_rdd)
	if len(park_stats)>0:
		json_file_data['semantic_types'].append(park_stats)
	
	#Type pf location
	location_stats = TypeoflocationStats(input_rdd)
	if len(location_stats)>0:
		json_file_data['semantic_types'].append(location_stats)	
	
	#Business Name
	business_stats = OrganisationStats(input_rdd)
	if len(business_stats)>0:
		json_file_data['semantic_types'].append(business_stats)

	#Agency Name
	agency_stats = AgencyStats(input_rdd)
	if len(agency_stats)>0:
		json_file_data['semantic_types'].append(agency_stats)

	#school Names
	school_stats = SchoolStats(input_rdd)
	if len(school_stats)>0:
		 json_file_data['semantic_types'].append(school_stats)

	#Street Names
	street_stats = StreetStats(input_rdd)
	if len(street_stats)>0:
		json_file_data['semantic_types'].append(street_stats)
	
	#Area of study
	study_stats = AreaOfStudiesStats(input_rdd)
	if len(study_stats)>0:
		json_file_data['semantic_types'].append(study_stats)

	#Subject In school
	subject_stats = SubjectInSchoolStats(input_rdd)
	if len(subject_stats)>0:
		json_file_data['semantic_types'].append(subject_stats)

	#Vehicle Type
	vehicle_type_stats = VehicleTypeStats(input_rdd)
	if len(vehicle_type_stats)>0:
		json_file_data['semantic_types'].append(vehicle_type_stats)

	#Neighbourhood
	neighbourhood_stats = NeighbourhoodStats(input_rdd)
	if len(neighbourhood_stats)>0:
		json_file_data['semantic_types'].append(neighbourhood_stats)
	
	#Building Classification
	building_classification_stats = BuildingClassificationStats(input_rdd)
	if len(building_classification_stats)>0:
		json_file_data['semantic_types'].append(building_classification_stats)

	final_json.append(json_file_data)
	with open('output/{}.json'.format(filename), 'w') as f:
		f.write(json.dumps(final_json, indent=4, separators=(',',':')))


