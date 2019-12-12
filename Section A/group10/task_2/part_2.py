import os
import re
import json
import subprocess

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, RegexTokenizer, NGram
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
import time


def main():
    db_list = ['5694-9szk.Business_Website_or_Other_URL.txt.gz', 'uwyv-629c.StreetName.txt.gz', 'faiq-9dfq.Vehicle_Color.txt.gz',
     'qcdj-rwhu.BUSINESS_NAME2.txt.gz', '6ypq-ih9a.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     'pvqr-7yc4.Vehicle_Color.txt.gz', 'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
     'uq7m-95z8.interest6.txt.gz', '5ziv-wcy4.WEBSITE.txt.gz', 'ydkf-mpxb.CrossStreetName.txt.gz',
     'w9ak-ipjd.Applicant_Last_Name.txt.gz', 'jz4z-kudi.Respondent_Address__City_.txt.gz',
     'rbx6-tga4.Owner_Street_Address.txt.gz', 'sqmu-2ixd.Agency_Name.txt.gz', 'aiww-p3af.Incident_Zip.txt.gz',
     'mmvm-mvi3.Org_Name.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz', 'uh2w-zjsn.Borough.txt.gz',
     'tqtj-sjs8.FromStreetName.txt.gz', 'mqdy-gu73.Color.txt.gz', '7jkp-5w5g.Agency.txt.gz',
     's3zn-tf7c.QUEENS_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'sqcr-6mww.School_Name.txt.gz',
     'vrn4-2abs.SCHOOL_LEVEL_.txt.gz', '2sps-j9st.PERSON_LAST_NAME.txt.gz', '2bmr-jdsv.DBA.txt.gz',
     '4d7f-74pe.Address.txt.gz', 'ji82-xba5.address.txt.gz', 'hy4q-igkk.School_Name.txt.gz', 's9d3-x4fz.EMPCITY.txt.gz',
     '5uac-w243.PREM_TYP_DESC.txt.gz', '64gx-bycn.EMPCITY.txt.gz', 'e9xc-u3ds.CANDMI.txt.gz',
     'h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz', 'p937-wjvj.HOUSE_NUMBER.txt.gz', 'dj4e-3xrn.SCHOOL_LEVEL_.txt.gz',
     'qu8g-sxqf.MI.txt.gz', 'mdcw-n682.Middle_Initial.txt.gz', 'pq5i-thsu.DVC_MAKE.txt.gz',
     'ub9e-s7ai.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '52dp-yji6.Owner_First_Name.txt.gz',
     'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz', 'vx8i-nprf.MI.txt.gz', 'k3cd-yu9d.Location_1.txt.gz',
     'p6h4-mpyy.PRINCIPAL_PHONE_NUMBER.txt.gz', 'sybh-s59s.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz',
     'kz72-dump.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '7btz-mnc8.Provider_Last_Name.txt.gz',
     'ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz', 'mjux-q9d4.SCHOOL_LEVEL_.txt.gz', 'hjvj-jfc9.Borough.txt.gz',
     'h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz', 'easq-ubfe.CITY.txt.gz', 'sv2w-rv3k.BORO.txt.gz',
     'qu8g-sxqf.First_Name.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_First_Name.txt.gz',
     'ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz', 'pgtq-ht5f.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz',
     '52dp-yji6.Owner_Last_Name.txt.gz', 's3k6-pzi2.interest4.txt.gz', '4y63-yw9e.SCHOOL_NAME.txt.gz',
     'gez6-674h.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'a9md-ynri.MI.txt.gz',
     'u553-m549.Independent_Website.txt.gz', 'uzcy-9puk.Street_Name.txt.gz', 'dg92-zbpx.VendorAddress.txt.gz',
     'jcih-dj9q.QUEENS_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', '735p-zed8.CANDMI.txt.gz',
     'vg63-xw6u.CITY.txt.gz', 'aiww-p3af.Cross_Street_1.txt.gz', 'sa5w-dn2t.Agency.txt.gz', 'cspg-yi7g.ADDRESS.txt.gz',
     'crbs-vur7.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'erm2-nwe9.City.txt.gz',
     'nyis-y4yr.Owner_s__Phone__.txt.gz', 'tukx-dsca.Address_1.txt.gz', '9b9u-8989.DBA.txt.gz',
     'e4p3-6ecr.Agency_Name.txt.gz', '5mw2-hzqx.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
     'kiv2-tbus.Vehicle_Make.txt.gz', 'w6yt-hctp.COMPARABLE_RENTAL_1__Building_Classification.txt.gz',
     'k3cd-yu9d.CANDMI.txt.gz', 'ii2w-6fne.Borough.txt.gz', 'w7w3-xahh.Location.txt.gz',
     'erm2-nwe9.Park_Facility_Name.txt.gz', '5nz7-hh6t.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     '8wbx-tsch.Website.txt.gz', 'xne4-4v8f.SCHOOL_LEVEL_.txt.gz', 'vw9i-7mzq.neighborhood.txt.gz',
     'yayv-apxh.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.Park_Facility_Name.txt.gz',
     'jz4z-kudi.Violation_Location__City_.txt.gz', 'kiv2-tbus.Vehicle_Body_Type.txt.gz',
     'fzv4-jan3.SCHOOL_LEVEL_.txt.gz', 'w7w3-xahh.Address_ZIP.txt.gz', 'i9pf-sj7c.INTEREST.txt.gz',
     'ci93-uc8s.ZIP.txt.gz', 'jtus-srrj.School_Name.txt.gz', 'a5td-mswe.Vehicle_Color.txt.gz',
     '29bw-z7pj.Location_1.txt.gz', 'vw9i-7mzq.interest4.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz',
     '3rfa-3xsf.Incident_Zip.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz',
     'kj4p-ruqc.StreetName.txt.gz', '4pt5-3vv4.Location.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz',
     'pqg4-dm6b.Address1.txt.gz', 'cqc8-am9x.Borough.txt.gz', '6rrm-vxj9.parkname.txt.gz',
     'tg4x-b46p.ZipCode_s_.txt.gz', 'jzt2-2f7h.School_Name.txt.gz', 'ci93-uc8s.Website.txt.gz',
     'm59i-mqex.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'uzcy-9puk.School_Phone_Number.txt.gz',
     'ci93-uc8s.Vendor_DBA.txt.gz', 'cyfw-hfqk.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
     '3rfa-3xsf.Intersection_Street_2.txt.gz', 'uqxv-h2se.neighborhood.txt.gz', 'w9ak-ipjd.Owner_s_Street_Name.txt.gz',
     'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz', '4twk-9yq2.CrossStreet2.txt.gz', 'fbaw-uq4e.CITY.txt.gz',
     'mdcw-n682.First_Name.txt.gz', 'w7w3-xahh.Address_City.txt.gz', 'i4ni-6qin.PRINCIPAL_PHONE_NUMBER.txt.gz',
     'imfa-v5pv.School_Name.txt.gz', 'sxx4-xhzg.Park_Site_Name.txt.gz', 'vw9i-7mzq.interest1.txt.gz',
     'sqcr-6mww.Cross_Street_1.txt.gz', '6anw-twe4.FirstName.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz',
     'uzcy-9puk.Park_Facility_Name.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', 'c284-tqph.Vehicle_Color.txt.gz',
     'm56g-jpua.COMPARABLE_RENTAL___1___Building_Classification.txt.gz', 'tsak-vtv3.Upcoming_Project_Name.txt.gz',
     'tg3t-nh4h.BusinessName.txt.gz', 'cgz5-877h.SCHOOL_LEVEL_.txt.gz',
     'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz', 'us4j-b5zt.Agency.txt.gz', 'vr8p-8shw.DVT_MAKE.txt.gz',
     '3qfc-4tta.BRONX_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz',
     'bawj-6bgn.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'ci93-uc8s.fax.txt.gz',
     'ffnc-f3aa.SCHOOL_LEVEL_.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz', 'rbx6-tga4.Owner_Street_Address.txt.gz',
     's3k6-pzi2.interest5.txt.gz', '2sps-j9st.PERSON_FIRST_NAME.txt.gz', 'ji82-xba5.street.txt.gz',
     'f7qh-bcr5.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', '3rfa-3xsf.Street_Name.txt.gz',
     'n84m-kx4j.VEHICLE_MAKE.txt.gz', 'hy4q-igkk.Location.txt.gz', 'sxmw-f24h.Cross_Street_2.txt.gz',
     'yahh-6yjc.School_Type.txt.gz', '72ss-25qh.Agency_ID.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz',
     'm56g-jpua.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     '3rfa-3xsf.School_Name.txt.gz', 'ic3t-wcy2.Applicant_s_First_Name.txt.gz', 'vw9i-7mzq.interest3.txt.gz',
     'i6b5-j7bu.TOSTREETNAME.txt.gz', 'i5ef-jxv3.Agency.txt.gz', '7crd-d9xh.website.txt.gz',
     'mdcw-n682.Last_Name.txt.gz', 'ge8j-uqbf.interest.txt.gz', 'q2ni-ztsb.Street_Address_1.txt.gz',
     '8k4x-9mp5.Last_Name__only_2014_15_.txt.gz', 'wks3-66bn.School_Name.txt.gz', '43nn-pn8j.DBA.txt.gz',
     'qgea-i56i.PREM_TYP_DESC.txt.gz', 'bdjm-n7q4.CrossStreet2.txt.gz', 'nhms-9u6g.Name__Last__First_.txt.gz',
     'bdjm-n7q4.Location.txt.gz', 'x3kb-2vbv.School_Name.txt.gz', 'uzcy-9puk.Location.txt.gz',
     '6anw-twe4.LastName.txt.gz',
     'tyfh-9h2y.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     '3rfa-3xsf.Cross_Street_2.txt.gz', 'bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz',
     '9jgj-bmct.Incident_Address_Street_Name.txt.gz', 'pdpg-nn8i.BORO.txt.gz', 'w9ak-ipjd.Owner_s_Business_Name.txt.gz',
     'rb2h-bgai.Website.txt.gz', 'jt7v-77mi.Vehicle_Make.txt.gz', 'as69-ew8f.TruckMake.txt.gz',
     'mrxb-9w9v.BOROUGH___COMMUNITY.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz',
     'dm9a-ab7w.AUTH_REP_LAST_NAME.txt.gz', '9z9b-6hvk.Borough.txt.gz',
     'wv4q-e75v.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'kwmq-dbub.CANDMI.txt.gz',
     'dvzp-h4k9.COMPARABLE_RENTAL_____1_____Building_Classification.txt.gz', '6ypq-ih9a.BOROUGH.txt.gz',
     'p2d7-vcsb.ACCOUNT_CITY.txt.gz', '2v9c-2k7f.DBA.txt.gz', 'erm2-nwe9.Landmark.txt.gz',
     'dm9a-ab7w.APPLICANT_FIRST_NAME.txt.gz', '72ss-25qh.Borough.txt.gz', 'qpm9-j523.org_neighborhood.txt.gz',
     '6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', 'nfkx-wd79.Address_1.txt.gz', 'jzdn-258f.Agency.txt.gz',
     'kiv2-tbus.Vehicle_Color.txt.gz', 'w9ak-ipjd.Filing_Representative_First_Name.txt.gz',
     'irhv-jqz7.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     'm3fi-rt3k.Street_Address_1_.txt.gz', 'ipu4-2q9a.Owner_s_House_City.txt.gz', 'qpm9-j523.org_website.txt.gz',
     'qgea-i56i.Lat_Lon.txt.gz', 'jvce-szsb.Website.txt.gz', 'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz',
     'kiyv-ks3f.phone.txt.gz', 'qe6k-pu9t.Agency.txt.gz', '5e7x-8jy6.School_Name.txt.gz', 'xne4-4v8f.SCHOOL.txt.gz',
     '7btz-mnc8.Provider_First_Name.txt.gz', 'uq7m-95z8.interest1.txt.gz', 'n5mv-nfpy.Location1.txt.gz',
     '8i43-kna8.CORE_SUBJECT.txt.gz', 'eccv-9dzr.Telephone_Number.txt.gz', '4n2j-ut8i.SCHOOL_LEVEL_.txt.gz',
     'dm9a-ab7w.STREET_NAME.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', '2bnn-yakx.Vehicle_Color.txt.gz',
     '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'jt7v-77mi.Vehicle_Color.txt.gz', 'bty7-2jhb.Owner_s_House_Zip_Code.txt.gz',
     'cvh6-nmyi.SCHOOL_LEVEL_.txt.gz', '7yds-6i8e.CORE_SUBJECT__MS_CORE_and_9_12_ONLY_.txt.gz',
     'ajxm-kzmj.NeighborhoodName.txt.gz', '3aka-ggej.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     '6bgk-3dad.RESPONDENT_ZIP.txt.gz', 'fbaw-uq4e.Location_1.txt.gz',
     'jxyc-rxiv.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     'n2s5-fumm.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'bjuu-44hx.DVV_MAKE.txt.gz',
     'uzcy-9puk.Street_Name.txt.gz', 's3k6-pzi2.interest1.txt.gz', 'wg9x-4ke6.Principal_phone_number.txt.gz',
     'vhah-kvpj.Borough.txt.gz', 'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz', '3rfa-3xsf.Street_Name.txt.gz',
     'urzf-q2g5.Phone_Number.txt.gz', 'him9-7gri.Agency.txt.gz', '3rfa-3xsf.Cross_Street_2.txt.gz',
     'mu46-p9is.CallerZipCode.txt.gz', 'a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
     'ytjm-yias.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'sxmw-f24h.Park_Facility_Name.txt.gz',
     'vuae-w6cg.Agency.txt.gz', 'qusa-igsv.BORO.txt.gz', '5tdj-xqd5.Borough.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz',
     't8hj-ruu2.Business_Phone_Number.txt.gz', 'ajgi-hpq9.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     'jhjm-vsp8.Agency.txt.gz', '4nft-bihw.Property_Address.txt.gz', '6je4-4x7e.SCHOOL_LEVEL_.txt.gz',
     'c284-tqph.Vehicle_Make.txt.gz', 'dpm2-m9mq.owner_zip.txt.gz', 'gk83-aa6y.SCHOOL_NAME.txt.gz',
     't8hj-ruu2.First_Name.txt.gz', 'as69-ew8f.StartCity.txt.gz', 'i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz',
     'myei-c3fa.Neighborhood_1.txt.gz', 'upwt-zvh3.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.School_Phone_Number.txt.gz',
     'kiv2-tbus.Vehicle_Make.txt.gz', 'weg5-33pj.SCHOOL_LEVEL_.txt.gz',
     'rmv8-86p4.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz']

    # sort files according to size
    # directory = os.path.join(os.sep, "user", "hm74", "NYCColumns")
    # file_paths = os.path.join(directory, db_list[0])
    # for file in db_list[1:]:
    #     location = os.path.join(directory, file)
    #     file_paths = file_paths + ' ' + location
    #
    # cmd = 'hadoop fs -du -s ' + file_paths
    # file_sizes = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
    # pairs = [(int(x.split()[0]), x.split()[-1]) for x in file_sizes]
    # pairs.sort(key=lambda s: s[0])
    # db_list_sorted = [s[1][22:] for s in pairs]

    # strategy pattern usually you would have a class then extend and apply, but this will do.
    functions = [count_website, count_zip_code, count_phone_number, count_lat_lon, count_borough, count_vehicle_type,
                 count_car_make, count_building_code, count_location_type, count_city_agency_abbrev, count_city_agency,
                 count_color, count_subject, count_area_study, count_school_level, count_address_street_name,
                 count_school_name, count_college_name, count_parks, count_business, count_neighborhood,
                 count_person_name, count_other]
    tokenizer = Tokenizer(inputCol="_c0", outputCol="token_raw")
    remover = StopWordsRemover(inputCol="token_raw", outputCol="token_filtered")
    regex_tokenizer = RegexTokenizer(inputCol="_c0", outputCol="letters", pattern="")
    regex_tokenizer_pad = RegexTokenizer(inputCol="_c0_pad", outputCol="letters_pad", pattern="")
    ngram = NGram(n=3, inputCol="letters", outputCol="ngrams")
    ngram_pad = NGram(n=3, inputCol="letters_pad", outputCol="ngrams_pad")
    pipeline = [tokenizer, remover, regex_tokenizer, regex_tokenizer_pad, ngram, ngram_pad]
    for file in db_list:
        if os.path.exists("task2_json/{}.json".format(file)):
            print('{} is already processed'.format(file))
            continue
        try:
            processed_path = os.path.join(os.sep, "user", "hm74", "NYCColumns", file)
            df_to_process = spark.read.option("delimiter", "\t").csv(processed_path)

            df_to_process = df_to_process.withColumn("id", F.monotonically_increasing_id())
            pad_udf = F.udf(pad_custom)
            df_to_process = df_to_process.withColumn('_c0_pad', pad_udf("_c0"))
            for stage in pipeline:
                df_to_process = stage.transform(df_to_process)
            start = time.time()
            for i, function in enumerate(functions):
                print("processing file {} function {}".format(file, function))
                sem_name, df_to_process, df_processed = function(df_to_process)
                if i == 8:
                    # oh look another thing nyu hpc infra can't do properly, looks like they didn't set up localcheckpointing.
                    # when doing itarative operations, i.e. applying functions to check rows, on joins you get degredation.
                    # One way to solve it is localcheckpointing, it will disguard the history of the transformation.
                    # df_to_process.rdd.localCheckpoint()
                    print("writing in and out")
                    df_to_process.write.mode("overwrite").parquet("process_{}.parquet".format(i))
                    df_to_process = spark.read.parquet("process_{}.parquet".format(i))
                    print("finished writing in")
                if i == 16:
                    print("writing in and out")
                    df_to_process.write.mode("overwrite").parquet("process_{}.parquet".format(i))
                    df_to_process = spark.read.parquet("process_{}.parquet".format(i))
                    print("finished writing in")
                if i == 0:
                    df_full = df_processed
                else:
                    df_full = df_full.union(df_processed)
                print("sem name {}".format(sem_name))
                # print("sem_name {}, count {}, count left {}".format(sem_name, df_processed.count(), df_to_process.count()))

            df_complete = df_full.dropna().groupBy("sem_type").agg({"sum(_c1)": "sum"})
            json_type = json.dumps(convert_df_to_dict(file, df_complete))
            with open("task2_json/{}.json".format(file), 'w+', encoding="utf-8") as f:
                f.write(json_type)
            print("process time {}".format(time.time()-start))
        except Exception as e:
            print("unable to process because {}".format(e))


def convert_df_to_dict(file, df):
    list_info = df.collect()
    # print(list_info)
    column_dict = {}
    column_dict['column_name'] = file
    column_dict['semantic_types'] = []
    for i in range(len(list_info)):
        type_dict = {}
        type_dict['semantic_type'] = list_info[i][0]
        type_dict['count'] = int(list_info[i][1])
        column_dict['semantic_types'].append(type_dict)
    # print(column_dict)
    return column_dict


def lat_lon_regex(val):
    regex = re.compile(r"^\s*[(]([0-9]|[1-8][0-9]|90)[.][0-9]+,\s-*([0-9]|[1-9][0-9]|1[0-7][0-9]|180)[.][0-9]+[)]\s*$")
    return re.match(regex, val) is not None


def phone_number_regex(val):
    regex = re.compile(r"^\s*[0-9]{10}\s*$|"
                       r"^\s*([0-9]{3}|[0-9]{4}|[(][0-9]{3}[)]|[0-9](-*|\s*)[0-9]{3})(-*|\s*)[0-9]{3}(-*|\s*)[0-9]{4}\s*$")
    return re.match(regex, val) is not None


def zip_code_regex(val):
    regex = re.compile(r"^\s*[0-9]{5}\s*$|"
                       r"^\s*[0-9]{9}\s*$|"
                       r"^\s*[0-9]{5}-[0-9]{4}\s*$")
    return re.match(regex, val) is not None


def address_regex(val):
    regex = re.compile(r"^(-|[0-9])+\s")
    if re.match(regex, val) is not None:
        return "address"
    else:
        return "street_name"


def person_name_regex(val):
    s = val.replace(" ", "")
    s = s.strip(".")
    regex = re.compile(r"^[a-zA-Z][a-zA-Z]+$")
    return re.match(regex, s) is not None


def pre_compute_city():
    df_city = spark.read.format("csv").load("/user/jz3350/ny_cities.csv")
    df_city = df_city.withColumnRenamed("City", "_c0")
    return df_city


def count_city(df_to_process):
    df_processed = df_to_process.join(df_pre_city, F.levenshtein(F.lower(df_to_process._c0), F.lower(df_pre_city._c0)) < 3)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'city', df_left, df_processed.select(F.sum("_c1"), F.lit('city').alias("sem_type"))


def count_lat_lon(df_to_process):
    udf_lat_lon_regex = F.udf(lat_lon_regex)
    df_processed = df_to_process.filter(udf_lat_lon_regex(df_to_process._c0) == True)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'lat_lon_cord', df_left, df_processed.select(F.sum("_c1"), F.lit('lat_lon_cord').alias("sem_type"))


def count_phone_number(df_to_process):
    udf_phone_number_regex = F.udf(phone_number_regex)
    df_processed = df_to_process.filter(udf_phone_number_regex(df_to_process._c0) == True)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'phone_number', df_left, df_processed.select(F.sum("_c1"), F.lit('phone_number').alias("sem_type"))


def count_zip_code(df_to_process):
    udf_zip_code_regex = F.udf(zip_code_regex)
    df_processed = df_to_process.filter(udf_zip_code_regex(df_to_process._c0) == True)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'zip_code', df_left, df_processed.select(F.sum("_c1"), F.lit('zip_code').alias("sem_type"))


def count_borough(df_to_process):
    borough_list = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', 'K', 'M', 'Q', 'R', 'X']
    df_borough = spark.createDataFrame(borough_list, StringType())
    df_to_processd = df_to_process.withColumn("upper", F.upper(df_to_process._c0))
    df_join = df_to_processd.join(df_borough, df_to_processd.upper == df_borough.value, "outer")
    df_processed = df_join.filter(df_borough.value.isNotNull())
    df_left = df_join.filter(df_borough.value.isNull()).drop("upper").drop("value")
    return 'borough', df_left, df_processed.select(F.sum("_c1"), F.lit('borough').alias("sem_type"))


def pre_compute_street():
    list_street = ["street", "avenue", "road", "parkway", "broadway", "plaza", "court", "place",
                   "boulevard", "terrace", "highway", "st", "drive", "concourse", "ave" ,
                   "lane", "walk", "way", "rd", "square", "center", "pl", "blvd",
                   "terr", "ct", "dr", "beach", "avenu", "aven", "boule", "boulev", "bouleva", "boul",
                   "sq", "slip", "pkway"]
    df_street_types = spark.createDataFrame(list_street, StringType()).select(
        F.collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    return df_street_types


def count_address_street_name(df_to_process):
    udf_address_regex = F.udf(address_regex)
    df_cross_join = df_to_process.crossJoin(df_pre_street)
    df_processed = df_cross_join.withColumn("size", F.size(F.array_intersect("token_filtered", "to_match")))
    df_street = df_processed.filter(df_processed.size != 0).withColumn("sem_type", F.lit(udf_address_regex(df_processed._c0)))
    df_left = df_processed.filter(df_processed.size == 0).drop("to_match").drop("size")
    return "address_street_name", df_left, df_street.groupBy('sem_type').agg({'_c1': 'sum'}).select('sum(_c1)', 'sem_type')


def pre_compute_neighborhood():
    list_neighborhood = ["center", "village", "central", "bay", "west", "east", "north", "south",
                         "upper", "lower", "side", "town", "cbd", "hill", "heights", "soho", "valley"
                         , "acres", "ridge", "harbor", "beach", "island", "club",
                         "ferry", "mall", "oaks", "point", "hts", "neck", "yard", "basin"
                         "slope", "hook"]
    df_neighborhood_types = spark.createDataFrame(list_neighborhood, StringType()).select(
        F.collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    return df_neighborhood_types


def count_neighborhood(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_neighborhood)
    df_processed = df_cross_join.withColumn("size", F.size(F.array_intersect("token_filtered", "to_match")))
    df_street = df_processed.filter(df_processed.size != 0)
    df_left = df_processed.filter(df_processed.size == 0).drop("to_match").drop("size")
    return "neighborhood", df_left, df_street.select(F.sum("_c1"), F.lit('neighborhood').alias("sem_type"))


def count_person_name(df_to_process):
    udf_person_name_regex = F.udf(person_name_regex)
    df_processed = df_to_process.filter(udf_person_name_regex(df_to_process._c0) == True)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'person_name', df_left, df_processed.select(F.sum("_c1"), F.lit('person_name').alias("sem_type"))


def website_regex(val):
    # https://stackoverflow.com/questions/7160737/python-how-to-validate-a-url-in-python-malformed-or-not
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, val) is not None


def count_other(df_to_process):
    return "other", df_to_process, df_to_process.select(F.sum("_c1"), F.lit("other").alias("sem_type"))


def count_website(df_to_process):
    udf_website_regex = F.udf(website_regex)
    df_to_process2 = df_to_process.withColumn("_c0_trim", F.regexp_replace(F.col("_c0"), "\\s+", ""))
    df_processed = df_to_process2.filter(udf_website_regex(df_to_process2._c0_trim) == True)
    df_left = df_to_process2.filter(F.col("id").isin(df_processed["id"]))
    df_left = df_left.drop("_c0_trim")
    return 'website', df_left, df_processed.select(F.sum("_c1"), F.lit('website').alias("sem_type"))


def building_code_regex(val):
    regex = re.compile(r'^[a-zA-Z][0-9].*')
    return re.match(regex, val) is not None


def count_building_code(df_to_process):
    udf_building_code_regex = F.udf(building_code_regex)
    df_to_process2 = df_to_process.withColumn("_c0_trim", F.regexp_replace(F.col("_c0"), "\\s+", ""))
    df_processed = df_to_process2.filter(udf_building_code_regex(df_to_process2._c0_trim) == True)
    df_left = df_to_process2.filter(F.col("id").isin(df_processed["id"]))
    df_left = df_left.drop("_c0_trim")
    return 'building_classification', df_left, df_processed.select(F.sum("_c1"), F.lit('building_classification').alias("sem_type"))


def count_car_make(df_to_process):
    df_processed = df_to_process.join(df_pre_car_make, F.levenshtein(F.lower(df_to_process._c0), F.lower(df_pre_car_make._c0)) < 3)
    # df_processed = calc_jaccard_sim(df_to_process, df_pre_car_make)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'car_make', df_left, df_processed.select(F.sum("_c1"), F.lit('car_make').alias("sem_type"))


def count_vehicle_type(df_to_process):
    # df_processed = df_to_process.join(df_pre_vehicle_type, levenshtein(lower(df_to_process._c0), lower(df_pre_vehicle_type._c0)) < 2)
    df_processed = calc_jaccard_sim(df_to_process, df_pre_vehicle_type)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'vehicle_type', df_left, df_processed.select(F.sum("_c1"), F.lit('vehicle_type').alias("sem_type"))


def count_parks(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_park)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'park_playground', df_left, df_processed.select(F.sum("_c1"), F.lit('parks_playground').alias("sem_type"))


def count_business(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_business)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'business_name', df_left, df_processed.select(F.sum("_c1"), F.lit('business_name').alias("sem_type"))


def count_location_type(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_location_type)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'location_type', df_left, df_processed.select(F.sum("_c1"), F.lit("location_type").alias("sem_type"))


def count_school_name(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_school_name)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'school_name', df_left, df_processed.select(F.sum("_c1"), F.lit('school_name').alias("sem_type"))


def count_color(df_to_process):
    df_processed = df_to_process.join(df_pre_color,
                                      F.levenshtein(F.lower(df_to_process._c0),
                                                    F.lower(df_pre_color._c0)) < 3)
    # df_processed = calc_jaccard_sim(df_left, df_pre_city_agency_abbrev)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'color', df_left, df_processed.select(F.sum("_c1"), F.lit('color').alias("sem_type"))


def count_city_agency(df_to_process):
    df_processed = calc_jaccard_sim(df_to_process, df_pre_city_agency)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'city_agency', df_left, df_processed.select(F.sum("_c1"), F.lit('city_agency').alias("sem_type"))


def count_city_agency_abbrev(df_to_process):
    df_processed = df_to_process.join(df_pre_city_agency_abbrev,
                                      F.levenshtein(F.lower(df_to_process._c0), F.lower(df_pre_city_agency_abbrev._c0)) < 1)
    # df_processed = calc_jaccard_sim(df_left, df_pre_city_agency_abbrev)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'city_agency', df_left, df_processed.select(F.sum("_c1"), F.lit('city_agency').alias("sem_type"))


def count_area_study(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_area_study)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'area_of_study', df_left, df_processed.select(F.sum("_c1"), F.lit('area_of_study').alias("sem_type"))


def count_subject(df_to_process):
    # df_processed = df_to_process.join(df_pre_city_agency,
    #                                   F.levenshtein(F.lower(df_to_process._c0), F.lower(df_pre_car_make._c0)) < 3)
    df_processed = calc_jaccard_sim(df_to_process, df_pre_subject)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'subject_in_school', df_left, df_processed.select(F.sum("_c1"), F.lit('subject_in_school').alias("sem_type"))


def count_school_level(df_to_process):
    df_processed = df_to_process.join(df_pre_school_level,
                                      F.levenshtein(F.lower(df_to_process._c0), F.lower(df_pre_school_level._c0)) < 3)
    # df_processed = calc_jaccard_sim(df_to_process, df_pre_school_level)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'school_level', df_left, df_processed.select(F.sum("_c1"), F.lit('school_level').alias("sem_type"))


def count_college_name(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_college)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.filter(F.col("id").isin(df_processed["id"]))
    return 'college_name', df_left, df_processed.select(F.sum("_c1"), F.lit('college_name').alias("sem_type"))


def pre_compute_vehicle_type():
    # https://data.ny.gov/Transportation/Vehicle-Makes-and-Body-Types-Most-Popular-in-New-Y/3pxy-wy2i
    df_vehicle_type = spark.read.option("header", "true").option("delimiter", ",").csv('/user/gl758/hd/vehicle_type.csv')
    df_vehicle_type = df_vehicle_type.select("Body Type").filter(F.col("Body Type") != "????").distinct()
    df_vehicle_type = df_vehicle_type.withColumnRenamed("Body Type", "_c0")
    df_vehicle_type = pre_compute_transformer(df_vehicle_type)
    return df_vehicle_type


def pre_compute_car_make():
    # obtained csv file from
    # https://github.com/arthurkao/vehicle-make-model-data
    df_car_make = spark.read.option("header", "true").option("delimiter", ",").csv('/user/gl758/hd/car_make.csv')
    df_car_make = df_car_make.withColumnRenamed("make", "_c0")
    df_car_make = pre_compute_transformer(df_car_make)
    return df_car_make


def pre_compute_park():
    list_park_names = ["zone", "park", "playground", "plgd", "beach", "east", "rockaway", "parkway", "south", "river",
                       "pond", "malls", "fort", "island", "meadow", "hill", "west", "lake", "coney", "crotona", "north",
                       "kissena", "alley", "point", "ocean", "field", "highbridge", "area", "lot", "landscape",
                       "courts", "bronx", "tot", "grove", "one", "fields", "washington", "broadway", "st.", "inwood",
                       "highland", "waterfront", "road", "dr.", "shore", "eastern", "cunningham", "hook", "riverside",
                       "red", "canarsie", "mall", "woods", "baisley", "center", "prospect", "marine", "soundview",
                       "morningside", "square", "esplanade", "battery", "cedar", "claremont", "upper", "tryon",
                       "corridor", "juniper", "lower", "house", "pedestrian", "lawn", "mary's", "mccarren", "tennis",
                       "pelham", "valley", "van", "greene", "walk", "memorial", "clove", "central", "lincoln",
                       "promenade", "brookville", "lakes", "bay", "nicholas", "orchard", "trail", "cortlandt",
                       "mosholu", "parade", "pool", "silver", "heron", "marcus", "conference", "high"]
    df_park_names_array = spark.createDataFrame(list_park_names, StringType()).select(F.collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    # df_parks = spark.read.option("delimiter", ",").csv('/user/gl758/hd/park_names.csv')
    # df_park_names = df_parks.select('_c4').distinct()
    # df_park_names = df_park_names.withColumnRenamed("_c4", "_c0")
    # df_park_names_array = get_top_n_in_array(df_park_names, 200)
    return df_park_names_array


def pre_compute_business():
    list_business_names = ["inc.", "inc", "corp.", "llc", "corp", "deli", "construction", "grocery", "auto", "new",
                           "food", "contracting", "wireless", "laundromat", "home", "michael", "john", "market",
                           "corporation", "cleaners", "joseph", "group", "parking", "robert", "construction,",
                           "services", "gourmet", "general", "david", "anthony", "shop", "james", "jose", "ltd.",
                           "service", "street", "improvement", "store", "repair", "grocery,", "richard", "laundry",
                           "william", "design", "avenue", "convenience", "jewelry", "mini", "thomas", "center",
                           "daniel", "management", "services,", "york", "star", "express", "ave", "christopher", "park",
                           "cleaners,", "east", "singh,", "restaurant", "dry", "laundromat,", "city", "best", "george",
                           "builders", "frank", "peter", "luis", "nyc", "contracting,", "towing", "gold", "garage",
                           "candy", "group,", "steven", "paul", "enterprises", "juan", "one", "restoration", "jr,",
                           "mobile", "deli,", "mark", "incorporated", "electronics", "grill", "west", "usa", "stop",
                           "meat", "edward", "medical", "carlos", "charles", "mohammed", "mart", "st.", "co.,", "tire",
                           "kevin", "green", "rodriguez,", "renovation", "development", "super", "car", "company",
                           "nicholas", "solutions", "pharmacy", "andrew", "news", "market,", "recovery", "remodeling",
                           "broadway", "sales", "family", "contractors", "collision", "american", "painting", "fruit",
                           "mohammad", "cleaner", "brian", "supply", "l.l.c.", "supermarket", "king", "trading",
                           "smoke", "improvements", "international", "discount", "renovations", "vincent", "lee,",
                           "cafe", "matthew", "enterprises,", "patrick", "island", "tech", "brothers", "kim,",
                           "brooklyn", "stephen", "victor", "ronald", "body", "mohamed", "eric", "lucky", "jason",
                           "kenneth", "ali", "jonathan", "plus", "williams,", "alexander", "world", "associates,",
                           "ltd", "building", "clean", "united", "interiors", "jeffrey", "fresh", "ave.", "automotive",
                           "first", "metro", "ny,", "associates", "gonzalez,", "farm", "wash", "maria", "sons",
                           "smith,", "maintenance", "care", "big", "furniture", "angel", "quality", "computer", "chen,",
                           "louis", "enterprise", "lopez,", "custom"]
    df_business_names_array = spark.createDataFrame(list_business_names, StringType()).select(
        F.collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    # df_business = spark.read.option("header", "true").option("delimiter", "\t").csv('/user/hm74/NYCOpenData/w7w3-xahh.tsv.gz')
    # df_business_names = df_business.select('Business Name').distinct().where(col("Business Name").isNotNull())
    # df_business_names = df_business_names.withColumnRenamed('Business Name', "_c0")
    # df_business_names_array = get_top_n_in_array(df_business_names, 200)
    return df_business_names_array


def pre_compute_location_type():
    list_location_type = ["airport", "atm", "bank", "bar", "club", "salon", "bookstore", "book", "bus", "bridge",
                         "store", "church", "building", "department", "office", "doctor", "cleaner", "laundry",
                         "factory", "supermarket", "station", "gym", "hospital", "jewelry", "pier", "mosque",
                         "school", "residence", "restaurant", "storage", "facility", "taxi", "transite", "tunnel"]
    df_location_types_array = spark.createDataFrame(list_location_type, StringType()).select(
       F.collect_list("value")).withColumnRenamed(
       "collect_list(value)", "to_match")
    return df_location_types_array


def pre_compute_school_name():
    list_school_name = ['gardens', '1', 'leaders', 'prep', 'education', 'school,', '4', 'john', 'park', 'stuyvesant',
                        'house', 'arts', 'ps', 'at', 'studies', 's', 'science', 'catholic', 'lady', 'my', 'learn',
                        'scholars', 'college', 'friends', 'crown', 'new', 'early', 'm', 'charles', 'saint', 'ps/is',
                        '5', 'learning', '&', 'charter', 'richmond', 'future', 'city', 'place', 'side', 'world',
                        'institute', 'start', 'brooklyn', 'corp', 'bushwick', 'sciences', 'williamsburg', 'nursery',
                        'childhood', 'board', 'ii', 'south', 'children', "children's", 'william', 'north', 'robert',
                        'school', 'excellence', 'street', 'bronx', 'bedford', 'life', 'llc', 'r', 'academy', 'urban',
                        'preparatory', 'little', 'dr', 'mosaic', 'collaborative', 'queens', 'community', 'lutheran',
                        'educational', 'technology', 'bay', 'j', 'neighborhood', 'services', 'all', 'yeshiva', 'york',
                        'h', 'edward', '20', 'iii', 'care', 'inc', 'united', 'first', 'i', 'career', 'child',
                        'american', 'young', 'daycare', 'success', 'island', 'leadership', 'our', '2', 'e', 'of',
                        'district', 'staten', 'collegiate', 'hill', 'is', 'ps/ms', 'center', 'center,', 'ny', 'church',
                        'secondary', 'harlem', 'hall', 'day', '3', 'jhs', 'richard', 'washington', 'math', 'head',
                        'development', 'program', 'public', 'dcc', 'l', 'west', 'a', 'mott', 'assembly', 'f', 'b',
                        'village', 'international', 'george', 'kids', 'language', 'st', 'heights', 'and', 'east',
                        'health', 'star', 'family', 'for', 'discovery', 'w', 'technical', 'avenue', 'manhattan',
                        'the', 'ms', 'ave']
    df_school_name_array = spark.createDataFrame(list_school_name, StringType()).select(
        F.collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    return df_school_name_array


def pre_compute_color():
    df_color = spark.read.option("header", "true").option("delimiter", ",").csv('/user/zw1923/colors.csv')
    df_color = df_color.withColumnRenamed("colors", "_c0")
    df_color = pre_compute_transformer(df_color)
    return df_color


def pre_compute_city_agency():
    df_city_agency = spark.read.option("header", "true").option("delimiter", ",").csv('/user/zw1923/city_agency.csv')
    df_city_agency = df_city_agency.withColumnRenamed("full", "_c0")
    df_city_agency = pre_compute_transformer(df_city_agency)
    df_city_agency_abbrev = spark.read.option("header", "true").option("delimiter", ",").csv('/user/zw1923/city_agency_abbrev.csv')
    df_city_agency_abbrev = df_city_agency_abbrev.withColumnRenamed("abbrev", "_c0")
    # df_city_agency_abbrev = pre_compute_transformer(df_city_agency_abbrev)
    return df_city_agency, df_city_agency_abbrev


def pre_compute_area_of_study():
    list_area_study = ['mathematics', 'formal', 'natural', 'literature', 'social', 'applied', 'history',
                       'chemistry', 'health', 'law', 'medicine', 'sciences', 'engineering', 'technology', 'archaeology',
                       'economics', 'earth', 'political', 'science', 'geography', 'anthropology', 'computer',
                       'psychology', 'sociology', 'biology', 'languages', 'theology', 'work', 'humanities', 'human',
                       'business', 'philosophy', 'space', 'performing', 'visual', 'arts', 'statistics', 'physics']
    df_area_study_array = spark.createDataFrame(list_area_study, StringType()).select(
        F.collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    return df_area_study_array


def pre_compute_subject():
    df_subject = spark.read.option("header", "true").option("delimiter", ",").csv('/user/zw1923/subjects.csv')
    df_subject = df_subject.withColumn("_c0", F.lower(df_subject.subjects)).drop("subjects")
    df_subject = pre_compute_transformer(df_subject)
    return df_subject


def pre_compute_school_level():
    df_school_level = spark.read.option("header", "true").option("delimiter", ",").csv('/user/zw1923/school_levels.csv')
    df_school_level = df_school_level.withColumn("_c0", F.lower(df_school_level.levels))
    # df_school_level = pre_compute_transformer(df_school_level)
    return df_school_level


def pre_compute_college_name():
    list_college_name = ['state', 'resources', 'nursing', 'arts', 'seminary', 'art', 'boricua', 'harry', 'union',
                         'tisch', 'plaza', 'university', 'conservatory', 'macaulay', 'wood', 'berkeley', 'mcallister',
                         'mannes', 'community', 'technical', 'labor', 'aeronautics', 'asa', 'juilliard', 'museum',
                         'institute', 'rockefeller', 'frank', 'bard', 'jay', 'hebrew', 'cuny', 'bramson', 'phillips',
                         'monroe', 'research', 'hospital', 'cardozo', 'the', 'mandl', 'wagner', 'beth', 'architecture',
                         'bronx', 'study', 'brunswick', 'of', 'parsons', 'design', 'isaac', '&', 'e.', 'maritime',
                         'general', 'sinai', 'gallatin', 'pratt', 'america', 'cornell', 'school', 'marymount', 'icahn',
                         'economics', 'vaughn', 'brooklyn', 'for', 'science', 'staten', 'studio', 'theological',
                         'arsdale', 'dramatic', 'hofstra', 'ort', 'flushing', 'affairs', 'visual', 'nyack', 'israel',
                         'european', 'jazz', 'gilder', 'justice', 'and', 'law', 'social', 'natural', 'gerstner', 'new',
                         'briarcliffe', 'keller', 'metropolitan', 'american', 'massage', 'individualized', 'saint',
                         'tech', 'borough', 'n.', 'fordham', 'city', 'cooper', 'francis', 'benjamin', 'laboratory',
                         'sloan', 'ballet', 'tobe-coburn', 'college', 'sciences', 'swedish', 'hunter', 'rabbi', 'lang',
                         'van', 'business', 'contemporary', 'st.', 'engineering', 'nyc', 'music', 'pace', 'criminal',
                         'island', 'international', 'einstein', 'globalization', 'technology', 'career', 'psychoanalysis',
                         'interior', 'mercy', 'manhattan', 'kingsborough', 'policy', 'professional', 'oriental', 'touro',
                         'tri-state', 'guttman', 'vincent', 'teachers', 'york', 'ministry', "john's", 'mount', 'downstate',
                         'street', 'queens', 'lehman', 'allied', 'william', 'empire', 'ailey', 'bethel', 'fashion',
                         'medicine', 'urban', 'biomedical', 'polytechnic', 'medgar', 'richard', 'g.', 'helene', 'eugene',
                         'jewish', "joseph's", 'devry', 'fuld', "sotheby's", 'therapy', 'rochelle', 'evers', 'east',
                         'graduate', 'health', 'studies', 'jr.', 'education', 'medical', 'john', 'optometry', 'crew',
                         'kettering', 'environment', 'maestro', 'columbia', 'milano', 'public', 'elchanon', 'journalism',
                         'alvin', 'zarb', 'film', 'suny', 'pacific', 'center', 'musical', 'yeshiva', 'bank',
                         'merchandising', 'management', 'queensborough', 'at', 'academy', 'program', 'honors',
                         'globe', 'history', 'long', 'barnard', 'liberal', 'acupuncture', 'hostos', 'laguardia', 'dance',
                         'albert', "king's", 'drama', 'weill', "christie's", 'baruch', 'cuny', 'liu', 'nyu', 'suny',
                         'aada', 'amda', 'fit', 'lim', 'nyaa', 'nyfa', 'nyit', 'nyss', 'sva', 'nyls', 'nymc', 'ats',
                         'gts', 'jts', 'nyts', 'uts', 'nyif', 'ort']
    df_college_name_array = spark.createDataFrame(list_college_name, StringType()).select(
        F.collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    return df_college_name_array


def pre_compute_transformer(df_to_process):
    # lpad and rpad are complete trash and don't take column length, have to make udf, incompetence is high
    pad_udf = F.udf(pad_custom)
    df_to_process = df_to_process.withColumn('_c0_pad', pad_udf("_c0"))
    tokenizer = Tokenizer(inputCol="_c0", outputCol="token_raw")
    remover = StopWordsRemover(inputCol="token_raw", outputCol="token_filtered")
    regex_tokenizer = RegexTokenizer(inputCol="_c0", outputCol="letters", pattern="")
    regex_tokenizer_pad = RegexTokenizer(inputCol="_c0_pad", outputCol="letters_pad", pattern="")
    ngram = NGram(n=3, inputCol="letters", outputCol="ngrams")
    ngram_pad = NGram(n=3, inputCol="letters_pad", outputCol="ngrams_pad")
    pipeline = [tokenizer, remover, regex_tokenizer, regex_tokenizer_pad, ngram, ngram_pad]
    for stage in pipeline:
        df_to_process = stage.transform(df_to_process)
    return df_to_process


def get_tokens_match_over_diff(df_to_process):
    df_processed = df_to_process.withColumn("score", F.size(F.array_intersect("token_filtered", "to_match"))/F.size("token_filtered"))
    return df_processed


def get_top_n_in_array(df_lookup, top):
    df_lookup = df_lookup.select('_c0').distinct()
    tokenizer = Tokenizer(inputCol="_c0", outputCol="token_raw")
    remover = StopWordsRemover(inputCol="token_raw", outputCol="token_filtered")
    df_lookup = tokenizer.transform(df_lookup)
    df_lookup = remover.transform(df_lookup)
    df_lookup = df_lookup.select((F.explode("token_filtered"))).groupby("col").count().sort('count', ascending=False)
    df_lookup = df_lookup.filter(F.length("col") > 2).limit(top).select(
        F.collect_list("col")).withColumnRenamed("collect_list(col)", "to_match")
    return df_lookup


def calc_jaccard_sim(df_to_process, df_match, thresh=.3, padded=True):
    if padded:
        df_processed = df_to_process.join(df_match,
                           (F.size(F.array_intersect(df_to_process.ngrams_pad, df_match.ngrams_pad)) /
                            F.size(F.array_union(df_to_process.ngrams_pad, df_match.ngrams_pad))) > thresh)
    else:
        df_processed = df_to_process.join(df_match,
                           (F.size(F.array_intersect(df_to_process.ngrams, df_match.ngrams)) /
                            F.size(F.array_union(df_to_process.ngrams, df_match.ngrams))) > thresh)
    return df_processed


def pad_custom(val):
    return "^^{}$$".format(val)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("big_data_proj_part2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    conf = spark.sparkContext._conf.setAll(
        [('spark.executor.memory', '8g'), ('spark.app.name', 'big_data_proj'), ('spark.executor.cores', '4'),
         ('spark.cores.max', '4'), ('spark.driver.memory', '8g')])
    spark.sparkContext.stop()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    new_conf = spark.sparkContext._conf.getAll()
    print(new_conf)
    df_pre_park = pre_compute_park()
    df_pre_park = df_pre_park.cache()

    df_pre_business = pre_compute_business()
    df_pre_business = df_pre_business.cache()

    df_pre_car_make = pre_compute_car_make()
    df_pre_car_make.cache()

    df_pre_vehicle_type = pre_compute_vehicle_type()
    df_pre_vehicle_type.cache()

    df_pre_location_type = pre_compute_location_type()
    df_pre_location_type.cache()

    df_pre_school_name = pre_compute_school_name()
    df_pre_school_name = df_pre_school_name.cache()

    df_pre_color = pre_compute_color()
    df_pre_color = df_pre_color.cache()

    df_pre_city_agency, df_pre_city_agency_abbrev = pre_compute_city_agency()
    df_pre_city_agency = df_pre_city_agency.cache()
    df_pre_city_agency_abbrev = df_pre_city_agency_abbrev.cache()

    df_pre_area_study = pre_compute_area_of_study()
    df_pre_area_study = df_pre_area_study.cache()

    df_pre_subject = pre_compute_subject()
    df_pre_subject = df_pre_subject.cache()

    df_pre_school_level = pre_compute_school_level()
    df_pre_school_level = df_pre_school_level.cache()

    df_pre_college = pre_compute_college_name()
    df_pre_college = df_pre_college.cache()

    df_pre_city = pre_compute_city()
    df_pre_city = df_pre_city.cache()

    df_pre_street = pre_compute_street()
    df_pre_street = df_pre_street.cache()

    df_pre_neighborhood = pre_compute_neighborhood()
    df_pre_neighborhood = df_pre_neighborhood.cache()

    main()
