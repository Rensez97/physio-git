import requests
import pickle
import time
import pandas as pd
import concurrent.futures
import traceback
import datetime
import locale
import googlemaps
import json

from datetime import date
from bs4 import BeautifulSoup


def retry_link(webpage):
    success = False
    retries = 0
    while not success and retries < 5:
        job_page = requests.get(webpage)
        if job_page.status_code == 200:
            success = True
        else:
            retries += 1
            print(webpage,job_page.status_code)
            time.sleep(11)
    
    return job_page


def convert_german_date(str_date):
    
    locale.setlocale(locale.LC_ALL, 'de_DE')
    # input_date = "Freitag, 12. Januar 2024"
    date_obj = datetime.datetime.strptime(str_date, "%A, %d. %B %Y")
    output_date = date_obj.strftime("%d-%m-%Y")
    
    return output_date


def thread(job,zipcodes):
    error_list = []
    try:
        webpage = "https://www.physioswiss.ch"+job["href"]
        job_page = retry_link(webpage)

        soup3 = BeautifulSoup(job_page.text, "html.parser")
        title = soup3.find("h1", {"property": "title"}).text
        grey_tags = soup3.find("div", {"class": "tags grey"})
        tags = grey_tags.find_all("span", {"class": "tag"})

        id = tags[0].text.strip()
        percent = " ".join(tags[1].text.strip().split())
        loc = tags[2].text.strip()
        company = tags[3].text.strip()
        date_posted = tags[4].text.strip().split(":  ")[1]
        posted = convert_german_date(date_posted)

        details = soup3.find("table", {"class": "details"})
        blocks = details.find_all("td")
        start = blocks[1].text.split(":")[1].strip()
        job_type = blocks[2].text.split(":")[1].strip()
        speciality = blocks[3].text.split("\n")[2].strip()
        duration = blocks[4].text.split(":")[1].strip()


        contact_menu  = soup3.find("div", {"class": "g-s-right wide details-meta"})
        contact_boxes = contact_menu.find_all('div', recursive=False)

        name = contact_boxes[0].find("h5").text.strip()
        text_paragraph = " ".join(contact_boxes[0].find("p", {"class": "fine-print"}).text.split())

        paragraph_list = text_paragraph.split()
        main_phone = ""
        direct_phone = ""
        email = ""
        for item in paragraph_list:
            if item == "Hauptnummer:":
                for item2 in paragraph_list[paragraph_list.index(item)+1:]:
                    if item2 not in ["Telefon", "E-Mail:"]:
                        main_phone += item2
                    else:
                        break
            if item == "Telefon":
                for item2 in paragraph_list[paragraph_list.index(item)+3:]:
                    if item2 != "E-Mail:":
                        direct_phone += item2
                    else:
                        break
            if item == "E-Mail:":
                email = paragraph_list[paragraph_list.index(item)+1]

        # if len(contact_boxes) == 3:
        #     text_paragraph2 = contact_boxes[1].find("p", {"class": "fine-print"}).text.split("\n")[1:]
        #     work_street = text_paragraph2[0].strip()
        #     work_zipcode = text_paragraph2[1].strip()
        #     work_kanton = text_paragraph2[2].split(":")[1].strip()
        # else:
        #     work_street = ""
        #     work_zipcode = ""
        #     work_kanton = ""

        text_paragraph3 = contact_boxes[-1].find("p", {"class": "fine-print"}).text.split("\n")[1:-1]
        if len(text_paragraph3) == 4:
            employer = text_paragraph3[0].strip()
            employer_street = text_paragraph3[1].strip()
            employer_extra = ""
            employer_zipcode = text_paragraph3[2].strip().split()[0]
            employer_town = text_paragraph3[2].strip().split()[1]
            employer_kanton = text_paragraph3[3].split(":")[1].strip()
        if len(text_paragraph3) == 5:
            employer = text_paragraph3[0].strip()
            employer_street = text_paragraph3[1].strip()
            employer_extra = text_paragraph3[2].strip()
            employer_zipcode = text_paragraph3[3].strip().split()[0]
            employer_town = text_paragraph3[3].strip().split()[1]
            employer_kanton = text_paragraph3[4].split(":")[1].strip()
        if len(text_paragraph3) == 6:
            employer = text_paragraph3[0].strip()
            employer_street = text_paragraph3[1].strip()
            employer_extra = text_paragraph3[3].strip()
            employer_zipcode = text_paragraph3[4].strip().split()[0]
            employer_town = text_paragraph3[4].strip().split()[1]
            employer_kanton = text_paragraph3[5].split(":")[1].strip()

        full_text = soup3.find("div", {"class" :"g-s-main wide-right content"}).text
        travel_dist = ""
        if "praxis" in full_text.lower():
            praxis = "Ja"
        else:
            praxis = ""

        for city, zip_list in zipcodes.items():
            if employer_zipcode in zip_list:
                travel_dist = city
                break

        result = {"Arbeitgeber": employer,
                "Adresse": employer_street,
                "Adresse extra": employer_extra,
                "Postzahl": employer_zipcode,
                "Ort": employer_town,
                "Kanton": employer_kanton,
                "<25 KM": travel_dist,
                "Praxis": praxis,
                "Stellenbeschreibung": title,
                "Arbeitspensum": percent,
                "Stellenantrit per": start,
                "Anstellungsverhältnis": duration,
                "Stellenangebot online per": posted,
                "Archivierungsdatum": "",
                "Kontakt": name,
                "Hauptnummer": main_phone,
                "Telefon Direkt": direct_phone,
                "E-mail": email,
                "Website Arbeitgeber": "",
                "Link": webpage,
                "Aktiv": "Ja"}

    except Exception as e:
        print("Exception place 4:", e, webpage, job_page, soup3, "\n",traceback.print_exc())
        if soup3:
            print(soup3)

    return result



def physio_swiss(zipcodes):
    print("Physio Swiss checken...")
    results = []
    error_list = []
    threaded_start = time.time()
    try:
        for i in range(1,30):
            print(i)
            req = requests.get("https://www.physioswiss.ch/de/jobs?page=" + str(i))
            soup = BeautifulSoup(req.text, "html.parser")
            soup2 = soup.find("div", {"id": "results"})
            jobs = soup2.find_all("a", {"class": "result-preview"})

            if len(jobs) == 0:
                break
            # if i > 1:
            #     break

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for job in jobs:
                    try:
                        futures.append(executor.submit(thread, job, zipcodes))
                    except Exception as e:
                        print("Exception place 2:", e)
                        error_list.append(e)
                # for future in concurrent.futures.as_completed(futures):
                #     try:
                #         result = future.result()
                #         results.append(result)
                #     except Exception as e:
                #         print("Exception place 3:", e)
                #         error_list.append(e)
            for future in futures:
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print("Exception place 3:", e)
                    error_list.append(e)

    except Exception as e:
        print("Exception place 1:", e, "\n",traceback.print_exc())
        error_list.append(e)

    print("End Physio Swiss\n Time for completion:", time.time() - threaded_start)

    return results


def company_file(databank):
    employer_dict = {}
    for item in databank:
        if item["Arbeitgeber"] not in employer_dict:
            employer_dict[item["Arbeitgeber"]] = {
                "Adresse": item["Adresse"],
                "Adresse extra": item["Adresse extra"],
                "Postzahl": item["Postzahl"],
                "Ort": item["Ort"],
                "Kanton": item["Kanton"],
                "<25 KM": item["<25 KM"],
                "Praxis": item["Praxis"],
                "Kontakt": item["Kontakt"],
                "Hauptnummer": item["Hauptnummer"],
                "Telefon Direkt": item["Telefon Direkt"],
                "E-mail": item["E-mail"],
                "Website Arbeitgeber": item["Website Arbeitgeber"],
                "Mail geschickt": "",
                "Telefonisch Kontakt gehabt": "",
                "Voorbijgaan moment": "",
                "Voorbij geweest": "",
                "Formular ausgefüllt": "",
                "Stellen aktiv": 0,
                "Stellen total": 0
            }
    print(employer_dict)
    with open("Arbeitgebers.pkl", "wb") as f:
        pickle.dump(employer_dict,f)


def get_website(name,place):
    try:
        gmaps = googlemaps.Client(key='AIzaSyDvftfSxdoS_rNmfIB3XFHeL70Uri_xSD4')
        places = gmaps.places(f'{name} near {place}', language='en')
        place_id = places['results'][0]['place_id']
        place_details = gmaps.place(place_id, language='en')
        website = place_details['result']['website']
    except:
        website = ""
        print(name,place)
    
    return website



def main():

    luzern = ['5632', '5636', '5637', '5642', '5643', '5644', '5645', '5646', '5647', '5712', '5734', '5735', '5736', '5737', '6000', '6002', '6003', '6004', '6005', '6006', '6007', '6008', '6009', '6010', '6011', '6012', '6013', '6014', '6015', '6016', '6017', '6018', '6019', '6020', '6021', '6022', '6023', '6024', '6025', '6026', '6027', '6028', '6030', '6031', '6032', '6033', '6034', '6035', '6036', '6037', '6038', '6039', '6042', '6043', '6044', '6045', '6047', '6048', '6052', '6053', '6055', '6056', '6060', '6061', '6062', '6063', '6064', '6066', '6067', '6072', '6073', '6102', '6103', '6105', '6106', '6110', '6112', '6113', '6114', '6122', '6123', '6125', '6126', '6130', '6132', '6160', '6161', '6162', '6163', '6166', '6167', '6170', '6203', '6204', '6205', '6206', '6207', '6208', '6210', '6212', '6213', '6214', '6215', '6216', '6217', '6218', '6221', '6222', '6231', '6232', '6233', '6248', '6274', '6275', '6276', '6277', '6280', '6281', '6283', '6284', '6285', '6286', '6287', '6288', '6289', '6294', '6295', '6300', '6301', '6302', '6303', '6304', '6305', '6310', '6312', '6314', '6317', '6318', '6319', '6330', '6331', '6332', '6333', '6340', '6341', '6342', '6343', '6344', '6346', '6349', '6353', '6354', '6356', '6362', '6363', '6365', '6370', '6371', '6372', '6373', '6374', '6375', '6376', '6377', '6382', '6383', '6386', '6387', '6388', '6402', '6403', '6404', '6405', '6410', '6414', '6415', '6416', '6422', '6424', '6440', '6441', '6442', '6466', '8932', '8933', '8934', '9545']
    bern = ['1712', '1713', '1714', '1715', '1717', '1719', '1738', '1783', '1785', '1786', '1792', '1793', '1794', '1797', '2503', '2540', '2552', '2553', '2554', '2555', '2556', '2557', '2558', '2560', '2562', '2563', '2564', '2565', '2572', '2575', '2576', '2577', '3000', '3001', '3002', '3003', '3004', '3005', '3006', '3007', '3008', '3010', '3011', '3012', '3013', '3014', '3015', '3017', '3018', '3019', '3020', '3024', '3027', '3029', '3030', '3032', '3033', '3034', '3035', '3036', '3037', '3038', '3039', '3040', '3041', '3042', '3043', '3044', '3045', '3046', '3047', '3048', '3049', '3050', '3052', '3053', '3054', '3063', '3065', '3066', '3067', '3068', '3070', '3071', '3072', '3073', '3074', '3075', '3076', '3077', '3078', '3082', '3083', '3084', '3085', '3086', '3087', '3088', '3089', '3095', '3096', '3097', '3098', '3099', '3110', '3111', '3112', '3113', '3114', '3115', '3116', '3122', '3123', '3124', '3125', '3126', '3127', '3128', '3132', '3144', '3145', '3147', '3148', '3150', '3152', '3153', '3154', '3155', '3156', '3157', '3158', '3159', '3172', '3173', '3174', '3175', '3176', '3177', '3178', '3179', '3182', '3183', '3184', '3185', '3186', '3202', '3203', '3204', '3205', '3206', '3207', '3208', '3210', '3212', '3213', '3214', '3215', '3216', '3225', '3226', '3237', '3250', '3251', '3252', '3253', '3254', '3255', '3256', '3257', '3262', '3263', '3264', '3266', '3267', '3268', '3270', '3271', '3272', '3273', '3274', '3280', '3282', '3283', '3284', '3285', '3286', '3292', '3293', '3294', '3295', '3296', '3297', '3298', '3302', '3303', '3305', '3306', '3307', '3308', '3309', '3312', '3313', '3314', '3315', '3317', '3321', '3322', '3323', '3324', '3325', '3326', '3400', '3401', '3402', '3412', '3413', '3414', '3415', '3417', '3418', '3419', '3421', '3422', '3423', '3424', '3425', '3426', '3427', '3428', '3432', '3433', '3434', '3435', '3436', '3437', '3438', '3439', '3452', '3454', '3455', '3456', '3472', '3473', '3503', '3504', '3506', '3507', '3508', '3510', '3512', '3513', '3531', '3532', '3533', '3534', '3535', '3538', '3543', '3550', '3603', '3609', '3612', '3613', '3615', '3617', '3627', '3628', '3629', '3634', '3635', '3636', '3638', '3661', '3662', '3663', '3664', '3665', '3671', '3672', '3673', '3674', '4571', '4576', '4577', '4578', '4579', '4581', '4582', '4583', '4584', '4585', '4586', '4587', '4588']
    basel = ['2805', '2813', '2814', '2825', '2826', '2827', '2828', '4000', '4001', '4002', '4003', '4004', '4005', '4007', '4008', '4009', '4010', '4011', '4012', '4013', '4015', '4016', '4017', '4018', '4019', '4020', '4023', '4024', '4025', '4030', '4031', '4032', '4033', '4034', '4035', '4039', '4040', '4041', '4042', '4051', '4052', '4053', '4054', '4055', '4056', '4057', '4058', '4059', '4060', '4065', '4070', '4075', '4078', '4080', '4081', '4082', '4083', '4084', '4085', '4086', '4087', '4088', '4089', '4091', '4092', '4093', '4094', '4095', '4096', '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', '4112', '4114', '4115', '4116', '4117', '4118', '4123', '4124', '4125', '4126', '4127', '4132', '4133', '4142', '4143', '4144', '4145', '4146', '4147', '4148', '4153', '4202', '4203', '4204', '4206', '4207', '4208', '4222', '4223', '4224', '4225', '4226', '4227', '4228', '4229', '4232', '4233', '4234', '4242', '4243', '4244', '4245', '4246', '4247', '4252', '4253', '4254', '4302', '4303', '4304', '4305', '4310', '4312', '4313', '4314', '4315', '4322', '4323', '4402', '4410', '4411', '4412', '4413', '4414', '4415', '4416', '4417', '4418', '4419', '4421', '4422', '4423', '4424', '4425', '4426', '4431', '4432', '4433', '4434', '4435', '4436', '4437', '4441', '4442', '4443', '4444', '4447', '4450', '4451', '4452', '4453', '4455', '4456', '4457', '4460', '4461', '4462', '4463', '4464', '4465', '4466', '4717', '4719']
    zurich = ['5242', '5243', '5244', '5300', '5400', '5401', '5402', '5404', '5405', '5406', '5408', '5412', '5413', '5415', '5416', '5420', '5423', '5425', '5426', '5430', '5431', '5432', '5436', '5442', '5443', '5444', '5445', '5452', '5453', '5454', '5462', '5463', '5464', '5466', '5467', '5504', '5505', '5506', '5507', '5512', '5522', '5524', '5525', '5604', '5605', '5606', '5607', '5608', '5610', '5611', '5612', '5613', '5614', '5615', '5616', '5618', '5619', '5620', '5621', '5622', '5623', '5624', '5625', '5626', '5627', '5628', '5630', '5632', '5634', '5636', '5637', '5642', '5643', '5644', '5645', '6288', '6289', '6300', '6301', '6302', '6303', '6304', '6305', '6310', '6312', '6313', '6314', '6319', '6330', '6331', '6332', '6333', '6340', '6341', '6342', '6345', '6346', '6349', '8000', '8001', '8002', '8003', '8004', '8005', '8006', '8008', '8010', '8020', '8021', '8022', '8023', '8024', '8026', '8027', '8031', '8032', '8033', '8034', '8035', '8036', '8037', '8038', '8040', '8041', '8042', '8043', '8044', '8045', '8046', '8047', '8048', '8049', '8050', '8051', '8052', '8053', '8055', '8057', '8058', '8059', '8060', '8063', '8064', '8065', '8066', '8070', '8071', '8074', '8075', '8079', '8080', '8081', '8085', '8086', '8087', '8088', '8090', '8091', '8092', '8093', '8096', '8098', '8099', '8102', '8103', '8104', '8105', '8106', '8107', '8108', '8109', '8112', '8113', '8114', '8115', '8117', '8118', '8121', '8122', '8123', '8124', '8125', '8126', '8127', '8130', '8132', '8133', '8134', '8135', '8136', '8142', '8143', '8152', '8153', '8154', '8155', '8156', '8157', '8158', '8162', '8164', '8165', '8166', '8172', '8173', '8174', '8175', '8180', '8181', '8182', '8183', '8184', '8185', '8186', '8187', '8192', '8193', '8194', '8195', '8198', '8246', '8301', '8302', '8303', '8304', '8305', '8306', '8307', '8308', '8309', '8310', '8311', '8312', '8314', '8315', '8317', '8320', '8322', '8325', '8330', '8331', '8332', '8335', '8340', '8343', '8344', '8345', '8352', '8354', '8400', '8401', '8402', '8404', '8405', '8406', '8408', '8409', '8411', '8412', '8413', '8414', '8415', '8416', '8418', '8421', '8422', '8423', '8424', '8425', '8426', '8427', '8428', '8442', '8444', '8454', '8455', '8457', '8458', '8459', '8471', '8472', '8482', '8483', '8484', '8486', '8487', '8488', '8489', '8492', '8493', '8494', '8542', '8600', '8602', '8603', '8604', '8605', '8606', '8607', '8608', '8609', '8610', '8612', '8613', '8614', '8615', '8616', '8617', '8618', '8620', '8621', '8622', '8623', '8624', '8625', '8626', '8627', '8633', '8634', '8635', '8700', '8702', '8703', '8704', '8706', '8707', '8708', '8712', '8713', '8714', '8800', '8801', '8802', '8803', '8804', '8805', '8806', '8807', '8810', '8812', '8813', '8815', '8816', '8818', '8820', '8824', '8825', '8832', '8833', '8901', '8902', '8903', '8904', '8905', '8906', '8907', '8908', '8909', '8910', '8911', '8912', '8913', '8914', '8915', '8916', '8917', '8918', '8919', '8925', '8926', '8932', '8933', '8934', '8942', '8951', '8952', '8953', '8954', '8955', '8956', '8957', '8962', '8964', '8965', '8966', '8967']
    lausanne = ['1000', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1012', '1014', '1015', '1017', '1018', '1019', '1020', '1022', '1023', '1024', '1025', '1026', '1027', '1028', '1029', '1030', '1031', '1032', '1033', '1034', '1035', '1036', '1037', '1038', '1039', '1040', '1041', '1042', '1043', '1044', '1045', '1046', '1047', '1051', '1052', '1053', '1054', '1055', '1058', '1059', '1061', '1062', '1063', '1066', '1068', '1070', '1071', '1072', '1073', '1076', '1077', '1078', '1080', '1081', '1082', '1083', '1084', '1085', '1088', '1090', '1091', '1092', '1093', '1094', '1095', '1096', '1097', '1098', '1099', '1110', '1112', '1113', '1114', '1115', '1116', '1117', '1121', '1122', '1123', '1124', '1125', '1126', '1127', '1128', '1131', '1132', '1134', '1135', '1136', '1141', '1142', '1143', '1144', '1145', '1146', '1147', '1148', '1149', '1162', '1163', '1164', '1165', '1166', '1167', '1168', '1169', '1170', '1172', '1173', '1174', '1175', '1176', '1180', '1185', '1186', '1187', '1188', '1189', '1300', '1302', '1303', '1304', '1305', '1306', '1307', '1308', '1310', '1311', '1312', '1313', '1315', '1316', '1317', '1318', '1320', '1321', '1322', '1323', '1324', '1326', '1330', '1350', '1352', '1353', '1372', '1373', '1374', '1375', '1376', '1377', '1407', '1408', '1409', '1410', '1412', '1413', '1416', '1417', '1418', '1432', '1433', '1434', '1435', '1509', '1510', '1512', '1513', '1514', '1515', '1522', '1607', '1608', '1609', '1610', '1611', '1612', '1613', '1614', '1615', '1616', '1617', '1618', '1619', '1624', '1670', '1673', '1674', '1675', '1676', '1677', '1678', '1683', '1688', '1699', '1800', '1801', '1802', '1803', '1804', '1805', '1806', '1807', '1808', '1809', '1811', '1814', '1815', '1816', '1817', '1820', '1822', '1823', '1832', '1833', '1896', '1897', '1898']
    geneve = ['1196', '1197', '1200', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1208', '1209', '1211', '1212', '1213', '1214', '1215', '1216', '1217', '1218', '1219', '1220', '1222', '1223', '1224', '1225', '1226', '1227', '1228', '1231', '1232', '1233', '1234', '1236', '1237', '1239', '1240', '1241', '1242', '1243', '1244', '1245', '1246', '1247', '1248', '1251', '1252', '1253', '1254', '1255', '1256', '1257', '1258', '1260', '1262', '1263', '1266', '1267', '1270', '1274', '1275', '1276', '1277', '1278', '1279', '1281', '1283', '1284', '1285', '1286', '1287', '1288', '1289', '1290', '1291', '1292', '1293', '1294', '1295', '1296', '1297', '1298', '1299'] 
    stgallen = ['6276', '8570', '8570', '8572', '8572', '8572', '8572', '8572', '8572', '8573', '8574', '8575', '8575', '8576', '8577', '8577', '8577', '8577', '8580', '8580', '8580', '8580', '8580', '8580', '8580', '8580', '8580', '8580', '8581', '8582', '8583', '8583', '8583', '8583', '8584', '8584', '8585', '8585', '8585', '8585', '8585', '8585', '8585', '8585', '8585', '8585', '8586', '8586', '8586', '8586', '8586', '8586', '8586', '8586', '8586', '8587', '8587', '8588', '8589', '8590', '8592', '8593', '8594', '8595', '8596', '8596', '8597', '8599', '9000', '9001', '9004', '9006', '9007', '9008', '9009', '9010', '9011', '9011', '9012', '9013', '9014', '9015', '9015', '9016', '9020', '9022', '9024', '9025', '9026', '9027', '9028', '9029', '9030', '9030', '9030', '9032', '9033', '9034', '9035', '9036', '9037', '9038', '9042', '9042', '9042', '9043', '9044', '9050', '9050', '9050', '9050', '9050', '9050', '9052', '9053', '9054', '9055', '9056', '9056', '9056', '9057', '9057', '9057', '9058', '9062', '9063', '9064', '9100', '9100', '9101', '9102', '9103', '9104', '9105', '9105', '9107', '9107', '9107', '9108', '9108', '9112', '9113', '9114', '9115', '9116', '9116', '9122', '9122', '9123', '9125', '9125', '9126', '9127', '9200', '9200', '9201', '9203', '9204', '9205', '9205', '9212', '9213', '9213', '9214', '9214', '9215', '9215', '9216', '9216', '9217', '9217', '9220', '9220', '9223', '9223', '9225', '9225', '9230', '9230', '9230', '9230', '9230', '9230', '9231', '9240', '9240', '9242', '9243', '9244', '9245', '9245', '9246', '9246', '9247', '9248', '9248', '9249', '9249', '9249', '9300', '9301', '9304', '9305', '9306', '9308', '9312', '9313', '9314', '9315', '9315', '9320', '9320', '9320', '9320', '9322', '9323', '9325', '9326', '9327', '9400', '9401', '9402', '9403', '9404', '9405', '9405', '9410', '9410', '9411', '9411', '9413', '9413', '9414', '9422', '9422', '9423', '9424', '9425', '9426', '9427', '9427', '9428', '9428', '9430', '9434', '9435', '9436', '9437', '9442', '9443', '9444', '9444', '9445', '9450', '9450', '9451', '9452', '9453', '9462', '9463', '9463', '9464', '9464', '9465', '9466', '9467', '9468', '9473', '9502', '9503', '9503', '9512', '9514', '9514', '9514', '9514', '9515', '9515', '9515', '9517', '9517', '9523', '9524', '9524', '9525', '9526', '9526', '9527', '9536', '9565', '9601', '9601', '9602', '9602', '9604', '9604', '9604', '9606', '9608', '9620', '9620', '9621', '9621', '9630', '9633', '9633', '9642', '9651', '9651', '9658']
    zipcodes = {
        'Luzern': luzern,
        'Bern': bern,
        'Basel': basel,
        'Zurich': zurich,
        'Lausanne': lausanne,
        'Geneve': geneve,
        'St. Gallen': stgallen
    }



    latest_results = physio_swiss(zipcodes)
    # databank = []
    with open("databank.pkl", "rb") as f:
        databank = pickle.load(f)

    # with open("latest_results 14012024.pkl", "rb") as f:
    #     latest_results = pickle.load(f)


    # # databank = latest_results
    # # with open("databank0.2.pkl", "rb") as f:
    # #     latest_results = pickle.load(f)
    # # databank = latest_results * 200


    # # Check if items from databank are also present in latest results
    # If not, change the status to non-active and add archive date
    formatted_date = date.today().strftime("%d%m%Y")
    for item in databank:
        active = False
        for item2 in latest_results:
            if item["Arbeitgeber"] == item2["Arbeitgeber"] and item["Stellenbeschreibung"] == item2["Stellenbeschreibung"] and item["Stellenantrit per"] == item2["Stellenantrit per"] and item["Stellenangebot online per"] == item2["Stellenangebot online per"]:
                active = True
        if active == False and item["Aktiv"] == "Ja":
            item["Aktiv"] = ""
            item["Archivierungsdatum"] = formatted_date
            # print("1111",item)


    # # Check if items from latest results are new, and if so add them to the databank
    for item in latest_results:
        present = False
        for item2 in databank:
            if item["Arbeitgeber"] == item2["Arbeitgeber"] and item["Stellenbeschreibung"] == item2["Stellenbeschreibung"] and item["Stellenantrit per"] == item2["Stellenantrit per"] and item["Stellenangebot online per"] == item2["Stellenangebot online per"]:
                present = True
        if present == False:
            item["Website Arbeitgeber"] = get_website(item["Arbeitgeber"],item["Ort"])
            databank.append(item)
            # print("2222",item)

    # company_file(databank)

    # # # Creating the Excel file
    df_full_databank = pd.DataFrame(databank)
    df_latest_results = df_full_databank[df_full_databank["Aktiv"] == "Ja"]
    # df_latest_results.sort_values(by=["Stellenangebot online per","Arbeitgeber"])
    df_only_databank = df_full_databank[df_full_databank["Aktiv"] == ""]
    df_only_databank.sort_values(by=['Arbeitgeber'])
    with pd.ExcelWriter("PhysioSwiss vacatures.xlsx") as writer:
        # df_latest_results.drop(columns=["Archivierungsdatum"]).to_excel(writer, sheet_name="Actuele Vacatures PhysioSwiss", index=False)    
        df_latest_results.to_excel(writer, sheet_name="Actuele Vacatures PhysioSwiss", index=False)
        df_only_databank.to_excel(writer, sheet_name="Databank PhysioSwiss", index=False)

    with pd.ExcelWriter(f"PhysioSwiss vacatures {formatted_date}.xlsx") as writer:
        # df_latest_results.drop(columns=["Archivierungsdatum"]).to_excel(writer, sheet_name="Actuele Vacatures PhysioSwiss", index=False)    
        df_latest_results.to_excel(writer, sheet_name="Actuele Vacatures PhysioSwiss", index=False)
        df_only_databank.to_excel(writer, sheet_name="Databank PhysioSwiss", index=False)


    # Storing the databank in a active file, and a history file
    with open("databank.pkl", "wb") as f:
        pickle.dump(databank,f)
    with open("databank "+formatted_date+".pkl", "wb") as f:
        pickle.dump(databank,f)

    # Storing the latest_results in a active file, and a history file
    with open("latest_results "+formatted_date+".pkl", "wb") as f:
        pickle.dump(latest_results,f)

if __name__ == "__main__":
    main()
