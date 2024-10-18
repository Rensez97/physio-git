import requests
import time
import concurrent.futures
import traceback
from datetime import datetime


from bs4 import BeautifulSoup
from helper_functions import retry_link, convert_german_date, capitalize_first_letter


def thread(job,zipcodes):
    try:
        webpage = job["href"]
        job_page = retry_link(webpage)
        # req = requests.get(job["href"])
        # with open("physioswiss_stelle.html", "w", encoding="utf-8") as file:
        #     file.write(req.text)
        # with open("physioswiss_stelle.html", "r", encoding="utf-8") as file:
        #     soup3 = BeautifulSoup(file, 'html.parser')
        soup3 = BeautifulSoup(job_page.text, "html.parser")
        id = webpage.split("/")[-3]
        title = soup3.find("h1", {"class": "single-jobad__page-header__title fs-mb-32 mb-md-48 mt-0"}).text.strip()
        grey_tags = soup3.find("div", {"class": "single-jobad__page-header__tag-wrapper d-flex align-items-center flex-wrap fs-mb-32 mb-md-48"})
        tags = grey_tags.find_all("span", {"class": "tag"})
        tags_texts = [tag.text.strip() for tag in tags]
        percent = " ".join(tags[-2].text.strip().split())
        start = tags[-1].text.split(":")[1].strip()
        duration = tags[-3].text.strip()

        posted_item = soup3.find("div", {"class": "single-jobad__page-header__meta-wrapper d-flex flex-wrap fs-mb-32"})
        posted_time = posted_item.find('time').get('datetime')
        posted = datetime.strptime(posted_time, '%Y-%m-%d').strftime('%d-%m-%Y')

        contact_menu  = soup3.find("div", {"class": "single-jobad__page-header__box p-32 p-md-48 mt-32 mt-md-0"})
        contact_boxes = contact_menu.find_all('address', recursive=False)
        name = contact_boxes[0].find("b").text.strip()

        contacts = contact_boxes[0].find_all('a', recursive=False)
        contact_texts = [a.text.strip() for a in contacts]
        email = ""
        main_phone = ""
        direct_phone = ""
        for item in contact_texts:
            if "@" in item:
                email = item
        phone_list = [item for item in contact_texts if item.startswith(('+', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'))]
        if len(phone_list) == 1:
            main_phone = phone_list[0]
        if len(phone_list) == 2:
            main_phone = phone_list[0]
            direct_phone = phone_list[1]

        for item in contact_boxes[1:]:
            nr_details = [text.strip() for text in item.stripped_strings if text.strip()]
            if "Arbeitgeber" in nr_details:
                nr_details.pop(0)
                if "Website" in nr_details:
                    nr_details.pop()
                if len(nr_details) == 3: 
                    employer = nr_details[0]
                    employer_street = nr_details[1]
                    employer_zipcode = nr_details[2].split()[0].strip()
                    employer_town = nr_details[2].split()[1].strip()
                if len(nr_details) == 4:
                    employer = nr_details[0]+" "+nr_details[1]
                    employer_street = nr_details[2]
                    employer_zipcode = nr_details[3].split()[0].strip()
                    employer_town = nr_details[3].split()[1].strip()


        full_text = soup3.find("div", {"class" :"single-jobad__content container container--boxed"}).text
        if "praxis" in full_text.lower():
            praxis = "Ja"
        else:
            praxis = ""

        travel_dist = ""
        for city, zip_list in zipcodes.items():
            if employer_zipcode in zip_list:
                travel_dist = city
                break

        result = {id: {
                "Arbeitgeber": employer,
                "Adresse": employer_street,
                "Adresse extra": "",
                "Postzahl": employer_zipcode,
                "Ort": employer_town,
                "Kanton": "",
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
                "Aktiv": "Ja"}}

    except Exception as e:
        print("Exception place 4:", e, webpage, "\n",traceback.print_exc())

    return result



def physio_swiss(zipcodes):
    print("Physio Swiss checken...")
    results = {}
    threaded_start = time.time()
    try:
        for i in range(1,30):
            print(i)
            req = requests.get("https://physioswiss.ch/stelleninserate/?_paged=" + str(i))
            # with open("physioswiss_main.html", "w", encoding="utf-8") as file:
            #     file.write(req.text)
            # with open("physioswiss_main.html", "r", encoding="utf-8") as file:
            #     soup = BeautifulSoup(file, 'html.parser')
            soup = BeautifulSoup(req.text, "html.parser")
            soup2 = soup.find("div", {"class": "wk-block-jobads-archive__grid facetwp-template"})
            jobs = soup2.find_all("a")

            # thread(jobs[0],zipcodes)
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
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        for key, value in result.items():
                            if key not in results:
                                results[key] = value
                    except Exception as e:
                        print("Exception place 3:", e)
    except Exception as e:
        print("Exception place 1:", e, "\n",traceback.print_exc())

    print(f"End Physio Swiss\n Time for completion:{time.time() - threaded_start}\n")

    return results