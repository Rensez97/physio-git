import requests
import time
import concurrent.futures
import traceback


from bs4 import BeautifulSoup
from helper_functions import retry_link, convert_german_date, capitalize_first_letter


def thread(job,zipcodes):
    error_list = []
    try:
        webpage = "https://www.physioswiss.ch"+job["href"]
        job_page = retry_link(webpage)
        soup3 = BeautifulSoup(job_page.text, "html.parser")

        id = webpage.split("/")[-2]
        title = soup3.find("h1", {"property": "title"}).text
        grey_tags = soup3.find("div", {"class": "tags grey"})
        tags = grey_tags.find_all("span", {"class": "tag"})

        percent = " ".join(tags[1].text.strip().split())
        date_posted = tags[4].text.strip().split(":  ")[1]
        posted = convert_german_date(date_posted)

        details = soup3.find("table", {"class": "details"})
        blocks = details.find_all("td")
        start = blocks[1].text.split(":")[1].strip()
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

        text_paragraph3 = contact_boxes[-1].find("p", {"class": "fine-print"}).text.split("\n")[1:-1]
        if len(text_paragraph3) == 4:
            employer = capitalize_first_letter(text_paragraph3[0].strip())
            employer_street = text_paragraph3[1].strip()
            employer_extra = ""
            employer_zipcode = text_paragraph3[2].strip().split()[0]
            employer_town = text_paragraph3[2].strip().split()[1]
            employer_kanton = text_paragraph3[3].split(":")[1].strip()
        if len(text_paragraph3) == 5:
            employer = capitalize_first_letter(text_paragraph3[0].strip())
            employer_street = text_paragraph3[1].strip()
            employer_extra = text_paragraph3[2].strip()
            employer_zipcode = text_paragraph3[3].strip().split()[0]
            employer_town = text_paragraph3[3].strip().split()[1]
            employer_kanton = text_paragraph3[4].split(":")[1].strip()
        if len(text_paragraph3) == 6:
            employer = capitalize_first_letter(text_paragraph3[0].strip())
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

        result = {"id": id,
                "Arbeitgeber": employer,
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
        print("Exception place 4:", e, webpage, "\n",traceback.print_exc())

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

    print(f"End Physio Swiss\n Time for completion:{time.time() - threaded_start}\n")

    return results