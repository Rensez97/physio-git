import requests
import pickle
import time
import pandas as pd
import concurrent.futures
import traceback

from datetime import date
from bs4 import BeautifulSoup


def thread(job):
    error_list = []
    try:
        webpage = "https://www.physioswiss.ch"+job["href"]
        job_page = requests.get(webpage)
        # time.sleep(5)
        soup3 = BeautifulSoup(job_page.text, "html.parser")

        title = soup3.find("h1", {"property": "title"}).text
        grey_tags = soup3.find("div", {"class": "tags grey"})
        tags = grey_tags.find_all("span", {"class": "tag"})

        id = tags[0].text.strip()
        percent = " ".join(tags[1].text.strip().split())
        loc = tags[2].text.strip()
        company = tags[3].text.strip()
        posted = tags[4].text.strip().split(":  ")[1]

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

        if len(contact_boxes) == 3:
            text_paragraph2 = contact_boxes[1].find("p", {"class": "fine-print"}).text.split("\n")[1:]
            work_street = text_paragraph2[0].strip()
            work_zipcode = text_paragraph2[1].strip()
            work_kanton = text_paragraph2[2].split(":")[1].strip()
        else:
            work_street = ""
            work_zipcode = ""
            work_kanton = ""

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
        

        result = {"Arbeitgeber": employer,
                "Adres": employer_street,
                "Adres extra": employer_extra,
                "Postzahl": employer_zipcode,
                "Ort": employer_town,
                "Kanton": employer_kanton,
                "Omschrijving vacature": title,
                "Arbeitspensum": percent,
                "Stellenantrit per": start,
                "Anstellungsverhältnis": duration,
                "Vacature online per": posted, 
                "Contactpersoon": name,
                "Hauptnummer": main_phone,
                "Telefon Direkt": direct_phone,
                "E-mail": email,
                "Website arbeitgeber": "",
                "Link naar vacature": webpage}

        # result = {"Company ID": id, 
        #         "Title": title,
        #         "Percentage": percent,
        #         "Start": start,
        #         "Job type": job_type,
        #         "Focus-area": speciality,
        #         "Employment type": duration,
        #         "Location": loc, "Company": company,
        #         "Date posted": posted, 
        #         "Contact name": name,
        #         "Main phone": main_phone,
        #         "Direct phone": direct_phone,
        #         "E-mail": email,
        #         "Employer": employer,
        #         "Employer street": employer_street,
        #         "Employer_extra": employer_extra,
        #         "Employer_zipcode": employer_zipcode,
        #         "Employer kanton": employer_kanton,
        #         "Workplace street": work_street,
        #         "Workplace zipcode": work_zipcode,
        #         "Workplace kanton": work_kanton,
        #         "Webpage": webpage}
    except Exception as e:
        print("Exception place 4:", e, webpage, "\n",traceback.print_exc())
        if soup3:
            print(soup3)

    return result



def physio_swiss():
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
                        futures.append(executor.submit(thread, job))
                    except Exception as e:
                        print("Exception place 2:", e)
                        error_list.append(e)
                # for future in concurrent.futures.as_completed(futures):
                #     try:
                #         result = future.result()
                #         results.append(result)
                #     except Exception as e:
                #         print("Exception:", e)
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

    # df = pd.DataFrame(results)

    print("End Physio Swiss\n Time for completion:", time.time() - threaded_start)

    return results



def main():
    latest_results = physio_swiss()
    # with open("active.pkl", "rb") as f:
    #     latest_results = pickle.load(f)

    with open("databank2.pkl", "rb") as f:
        databank = pickle.load(f)

    if len(databank) == 0:
        only_databank = []
    else:
        only_databank = [item for item in databank if item not in latest_results]
        print("Only databank\n:",only_databank)


    # Creating the Excel file
    df_latest_results = pd.DataFrame(latest_results)
    df_databank = pd.DataFrame(only_databank)
    with pd.ExcelWriter("PhysioSwiss vacatures.xlsx") as writer:
        df_latest_results.to_excel(writer, sheet_name="Actuele Vacatures PhysioSwiss", index=False)
        df_databank.to_excel(writer, sheet_name="Databank PhysioSwiss", index=False)


    # Creating the main data files
    with open("active.pkl", "wb") as f:
        pickle.dump(latest_results,f)
    with open("databank.pkl", "wb") as f:
        pickle.dump(only_databank,f)

    print("l")
    # Creating the extra data files to be able to traceback
    formatted_date = date.today().strftime("%Y%m%d")
    with open("active"+formatted_date+".pkl", "wb") as f:
        pickle.dump(latest_results,f)

    with open("databank"+formatted_date+".pkl", "wb") as f:
        pickle.dump(only_databank,f)


if __name__ == "__main__":
    main()









# def physio_swiss():
#     print("Physio Swiss checken...")
#     results = []
#     threaded_start = time.time()
#     with requests.Session() as session:
#         try:
#             for i in range(1,30):
#                 req = session.get("https://www.physioswiss.ch/de/jobs?page=" + str(i))
#                 soup = BeautifulSoup(req.text, "html.parser")
#                 soup2 = soup.find("div", {"id": "results"})
#                 jobs = soup2.find_all("a", {"class": "result-preview"})

#                 if len(jobs) == 0:
#                     break

#                 for job in jobs:
#                     try:
#                         webpage = "https://www.physioswiss.ch"+job["href"]
#                         job_page = session.get(webpage)
#                         soup3 = BeautifulSoup(job_page.text, "html.parser")

#                         title = soup3.find("h1", {"property": "title"}).text
#                         grey_tags = soup3.find("div", {"class": "tags grey"})
#                         tags = grey_tags.find_all("span", {"class": "tag"})

#                         id = tags[0].text.strip()
#                         percent = " ".join(tags[1].text.strip().split())
#                         loc = tags[2].text.strip()
#                         company = tags[3].text.strip()
#                         result = {"id": id, "title": title, "percent": percent, "loc": loc, "company": company, "Webpage": webpage, "timestamp": datetime.datetime.now()}
#                         results.append(result)

#                     except Exception as e:
#                         print("Exception:", e)
#         except Exception as e:
#             print("Exception:", e)
            
#     df = pd.DataFrame(results)
#     print("End Physio Swiss\n Threaded time normal:", time.time() - threaded_start)

#     return df
