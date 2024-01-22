
import pickle
import pandas as pd
import googlesearch
import gspread
import requests
import os
import sys
import traceback

from datetime import date

from dotenv import load_dotenv
from scraper_functions import physio_swiss
from helper_functions import check_zipcodes, capitalize_first_letter
from email_functions import send_error_report, send_log_report


def get_website(name, place,places_key):

    try:
        base = "https://places.googleapis.com/v1/places:searchText"
        payload = {
        "textQuery" : f'{name} {place}'
        }
        headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': places_key,
        'X-Goog-FieldMask': 'places.websiteUri'
        }
        response = requests.post(base, json=payload, headers=headers).json()
        website = response['places'][0]['websiteUri']
        print("MAPS:",website)
    except:
        search_str = f"{name} {place}"
        result = googlesearch.search(search_str, lang="de")
        website = next(result)
        print("SEARCH:",website)

    return website



def check_inactive(latest_results,databank):
    # Check if items from databank are also present in latest results
    # If not, change the status to non-active and add archive date
    print("Checking for inactive vacancies...")
    for item in databank:
        active = False
        for item2 in latest_results:
            if item["id"] == item2["id"]:
                active = True
                for key in item2:
                    if item[key] != item2[key]:
                        item[key] = item2[key]
                        print(f"Updated: {item[key]} with {item2[key]}")
                if item["Aktiv"] == "":
                    item["Aktiv"] = "Ja"
                    print(f"Set to active: {item}{item2}")
        if active == False and item["Aktiv"] == "Ja":
            print(f"Set to inactive:{item}{item2}")
            item["Aktiv"] = ""
            item["Archivierungsdatum"] = date.today().strftime("%d-%m-%Y")

    print("Check inactive vacancies completed!\n")
    return databank


def update_databank(latest_results,databank,places_key):
    # Check if items from latest results are new, and if so add them to the databank
    print("Updating the databank...")
    for item in latest_results:
        present = False
        for item2 in databank:
            if item["id"] == item2["id"]:
                present = True
        if present == False:
            item["Website Arbeitgeber"] = get_website(item["Arbeitgeber"],item["Ort"],places_key)
            databank.append(item)

    print("Databank updated!\n")
    return databank



def create_dfs(databank):
    print("Creating dataframes...")
    df_id_databank = pd.DataFrame(databank)
    df_full_databank = df_id_databank.drop(columns=['id'])
    df_full_databank["Stellenangebot online per"] = pd.to_datetime(df_full_databank["Stellenangebot online per"], format='%d-%m-%Y').dt.date

    df_latest_results_unsorted = df_full_databank[df_full_databank["Aktiv"] == "Ja"]
    df_latest_results = df_latest_results_unsorted.sort_values(by=["Stellenangebot online per", "Arbeitgeber"], ascending=[False,True])
    df_latest_results["Stellenangebot online per"] = pd.to_datetime(df_latest_results["Stellenangebot online per"], format='%Y-%m-%d').dt.strftime('%d-%m-%Y')
    df_latest_results_basel = df_latest_results[df_latest_results["<25 KM"] == "Basel"]
    df_latest_results_bern = df_latest_results[df_latest_results["<25 KM"] == "Bern"]
    df_latest_results_geneve = df_latest_results[df_latest_results["<25 KM"] == "Geneve"]
    df_latest_results_lausanne = df_latest_results[df_latest_results["<25 KM"] == "Lausanne"]
    df_latest_results_luzern = df_latest_results[df_latest_results["<25 KM"] == "Luzern"]
    df_latest_results_stgallen = df_latest_results[df_latest_results["<25 KM"] == "St. Gallen"]
    df_latest_results_zurich = df_latest_results[df_latest_results["<25 KM"] == "Zurich"]

    df_only_databank_unsorted = df_full_databank[df_full_databank["Aktiv"] == ""]
    df_only_databank = df_only_databank_unsorted.sort_values(by=["Archivierungsdatum", "Arbeitgeber"], ascending=[False,True])
    df_only_databank["Stellenangebot online per"] = pd.to_datetime(df_only_databank["Stellenangebot online per"], format='%Y-%m-%d').dt.strftime('%d-%m-%Y')
    df_only_databank_basel = df_only_databank[df_only_databank["<25 KM"] == "Basel"]
    df_only_databank_bern = df_only_databank[df_only_databank["<25 KM"] == "Bern"]
    df_only_databank_geneve = df_only_databank[df_only_databank["<25 KM"] == "Geneve"]
    df_only_databank_lausanne = df_only_databank[df_only_databank["<25 KM"] == "Lausanne"]
    df_only_databank_luzern = df_only_databank[df_only_databank["<25 KM"] == "Luzern"]
    df_only_databank_stgallen = df_only_databank[df_only_databank["<25 KM"] == "St. Gallen"]
    df_only_databank_zurich = df_only_databank[df_only_databank["<25 KM"] == "Zurich"]

    df_latest_list = [df_latest_results, df_latest_results_basel, df_latest_results_bern, df_latest_results_geneve, df_latest_results_lausanne, df_latest_results_luzern, df_latest_results_stgallen, df_latest_results_zurich]
    df_only_databank_list = [df_only_databank, df_only_databank_basel, df_only_databank_bern, df_only_databank_geneve, df_only_databank_lausanne, df_only_databank_luzern, df_only_databank_stgallen, df_only_databank_zurich]

    print("Dataframes creation completed!\n")
    return df_latest_list, df_only_databank_list



def store_local(databank,latest_results,df_latest_list,df_only_databank_list,formatted_date,version):
    print("Storing all data locally...")
    with pd.ExcelWriter(f"data/Physioswiss Stellen Aktiv {formatted_date}-{version}.xlsx") as writer:
        df_latest_list[0].to_excel(writer, sheet_name="Alle aktiven Stellen", index=False)
        df_latest_list[1].to_excel(writer, sheet_name="Basel", index=False)
        df_latest_list[2].to_excel(writer, sheet_name="Bern", index=False)
        df_latest_list[3].to_excel(writer, sheet_name="Geneve", index=False)
        df_latest_list[4].to_excel(writer, sheet_name="Lausanne", index=False)
        df_latest_list[5].to_excel(writer, sheet_name="Luzern", index=False)
        df_latest_list[6].to_excel(writer, sheet_name="St. Gallen", index=False)
        df_latest_list[7].to_excel(writer, sheet_name="Zurich", index=False)

    with pd.ExcelWriter(f"data/Physioswiss Stellen Historisch {formatted_date}-{version}.xlsx") as writer:
        df_only_databank_list[0].to_excel(writer, sheet_name="Alle historischen Stellen", index=False)
        df_only_databank_list[1].to_excel(writer, sheet_name="Basel", index=False)
        df_only_databank_list[2].to_excel(writer, sheet_name="Bern", index=False)
        df_only_databank_list[3].to_excel(writer, sheet_name="Geneve", index=False)
        df_only_databank_list[4].to_excel(writer, sheet_name="Lausanne", index=False)
        df_only_databank_list[5].to_excel(writer, sheet_name="Luzern", index=False)
        df_only_databank_list[6].to_excel(writer, sheet_name="St. Gallen", index=False)
        df_only_databank_list[7].to_excel(writer, sheet_name="Zurich", index=False)


    # Storing the databank in a active file, and a history file
    with open(f"data/databank-{version}.pkl", "wb") as f:
        pickle.dump(databank,f)
    with open(f"data/databank {formatted_date}-{version}.pkl", "wb") as f:
        pickle.dump(databank,f)

    # # Storing the latest_results in a active file, and a history file
    with open(f"data/latest_results {formatted_date}-{version}.pkl", "wb") as f:
        pickle.dump(latest_results,f)

    print("Local storing complete!\n")



def update_company_file(gc,databank):
    print("Updating company file...")
    file_name = "Physio Arbeitgebers"
    spreadsheet = gc.open(file_name).sheet1
    data = spreadsheet.get_all_values()
    current_df = pd.DataFrame(data[1:], columns=data[0])
    new_rows = []
    for item in databank:
        if current_df["Arbeitgeber"].isin([item["Arbeitgeber"]]).any():
            x = 1
            # print("OLD:",item["Arbeitgeber"])
        else:
            print("NEW:",item["Arbeitgeber"])
            new_row = pd.DataFrame({
                "Arbeitgeber": [item["Arbeitgeber"]],
                "Mail geschickt": "",
                "Telefonisch Kontakt gehabt": "",
                "Voorbijgaan moment": "",
                "Voorbij geweest": "",
                "Formular ausgefüllt": "",
                "Adresse": [item["Adresse"]],
                "Adresse extra": [item["Adresse extra"]],
                "Postzahl": [item["Postzahl"]],
                "Ort": [item["Ort"]],
                "Kanton": [item["Kanton"]],
                "<25 KM": [item["<25 KM"]],
                "Praxis": [item["Praxis"]],
                "Kontakt": [item["Kontakt"]],
                "Hauptnummer": [item["Hauptnummer"]],
                "Telefon Direkt": [item["Telefon Direkt"]],
                "E-mail": [item["E-mail"]],
                "Website Arbeitgeber": [item["Website Arbeitgeber"]]
            })

            new_rows.append(new_row)
    current_df = pd.concat([current_df] + new_rows, ignore_index=True)
    current_df.sort_values(by="Arbeitgeber", inplace=True)
    current_df_list = [current_df.columns.values.tolist()] + current_df.values.tolist()
    spreadsheet.batch_update([{'values':current_df_list,'range':""}])
    # spreadsheet.update(values=current_df_list,range_name="")
    # spreadsheet.update([{'values':current_df_list,'range':""}])
    print("Company file updated!\n")


def upload_stellen_files(gc,file_name,df_list):

    print(f"Uploading the {file_name} file")
    wb = gc.open(file_name)

    if "Aktiv" in file_name:
        main = "Alle aktiven Stellen"
    elif "Historisch" in file_name:
        main = "Alle historischen Stellen"
    worksheets = [main, "Basel", "Bern", "Geneve", "Lausanne", "Luzern", "St. Gallen", "Zurich"]
    ws_list = [wb.worksheet(ws) for ws in worksheets]

    for ws in ws_list:
        ws.clear()

    for i, ws in enumerate(ws_list):
        df = df_list[i]
        data = [df.columns.values.tolist()] + df.values.tolist()
        ws.batch_update([{'values': data, 'range': ""}])

    print(f"Upload of {file_name} file completed!\n")


def update_google_sheets(databank,df_latest_list,df_only_databank_list,sheets_key):
    print("Updating google sheets\n")
    gc = gspread.service_account(sheets_key)
    update_company_file(gc,databank)
    upload_stellen_files(gc,"Physioswiss Stellen Aktiv",df_latest_list)
    upload_stellen_files(gc,"Physioswiss Stellen Historisch",df_only_databank_list)
    print("All sheets up to date!\n")



def command_input():
    load_dotenv()
    if len(sys.argv) == 4:
        if sys.argv[1] == "test":
            version = "test"
            sheets_key = "testuser-key.json"
            places_key = os.getenv("API_KEY_TEST")
        elif sys.argv[1] == "prod":
            version = "prod"
            sheets_key = "produser-key.json"
            places_key = os.getenv("API_KEY_PROD")
        else:
            print("Please provide first argument: test or prod for the credentials.")
            sys.exit()

        if sys.argv[2].strip() == "new":
            zipcodes = check_zipcodes()
            latest_results = physio_swiss(zipcodes)
        elif len(sys.argv[2]) == 8:
            with open(f"data/latest_results {sys.argv[2]}-{version}.pkl", "rb") as f:
                latest_results = pickle.load(f)
        else:
            print("Please provide argument: new or date(DDMMYYYY)")
            sys.exit()

        if sys.argv[3].strip() == "log":
            log = True
        elif sys.argv[3].strip() == "no-log":
            log = False
        else:
            print("Please provide argument: log or no-log")
            sys.exit()
    else:
        print("Please use first argument test or prod for the credentials, second argument new or date in format DDMMYYYY, and third argument log or no-log for sending log by email.\nFormat must be of: script.py (test or prod) (new/date(DDMMYYYY)) (log/no-log)")
        sys.exit()

    return version, sheets_key, places_key, latest_results, log


def main():

    try:
        formatted_date = date.today().strftime("%d%m%Y")
        print(formatted_date)

        version, sheets_key, places_key, latest_results, log = command_input()
        with open(f"data/databank-{version}.pkl", "rb") as f:
            databank = pickle.load(f)

        updated_databank = update_databank(latest_results,checked_databank,places_key)
        checked_databank = check_inactive(latest_results,databank)

            
        df_latest_list, df_only_databank_list = create_dfs(updated_databank)
        store_local(updated_databank,latest_results,df_latest_list,df_only_databank_list,formatted_date, version)
        update_google_sheets(updated_databank,df_latest_list,df_only_databank_list,sheets_key)

        if log == True:
            send_log_report(formatted_date)

    except Exception as e:
        log = False
        if log == True:
            send_error_report(e,traceback.format_exc())
        else:
            print(e,traceback.print_exc())

if __name__ == "__main__":
    main()
