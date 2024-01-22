
import pickle
import pandas as pd
import googlesearch
import gspread
import requests
import os
import sys

from datetime import date

from scraper_functions import physio_swiss
from helper_functions import check_zipcodes
from dotenv import load_dotenv


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



def check_inactive(latest_results,databank,formatted_date):
    # Check if items from databank are also present in latest results
    # If not, change the status to non-active and add archive date
    print("Checking for inactive vacancies...")
    for item in databank:
        active = False
        for item2 in latest_results:
            if item["Arbeitgeber"] == item2["Arbeitgeber"] and item["Stellenbeschreibung"] == item2["Stellenbeschreibung"] and item["Stellenangebot online per"] == item2["Stellenangebot online per"]:
                active = True
        if active == False and item["Aktiv"] == "Ja":
            item["Aktiv"] = ""
            item["Archivierungsdatum"] = formatted_date

    print("Check inactive vacancies completed!\n")
    return databank


def update_databank(latest_results,databank,places_key):
    # Check if items from latest results are new, and if so add them to the databank
    print("Updating the databank...")
    for item in latest_results:
        present = False
        for item2 in databank:
            if item["Arbeitgeber"] == item2["Arbeitgeber"] and item["Stellenbeschreibung"] == item2["Stellenbeschreibung"] and item["Stellenangebot online per"] == item2["Stellenangebot online per"]:
                present = True
        if present == False:
            item["Website Arbeitgeber"] = get_website(item["Arbeitgeber"],item["Ort"],places_key)
            databank.append(item)

    print("Databank updated!\n")
    return databank



def create_dfs(databank):
    print("Creating dataframes...")
    df_full_databank = pd.DataFrame(databank)
    df_full_databank["Stellenangebot online per"] = pd.to_datetime(df_full_databank["Stellenangebot online per"], format='%d-%m-%Y').dt.date

    df_latest_results_unsorted = df_full_databank[df_full_databank["Aktiv"] == "Ja"]
    df_latest_results = df_latest_results_unsorted.sort_values(by=["Stellenangebot online per", "Arbeitgeber"], ascending=[False,True])
    df_latest_results["Stellenangebot online per"] = pd.to_datetime(df_latest_results["Stellenangebot online per"], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
    df_latest_results_basel = df_latest_results[df_latest_results["<25 KM"] == "Basel"]
    df_latest_results_bern = df_latest_results[df_latest_results["<25 KM"] == "Bern"]
    df_latest_results_geneve = df_latest_results[df_latest_results["<25 KM"] == "Geneve"]
    df_latest_results_lausanne = df_latest_results[df_latest_results["<25 KM"] == "Lausanne"]
    df_latest_results_luzern = df_latest_results[df_latest_results["<25 KM"] == "Luzern"]
    df_latest_results_stgallen = df_latest_results[df_latest_results["<25 KM"] == "St. Gallen"]
    df_latest_results_zurich = df_latest_results[df_latest_results["<25 KM"] == "Zurich"]

    df_only_databank_unsorted = df_full_databank[df_full_databank["Aktiv"] == ""]
    df_only_databank = df_only_databank_unsorted.sort_values(by=["Archivierungsdatum", "Arbeitgeber"], ascending=[False,True])
    df_only_databank["Stellenangebot online per"] = pd.to_datetime(df_only_databank["Stellenangebot online per"], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
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



def store_local(databank,latest_results,df_latest_list,df_only_databank_list,formatted_date):
    print("Storing all data locally...")
    with pd.ExcelWriter(f"data/Physioswiss Stellen Aktiv {formatted_date}.xlsx") as writer:
        df_latest_list[0].to_excel(writer, sheet_name="Alle aktiven Stellen", index=False)
        df_latest_list[1].to_excel(writer, sheet_name="Basel", index=False)
        df_latest_list[2].to_excel(writer, sheet_name="Bern", index=False)
        df_latest_list[3].to_excel(writer, sheet_name="Geneve", index=False)
        df_latest_list[4].to_excel(writer, sheet_name="Lausanne", index=False)
        df_latest_list[5].to_excel(writer, sheet_name="Luzern", index=False)
        df_latest_list[6].to_excel(writer, sheet_name="St. Gallen", index=False)
        df_latest_list[7].to_excel(writer, sheet_name="Zurich", index=False)

    with pd.ExcelWriter(f"data/Physioswiss Stellen Historisch {formatted_date}.xlsx") as writer:
        df_only_databank_list[0].to_excel(writer, sheet_name="Alle historischen Stellen", index=False)
        df_only_databank_list[1].to_excel(writer, sheet_name="Basel", index=False)
        df_only_databank_list[2].to_excel(writer, sheet_name="Bern", index=False)
        df_only_databank_list[3].to_excel(writer, sheet_name="Geneve", index=False)
        df_only_databank_list[4].to_excel(writer, sheet_name="Lausanne", index=False)
        df_only_databank_list[5].to_excel(writer, sheet_name="Luzern", index=False)
        df_only_databank_list[6].to_excel(writer, sheet_name="St. Gallen", index=False)
        df_only_databank_list[7].to_excel(writer, sheet_name="Zurich", index=False)


    # Storing the databank in a active file, and a history file
    with open("data/databank.pkl", "wb") as f:
        pickle.dump(databank,f)
    with open("data/databank "+formatted_date+".pkl", "wb") as f:
        pickle.dump(databank,f)

    # # Storing the latest_results in a active file, and a history file
    with open("data/latest_results "+formatted_date+".pkl", "wb") as f:
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
    if "Aktiv" in file_name:
        main = gc.open(file_name).worksheet("Alle aktiven Stellen")
    elif "Historisch" in file_name:
        main = gc.open(file_name).worksheet("Alle historischen Stellen")
    basel = gc.open(file_name).worksheet("Basel")
    bern = gc.open(file_name).worksheet("Bern")
    geneve = gc.open(file_name).worksheet("Geneve")
    lausanne = gc.open(file_name).worksheet("Lausanne")
    luzern = gc.open(file_name).worksheet("Luzern")
    stgallen = gc.open(file_name).worksheet("St. Gallen")
    zurich = gc.open(file_name).worksheet("Zurich")

    main.clear()
    basel.clear()
    bern.clear()
    geneve.clear()
    lausanne.clear()
    luzern.clear()
    stgallen.clear()
    zurich.clear()

    main.batch_update([{'values':[df_list[0].columns.values.tolist()] + df_list[0].values.tolist(),'range':""}])
    basel.batch_update([{'values':[df_list[1].columns.values.tolist()] + df_list[1].values.tolist(),'range':""}])
    bern.batch_update([{'values':[df_list[2].columns.values.tolist()] + df_list[2].values.tolist(),'range':""}])
    geneve.batch_update([{'values':[df_list[3].columns.values.tolist()] + df_list[3].values.tolist(),'range':""}])
    lausanne.batch_update([{'values':[df_list[4].columns.values.tolist()] + df_list[4].values.tolist(),'range':""}])
    luzern.batch_update([{'values':[df_list[5].columns.values.tolist()] + df_list[5].values.tolist(),'range':""}])
    stgallen.batch_update([{'values':[df_list[6].columns.values.tolist()] + df_list[6].values.tolist(),'range':""}])
    zurich.batch_update([{'values':[df_list[7].columns.values.tolist()] + df_list[7].values.tolist(),'range':""}])
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
    if len(sys.argv) == 3:
        if sys.argv[1] == "test":
            sheets_key = "testuser-key.json"
            places_key = os.getenv("API_KEY_TEST")
        elif sys.argv[1] == "prod":
            sheets_key = "produser-key.json"
            places_key = os.getenv("API_KEY_PROD")
        else:
            print("Please provide first argument: test or prod for the credentials.")
            sys.exit()

        if sys.argv[2].strip() == "new":
            zipcodes = check_zipcodes()
            latest_results = physio_swiss(zipcodes)
        elif len(sys.argv[2]) == 8:
            with open(f"data/latest_results {sys.argv[2]}.pkl", "rb") as f:
                latest_results = pickle.load(f)
        else:
            print("Please provide argument: new or date(DDMMYYYY)")
            sys.exit()
    else:
        print("Please use first argument test or prod for the credentials, and second argument new or date in format DDMMYYYY.\nFormat must be of: script.py (test or prod) (new/date(DDMMYYYY))")
        sys.exit()

    return sheets_key, places_key, latest_results


def main():
    formatted_date = date.today().strftime("%d%m%Y")
    print(formatted_date)

    sheets_key, places_key, latest_results = command_input()

    with open("data/databank.pkl", "rb") as f:
        databank = pickle.load(f)

    checked_databank = check_inactive(latest_results,databank,formatted_date)
    updated_databank = update_databank(latest_results,checked_databank,places_key)
        
    df_latest_list, df_only_databank_list = create_dfs(updated_databank)
    store_local(updated_databank,latest_results,df_latest_list,df_only_databank_list,formatted_date)
    update_google_sheets(updated_databank,df_latest_list,df_only_databank_list,sheets_key)

if __name__ == "__main__":
    main()
