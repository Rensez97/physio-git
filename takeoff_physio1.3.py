import pickle
import pandas as pd
import gspread
import os
import sys
import traceback
import time

from datetime import date

from dotenv import load_dotenv
from scraper_functions import physio_swiss
from helper_functions import check_zipcodes, get_website, inactivity_validation
from email_functions import send_error_report, send_log_report



def check_inactive(latest_results,databank):
    # Check if items from databank are also present in latest results
    # If not, change the status to non-active and add archive date
    print("Checking for inactive vacancies...")
    for key in databank:
        if key in latest_results:
            if databank[key]["Aktiv"] == "":
                databank[key]["Aktiv"] = "Ja"
        else:
            if databank[key]["Archivierungsdatum"] == "":
                val = inactivity_validation(databank[key]['Link'])
                if val == True:
                    databank[key]["Aktiv"] = ""
                    databank[key]["Archivierungsdatum"] = date.today().strftime("%d-%m-%Y")

    print("Check inactive vacancies completed!\n")
    return databank



def update_databank(latest_results,databank,places_key):
    # Check if items from latest results are new, and if so add them to the databank
    print("Updating the databank...")
    for key, value in latest_results.items():
        if key not in databank:
            databank[key] = value
            databank[key]["Website Arbeitgeber"] = get_website(databank[key]["Arbeitgeber"],databank[key]["Ort"],places_key)

    print("Databank updated!\n")
    return databank



def create_dfs(databank):
    print("Creating dataframes...")
    df_full_databank = pd.DataFrame.from_dict(databank, orient='index')
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



def store_local(databank,latest_results,df_latest_list,df_only_databank_list,formatted_date,version, filepath):
    print("Storing all data locally...")
    with pd.ExcelWriter(f"{filepath}data/Physioswiss Stellen Aktiv {formatted_date}-{version}.xlsx") as writer:
        df_latest_list[0].to_excel(writer, sheet_name="Alle aktiven Stellen", index=False)
        df_latest_list[1].to_excel(writer, sheet_name="Basel", index=False)
        df_latest_list[2].to_excel(writer, sheet_name="Bern", index=False)
        df_latest_list[3].to_excel(writer, sheet_name="Geneve", index=False)
        df_latest_list[4].to_excel(writer, sheet_name="Lausanne", index=False)
        df_latest_list[5].to_excel(writer, sheet_name="Luzern", index=False)
        df_latest_list[6].to_excel(writer, sheet_name="St. Gallen", index=False)
        df_latest_list[7].to_excel(writer, sheet_name="Zurich", index=False)

    with pd.ExcelWriter(f"{filepath}data/Physioswiss Stellen Historisch {formatted_date}-{version}.xlsx") as writer:
        df_only_databank_list[0].to_excel(writer, sheet_name="Alle historischen Stellen", index=False)
        df_only_databank_list[1].to_excel(writer, sheet_name="Basel", index=False)
        df_only_databank_list[2].to_excel(writer, sheet_name="Bern", index=False)
        df_only_databank_list[3].to_excel(writer, sheet_name="Geneve", index=False)
        df_only_databank_list[4].to_excel(writer, sheet_name="Lausanne", index=False)
        df_only_databank_list[5].to_excel(writer, sheet_name="Luzern", index=False)
        df_only_databank_list[6].to_excel(writer, sheet_name="St. Gallen", index=False)
        df_only_databank_list[7].to_excel(writer, sheet_name="Zurich", index=False)


    # Storing the databank in a active file, and a history file
    with open(f"{filepath}data/databank-{version}.pkl", "wb") as f:
        pickle.dump(databank,f)
    with open(f"{filepath}data/databank {formatted_date}-{version}.pkl", "wb") as f:
        pickle.dump(databank,f)

    # # Storing the latest_results in a active file, and a history file
    with open(f"{filepath}data/latest_results {formatted_date}-{version}.pkl", "wb") as f:
        pickle.dump(latest_results,f)

    print("Local storing complete!\n")



def update_company_file(gc,databank):
    print("Updating company file...")
    file_name = "Physio Arbeitgebers"
    spreadsheet = gc.open(file_name).sheet1
    data = spreadsheet.get_all_values()
    current_df = pd.DataFrame(data[1:], columns=data[0])
    new_rows = []
    for value in databank.values():
        if value['Arbeitgeber'] not in current_df['Arbeitgeber'].values:
            new_row = pd.DataFrame({
                "Arbeitgeber": [value["Arbeitgeber"]],
                "Schon jemand vermittelt": "",
                "Jemand vorbei gewesen": "",
                "Mail geschickt": "",
                "LinkedIn hinzugefügt": "",
                "Telefonisch Kontakt gehabt": "",
                "Voorbijgaan moment": "",
                "Voorbij geweest": "",
                "Formular ausgefüllt": "",
                "Adresse": [value["Adresse"]],
                "Adresse extra": [value["Adresse extra"]],
                "Postzahl": [value["Postzahl"]],
                "Ort": [value["Ort"]],
                "Kanton": [value["Kanton"]],
                "<25 KM": [value["<25 KM"]],
                "Praxis": [value["Praxis"]],
                "Kontakt": [value["Kontakt"]],
                "Hauptnummer": [value["Hauptnummer"]],
                "Telefon Direkt": [value["Telefon Direkt"]],
                "E-mail": [value["E-mail"]],
                "Website Arbeitgeber": [value["Website Arbeitgeber"]]
            })

            new_rows.append(new_row)
    current_df = pd.concat([current_df] + new_rows, ignore_index=True)
    current_df.sort_values(by="Arbeitgeber", inplace=True)
    current_df_list = [current_df.columns.values.tolist()] + current_df.values.tolist()
    spreadsheet.batch_update([{'values':current_df_list,'range':""}])

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

    for i, ws in enumerate(ws_list):
        current_data = ws.get_all_values()
        if len(current_data) > 0:
            cell_range = 'A2:' + gspread.utils.rowcol_to_a1(len(current_data) + 1, len(current_data[0]))
        else:
            cell_range = ''

        ws.batch_clear([cell_range])

        df = df_list[i]
        data = df.values.tolist()

        if len(data) > 0:
            cell_range = 'A2:' + gspread.utils.rowcol_to_a1(len(data) + 1, len(data[0]))
        else:
            cell_range = ''

        data = [df.columns.values.tolist()] + data
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
    if len(sys.argv) == 5:
        if sys.argv[1].strip() == "server":
            filepath = "/home/physio-git/"
        elif sys.argv[1].strip() == "local":
            filepath = ""
        else:
            print("Please provide argument: server or local for filepath to use")
            sys.exit()

        if sys.argv[2] == "test":
            version = "test"
            sheets_key = f"{filepath}testuser-key.json"
            places_key = os.getenv("API_KEY_TEST")
        elif sys.argv[2] == "prod":
            version = "prod"
            sheets_key = f"{filepath}produser-key.json"
            places_key = os.getenv("API_KEY_PROD")
        else:
            print("Please provide argument: test or prod for the credentials.")
            sys.exit()

        if sys.argv[3].strip() == "new":
            scrape = "new"
        elif len(sys.argv[3]) == 8:
            scrape = sys.argv[3]
        else:
            print("Please provide argument: new or date(DDMMYYYY)")
            sys.exit()

        if sys.argv[4].strip() == "log":
            log = True
        elif sys.argv[4].strip() == "no-log":
            log = False
        else:
            print("Please provide argument: log or no-log")
            sys.exit()
    else:
        print("Please use first argument server or local for file path to use, second argument test or prod for the credentials, third argument new or date in format DDMMYYYY, and fourth argument log or no-log for sending log by email.\nFormat must be of: script.py (server/local) (test or prod) (new/date(DDMMYYYY)) (log/no-log)")
        sys.exit()

    return version, sheets_key, places_key, scrape, filepath, log


def main():

    try:
        version, sheets_key, places_key, scrape, filepath, log = command_input()

        if log == True:
            log_file = open("/home/takeoff_physio_log.log", 'a')
            sys.stdout = log_file

        formatted_date = date.today().strftime("%d%m%Y")
        print(formatted_date)


        if scrape == "new":
            zipcodes = check_zipcodes()
            latest_results = physio_swiss(zipcodes)
        else:
            with open(f"{filepath}data/latest_results {sys.argv[3]}-{version}.pkl", "rb") as f:
                latest_results = pickle.load(f)

        with open(f"{filepath}data/databank-{version}.pkl", "rb") as f:
            databank = pickle.load(f)

        checked_databank = check_inactive(latest_results,databank)
        updated_databank = update_databank(latest_results,checked_databank,places_key)

        databank = updated_databank

        df_latest_list, df_only_databank_list = create_dfs(databank)
        store_local(databank,latest_results,df_latest_list,df_only_databank_list,formatted_date, version, filepath)
        update_google_sheets(databank,df_latest_list,df_only_databank_list,sheets_key)

        if log == True:
            print("Email will be send with log report")
            log_file.close()
            send_log_report(formatted_date)

    except Exception as e:
        if log == True:
            print("E-mail will be send with error")
            log_file.close()
            send_error_report(e,traceback.format_exc())
        else:
            print(e,traceback.print_exc())

if __name__ == "__main__":
    main()
