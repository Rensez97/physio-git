import pandas as pd
import gspread
import sys

from datetime import datetime, timedelta
from email_functions import send_weekly_update


def command_input():
    if len(sys.argv) == 3:
        if sys.argv[1].strip() == "server":
            filepath = "/home/physio-git/"
        elif sys.argv[1].strip() == "local":
            filepath = ""
        else:
            print("Please provide argument: server or local for filepath to use")
            sys.exit()

        if sys.argv[2] == "test":
            sheets_key = f"{filepath}testuser-key.json"
        elif sys.argv[2] == "prod":
            sheets_key = f"{filepath}produser-key.json"
        else:
            print("Please provide argument: test or prod for the credentials.")
            sys.exit()
    else:
        print("Please use first argument server or local for file path to use, second argument test or prod for the credentials")
        sys.exit()

    return sheets_key


def main():
    sheets_key = command_input()
    gc = gspread.service_account(sheets_key)
    spreadsheet = gc.open("Physio Arbeitgebers").sheet1
    data = spreadsheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])

    df['Online per'] = pd.to_datetime(df['Online per'], format='%d-%m-%Y', errors='coerce')
    current_date = datetime.now()
    seven_days_ago = current_date - timedelta(days=7)
    last_week_date = current_date - timedelta(days=current_date.weekday() + 7)
    last_week_number = last_week_date.strftime("%U")
    filtered_df = df[(df['Online per'] >= seven_days_ago) & (df['Online per'] <= current_date)]

    print(filtered_df)

    send_weekly_update(df,last_week_number)

if __name__ == "__main__":
    main()
