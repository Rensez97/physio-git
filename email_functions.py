import ssl
import smtplib

from email.message import EmailMessage


def setup_email(text,subject):
    message = EmailMessage()
    message.set_content(text)
    message['FROM'] = "huizzoeker@gmail.com"
    message['TO'] = ["rensevdzee@hotmail.com"]
    message['SUBJECT'] = subject

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(message['FROM'], "raxakvbumsspboud")
        smtp.send_message(message)


def send_log_report(formatted_date):
    subject = f"Log report of takeoff physio on {formatted_date}"
    with open(f"/home/takeoff_physio_log.log", "r") as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if line.strip() == formatted_date:
                text = ''.join(lines[i:])
                break

    setup_email(text,subject)


def send_error_report(exception,trace):
    subject = f"Error during takeoff physio"
    text = f"{exception}\n\n{trace}"

    setup_email(text,subject)


def send_weekly_update(df,last_week_number):
    subject = f"Update neue Arbeitgeber Woche {last_week_number}"
    text = df
    
    setup_email(text,subject)