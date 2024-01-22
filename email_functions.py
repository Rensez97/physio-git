import ssl
import smtplib

from email.message import EmailMessage


def setup_email(text,subject):
    message = EmailMessage()
    message.set_content(text)
    message['FROM'] = "huizzoeker@outlook.com"
    message['TO'] = ["rensevdzee@hotmail.com"]
    message['SUBJECT'] = subject
    context = ssl.create_default_context()
    with smtplib.SMTP('smtp-mail.outlook.com', 587) as smtp:
        smtp.starttls(context=context)
        smtp.login(message['FROM'], "Ludosanders")
        smtp.send_message(message)
        smtp.quit()



def send_log_report(formatted_date):
    subject = f"Log report of takeoff physio on {formatted_date}"
    with open(f"/home/takeoff_physio_log.log", "r") as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if line.strip() == formatted_date:
                text = ''.join(lines[i+1:])
                break

    setup_email(text,subject)


def send_error_report(exception,trace):
    subject = f"Error during takeoff physio"
    text = f"{exception}\n\n{trace}"

    setup_email(text,subject)