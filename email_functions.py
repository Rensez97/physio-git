import ssl
import smtplib

from email.message import EmailMessage


# email the error if there has happened something faulty
def email_error(website, error, huis):
    message = EmailMessage()
    message.set_content(str(error)+'\n'+str(huis))
    message['FROM'] = "huizzoeker@outlook.com"
    message['TO'] = ["rensevdzee@hotmail.com"]
    message['SUBJECT'] = "Error bij "+website
    context = ssl.create_default_context()
    # set up SMTP server
    with smtplib.SMTP('smtp-mail.outlook.com', 587) as smtp:
        smtp.starttls(context=context)
        smtp.login(message['FROM'], "Ludosanders")
        smtp.send_message(message)
        smtp.quit()


# email the results if there has been new houses spotted
def email_new(results, alert):
    #print(products, new_products, updated_products)
    message = EmailMessage()
    message.set_content(results)
    message['FROM'] = "huizzoeker@outlook.com"
    message['TO'] = ["rensevdzee@hotmail.com","lottejgr@aol.com"]
    if alert == 1:
        message['SUBJECT'] = "1 nieuwe osso gevonden"
    if alert > 1:
        message['SUBJECT'] = str(alert)+" nieuwe ossos gevonden"
    context = ssl.create_default_context()
    # set up SMTP server
    with smtplib.SMTP('smtp-mail.outlook.com', 587) as smtp:
        smtp.starttls(context=context)
        smtp.login(message['FROM'], "Ludosanders")
        smtp.send_message(message)
        smtp.quit()


# combine the data of the house into an appropiate sentence for the email
def write_msg(new,old):
    str_list = []
    if len(new) > 0:
        str_list.append("**Nieuw**\n")
        for item in new:
            str_list.append("{} met {} m2 voor {} {}!  {}".format(item[1],item[3], item[5], item[6], item[8]))
    if len(old) > 0:
        str_list.append("\n\n**Oud**\n")
        for item in old:
            str_list.append("{} met {} m2 voor {} {}!  {}".format(item[1],item[3], item[5], item[6], item[8]))
        # if item[0] == "(Nieuw)":
        #     str_list.append("{}Potentieel huis gevonden met {} m2 voor {} {}!  {}".format(
        #         item[0], item[1], item[2], item[3], item[4]))
        # else:
        #     str_list.append("Potentieel huis gevonden met {} m2 voor {} {}!  {}".format(
        #         item[1], item[2], item[3], item[4]))
    return "\n".join(str_list)
