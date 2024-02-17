"""
Need to enable these two requirements:
     1. IMAP in gmail setting
     2. 2-factor authentication and app password (record it for later use)
"""

import imaplib 
import email 
import yaml
import os
import json
import base64

with open("credential.yml") as f:
     content = f.read()

credential = yaml.load(content,Loader=yaml.FullLoader)

user_mail = credential['user']
password = credential['password']

imap_url = 'imap.gmail.com'

my_mail = imaplib.IMAP4_SSL(imap_url)

# Login using credential 
my_mail.login(user_mail,password)

# Select the Inbox to fetch the email 
my_mail.select('Inbox')

# Filter out only the emails with specific key and value, example: 
key_value = '(FROM "no-reply@grab.com" SUBJECT "Your Grab E-Receipt")'
_,data = my_mail.search(None, key_value)

# _,data = my_mail.search(None,'ALL') # fetch all mails; the _ is the status of the request, which is usually OK; the data is the list of ids of mails, returned as a string 

idlist = data[0].split() # split the data string into the list of string, each is the id of an email 

# Fetch the emails
mails = []
for id in idlist:
     typ, mail = my_mail.fetch(id, '(RFC822)')
     mails.append(mail)
     print("fetched email " + id.decode('utf_8'))

# Extract the mail contents 
email_path = "../data/email"
if not os.path.exists(email_path):
    os.makedirs(email_path)


for i,mail in enumerate(mails,1):
    for response_part in mail:
        if isinstance(response_part, tuple):
            my_msg = email.message_from_bytes(response_part[1])
            for part in my_msg.walk():
                if part.get_content_type() == 'text/html':
                    content = base64.b64decode(part.get_payload()).decode('utf-8')
                        
            # Write email data to an HTML file
            file_name = f"email_{i:02d}.html"
            file_path = os.path.join(email_path, file_name)
            with open(file_path, "w",encoding='utf-8') as html_file:
                html_file.write(content)


