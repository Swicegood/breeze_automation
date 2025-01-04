from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import os
import subprocess
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from batches2csv import get_batches, contributions_with_addresses, makefilename, save, parse_range
import os
from email import encoders

SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

def get_gmail_service():
    creds = None
    if os.path.exists('./token/token.json'):
        creds = Credentials.from_authorized_user_file('./token/token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                os.remove('token.json')  # Remove the invalid token
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_console()
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_console()

        # Save the credentials for the next run
        with open('./token/token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    return service

def grab_emails(search_str):
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    service = get_gmail_service()

    # Call the Gmail API
    results = service.users().messages().list(userId='me', q=search_str, maxResults=10).execute()
    messages = results.get('messages', [])
    emailmatches = []    

    if not messages:
        print('No messages found.')
    else:
        for message in messages:
            emailmatches.append(service.users().messages().get(userId='me', id=message['id']).execute())
            emailmatches[-1]['confidence'] = 7

    return emailmatches

def unread_emails(emails):
    unread = []
    for email in emails:
        if 'UNREAD' in email['labelIds']:
            unread.append(email)
    return unread

def mark_as_read(email):
    service = get_gmail_service()
    service.users().messages().modify(userId='me', id=email['id'], body={'removeLabelIds': ['UNREAD']}).execute()

def archive_email(email):
    service = get_gmail_service()
    service.users().messages().modify(userId='me', id=email['id'], body={'removeLabelIds': ['INBOX']}).execute()

def get_args(email):
    subject_str = next(header['value'] for header in email['payload']['headers'] if header['name'] == 'Subject')
    return subject_str.split(" ")[-1]

def get_email_address(email):
    return next(header['value'] for header in email['payload']['headers'] if header['name'] == 'From')

def process_attachments(email):
    """Download attachments starting with 'PP' or 'SQ' and process them using
    papal2breeze or square2breeze. Then email the resulting CSV to jguru108@gmail.com.
    """
    service = get_gmail_service()
    payload = email['payload']
    parts = payload.get('parts', [])
    for part in parts:
        filename = part.get('filename', '')
        if filename.startswith('PP') or filename.startswith('SQ'):
            # Retrieve attachment
            att_id = part['body']['attachmentId']
            attachment_data = service.users().messages().attachments().get(
                userId='me', messageId=email['id'], id=att_id
            ).execute()
            file_data = base64.urlsafe_b64decode(attachment_data['data'])
            
            # Save attachment locally
            local_filepath = os.path.join('.', filename)
            with open(local_filepath, 'wb') as f:
                f.write(file_data)

            # Determine which script to run
            script_name = 'papal2breeze.py' if filename.startswith('PP') else 'square2breeze.py'

            # Run the script to generate a CSV
            subprocess.run(["python", script_name, local_filepath], check=True)

            # Assume script outputs a CSV with a predictable name, e.g. local_filepath + ".csv"
            csv_filepath = local_filepath + ".csv"
            if os.path.exists(csv_filepath):
                # Build and send an email with the CSV file attached
                result_message = MIMEMultipart()
                result_message['to'] = "jguru108@gmail.com"
                result_message['subject'] = f"Processed file: {filename}"
                result_message.attach(MIMEText(f"Here is the CSV generated from {filename}."))

                with open(csv_filepath, "rb") as csvfile:
                    part_csv = MIMEBase("application", "octet-stream")
                    part_csv.set_payload(csvfile.read())
                encoders.encode_base64(part_csv)
                part_csv.add_header("Content-Disposition", f"attachment; filename={os.path.basename(csv_filepath)}")
                result_message.attach(part_csv)

                raw_msg = base64.urlsafe_b64encode(result_message.as_bytes()).decode()
                service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
                print(f"Sent CSV {csv_filepath} to jguru108@gmail.com")

if __name__ == "__main__":
    email_matches = grab_emails("Need batch")
    unread = unread_emails(email_matches)
    if not unread:
        print("No unread Need batch emails")
        exit(0)
    for email in unread:
        arg_str = get_args(email)
        args = parse_range(arg_str)
        batch_data = get_batches(args)
        contributions_for_letters = contributions_with_addresses(batch_data)
        filename = makefilename(args)
        save(contributions_for_letters, filename)
        # wait for the file to be created
        while not os.path.exists(filename):
            pass
        # send the file
        message = MIMEMultipart()
        message['to'] = get_email_address(email)
        message['subject'] = filename
        message.attach(MIMEText("Here is your file"))
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )
        message.attach(part)
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        service = get_gmail_service()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        print(f"Sent {filename}")

        # Process attachments (new functionality):
        process_attachments(email)

        mark_as_read(email)
        archive_email(email)
    print("Done")







