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
import logging
import datetime
import sys

SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout, # Ensure logs go to stdout for Docker capture
    force=True
)
logger = logging.getLogger('main')

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
        if 'labelIds' in email and 'UNREAD' in email['labelIds']:
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
    logging.info(f"Checking {len(parts)} parts for PP/SQ attachments")
    
    for part in parts:
        filename = part.get('filename', '')
        if filename.startswith('PP') or filename.startswith('SQ'):
            logging.info(f"Processing attachment: {filename}")
            try:
                # Retrieve attachment
                att_id = part['body']['attachmentId']
                logging.info(f"Downloading attachment ID: {att_id}")
                attachment_data = service.users().messages().attachments().get(
                    userId='me', messageId=email['id'], id=att_id
                ).execute()
                file_data = base64.urlsafe_b64decode(attachment_data['data'])
                
                # Save attachment locally
                local_filepath = os.path.join('.', filename)
                logging.info(f"Saving attachment to: {local_filepath}")
                with open(local_filepath, 'wb') as f:
                    f.write(file_data)

                # Determine which script to run
                script_name = 'papal2breeze.py' if filename.startswith('PP') else 'square2breeze.py'
                logging.info(f"Running {script_name} on {local_filepath}")

                # Add before running subprocess:
                if not os.path.exists(script_name):
                    logging.error(f"Required script {script_name} not found in {os.getcwd()}")
                    continue

                # Run the script to generate a CSV
                try:
                    return_code = run_subprocess(["python3", script_name, local_filepath])
                    
                    if return_code != 0:
                        logging.error(f"Script execution failed with return code {return_code}")
                        # Skip to the next email
                        continue

                except FileNotFoundError:
                    logging.error(f"Script {script_name} not found in current directory")
                    continue

                # Handle the CSV
                csv_filepath = '.'.join(local_filepath.split(".")[:-1])+"_giving_ready_for_breeze.csv" 
                if os.path.exists(csv_filepath):
                    logging.info(f"Found generated CSV: {csv_filepath}")
                    
                    # Build and send an email with the CSV file
                    logging.info(f"Preparing to email CSV to jguru108@gmail.com")
                    result_message = MIMEMultipart()
                    result_message['to'] = "jguru108@gmail.com"
                    result_message['subject'] = f"Processed file: {filename}"
                    result_message.attach(MIMEText(f"Here is the CSV generated from {filename}."))

                    with open(csv_filepath, "rb") as csvfile:
                        part_csv = MIMEBase("application", "octet-stream")
                        part_csv.set_payload(csvfile.read())
                    encoders.encode_base64(part_csv)
                    part_csv.add_header("Content-Disposition", 
                                      f"attachment; filename={os.path.basename(csv_filepath).replace('PP', 'Paypal').replace('SQ', 'Square')}")
                    result_message.attach(part_csv)

                    raw_msg = base64.urlsafe_b64encode(result_message.as_bytes()).decode()
                    service.users().messages().send(userId="me", 
                                                  body={"raw": raw_msg}).execute()
                    logging.info(f"Successfully sent CSV {csv_filepath} to jguru108@gmail.com")
                else:
                    logging.error(f"Expected CSV file not found: {csv_filepath}")
                    
            except Exception as e:
                logging.error(f"Error processing attachment {filename}: {str(e)}", 
                            exc_info=True)
                continue

def grab_emails_with_attachment(prefixes):
    """Return all emails that have attachments whose filenames match one
    of the given prefixes (e.g. PP... or SQ...).
    """
    service = get_gmail_service()
    # Build a query for attachments. Example: has:attachment (filename:PP* OR filename:SQ*)
    query_parts = []
    for p in prefixes:
        query_parts.append(f"filename:{p}*")
    query_str = "has:attachment (" + " OR ".join(query_parts) + ")"

    results = service.users().messages().list(
        userId='me', q=query_str, maxResults=10
    ).execute()
    messages = results.get('messages', [])
    emailmatches = []

    if messages:
        for message in messages:
            detailed_msg = service.users().messages().get(userId='me', id=message['id']).execute()
            detailed_msg['confidence'] = 7
            emailmatches.append(detailed_msg)

    return emailmatches

def run_subprocess(cmd, env=None):
    """Run a subprocess and stream output in real-time to logger"""
    logger.info(f"Starting subprocess: {' '.join(cmd)}")
    
    # Start process with pipe for stdout and stderr
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # Line buffered
        env=env
    )
    
    # Function to handle output from the subprocess
    def log_output(pipe, log_level):
        for line in iter(pipe.readline, ''):
            if line:
                if log_level == 'info':
                    logger.info(f"Subprocess: {line.strip()}")
                else:
                    logger.error(f"Subprocess error: {line.strip()}")
    
    # Import threading to handle concurrent reading of stdout and stderr
    import threading
    
    # Create and start threads to handle output
    stdout_thread = threading.Thread(target=log_output, args=(process.stdout, 'info'))
    stderr_thread = threading.Thread(target=log_output, args=(process.stderr, 'error'))
    stdout_thread.daemon = True
    stderr_thread.daemon = True
    stdout_thread.start()
    stderr_thread.start()
    
    # Wait for process to finish
    return_code = process.wait()
    
    # Wait for threads to finish processing output
    stdout_thread.join()
    stderr_thread.join()
    
    logger.info(f"Subprocess completed with return code: {return_code}")
    return return_code

if __name__ == "__main__":
    logging.info("Starting email processing job")
    
    logging.info("Searching for 'Need batch' emails")
    email_matches = grab_emails("Need batch")
    logging.info(f"Found {len(email_matches)} 'Need batch' emails")
    
    logging.info("Searching for emails with PP/SQ attachments")
    attachment_matches = grab_emails_with_attachment(["PP", "SQ"])
    logging.info(f"Found {len(attachment_matches)} emails with PP/SQ attachments")
    
    email_matches += attachment_matches
    
    unread = unread_emails(email_matches)
    logging.info(f"Found {len(unread)} unread emails to process")
    
    if not unread:
        logging.info("No unread emails to process")
    
    for i, email in enumerate(unread, 1):
        try:
            subject = next((header['value'] for header in email['payload']['headers'] if header['name'] == 'Subject'), 'No subject')
            from_addr = get_email_address(email)
            logging.info(f"Processing email {i}/{len(unread)}: From={from_addr}, Subject={subject}")
            
            # Process "Need batch" type emails
            if "Need batch" in subject:
                logging.info("Processing as 'Need batch' email")
                arg_str = get_args(email)
                logging.info(f"Parsed arguments: {arg_str}")
                
                args = parse_range(arg_str)
                logging.info(f"Getting batch data for range: {args}")
                batch_data = get_batches(args)
                
                logging.info("Processing contributions with addresses")
                contributions_for_letters = contributions_with_addresses(batch_data)
                
                filename = makefilename(args)
                logging.info(f"Saving to file: {filename}")
                save(contributions_for_letters, filename)
                
                # wait for the file to be created
                while not os.path.exists(filename):
                    logging.debug("Waiting for file to be created...")
                    pass
                
                logging.info(f"Sending email with attachment {filename} to {from_addr}")
                message = MIMEMultipart()
                message['to'] = from_addr
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
                logging.info(f"Successfully sent {filename}")
            
            # Process attachments for all emails
            logging.info("Checking for PP/SQ attachments")
            process_attachments(email)
            
        except Exception as e:
            logging.error(f"Error processing email: {str(e)}", exc_info=True)
            continue
        
        if 'e' not in locals():     # Only mark as read and archive if no errors occurred
            try:
                logging.info(f"Marking email {email['id']} as read and archiving")
                mark_as_read(email)
                archive_email(email)
            except Exception as ee:
                logging.error(f"Error marking email as read or archiving: {str(ee)}", exc_info=True)
    
    logging.info("Email processing job completed")







